"""Resumable, immutable raw proofs for same-transaction Hub PFM forwarding."""
import base64
import os
import json
import time
from collections import defaultdict
from pathlib import Path

import hub_fee_monitor_v41 as http
from criterion2_export import all_days
from criterion2_pipeline import ROOT,DATA,IN_DATA,read,write_once,atomic,load_config,incoming_config
from criterion2_core import digest
from criterion2_transit_evidence import MODEL,links,verify_tx,query_data,topology

TRANSIT_ROOT=ROOT/'data/criterion2_transit'


def universe_id():
    _,out_uid=load_config();_,in_uid=incoming_config()
    return digest(dict(model=MODEL,incoming=in_uid,outgoing=out_uid))[:16]


def cohorts(out_root=DATA,in_root=IN_DATA):
    _,out_uid=load_config();_,in_uid=incoming_config()
    outgoing={(d['plan']['universe_id'],d['plan']['date']):d for d in all_days(out_root) if d['complete'] and d['plan']['chain_id']=='cosmoshub-4' and d['plan']['universe_id']==out_uid}
    for incoming in all_days(in_root):
        if not incoming['complete'] or incoming['plan']['chain_id']!='cosmoshub-4' or incoming['plan']['universe_id']!=in_uid:continue
        matches=[d for (_,day),d in outgoing.items() if day==incoming['plan']['date'] and
                 d['plan']['members']==incoming['plan']['members']]
        if len(matches)!=1:continue
        outgoing_day=matches[0];a=incoming['plan'];b=outgoing_day['plan']
        if (a['start_height'],a['end_height'])!=(b['start_height'],b['end_height']):raise ValueError('Transit cohort height mismatch')
        uid=digest(dict(model=MODEL,incoming=a['universe_id'],outgoing=b['universe_id']))[:16]
        parents=defaultdict(list);children=defaultdict(list)
        for p in incoming['packets']:parents[p['tx_hash']].append(p)
        for p in outgoing_day['packets']:children[p['tx_hash']].append(p)
        hashes=sorted(set(parents)&set(children),key=lambda h:(int(parents[h][0]['height']),int(parents[h][0]['tx_index']),h))
        plan=dict(model=MODEL,universe_id=uid,date=a['date'],chain_id=a['chain_id'],source=a['source'],
                  incoming_universe_id=a['universe_id'],outgoing_universe_id=b['universe_id'],
                  start_height=a['start_height'],end_height=a['end_height'],candidate_hashes=hashes,
                  incoming_digest=digest(incoming['packets']),outgoing_digest=digest(outgoing_day['packets']))
        yield dict(plan=plan,incoming=incoming,outgoing=outgoing_day,parents=parents,children=children)


def directory(cohort,root=TRANSIT_ROOT):
    p=cohort['plan'];return Path(root)/p['universe_id']/'cosmoshub'/p['date']


def batch(source,calls):
    """Validate every batch ID; a transport/provider error never becomes an empty result."""
    try:payload,_=http.get_any([source],'/',rpc_calls=calls)
    except RuntimeError as exc:
        if 'batching is not supported' not in str(exc):raise
        payload=[]
        for call in calls:
            params=dict(call['params'])
            if call['method']=='tx':params['hash']='0x'+base64.b64decode(params['hash']).hex()
            if call['method']=='abci_query':
                params['path']=json.dumps(params['path']);params['data']='0x'+params['data']
            params={k:('true' if v else 'false') if isinstance(v,bool) else v for k,v in params.items()}
            answer,_=http.get_any([source],'/'+call['method'],params=params)
            payload.append(dict(id=call['id'],**{k:answer[k] for k in ('result','error') if k in answer}))
    if not isinstance(payload,list) or len(payload)!=len(calls):raise ValueError('Incomplete RPC batch')
    wanted={c['id'] for c in calls};results={}
    for item in payload:
        if not isinstance(item,dict) or item.get('id') not in wanted or item['id'] in results:raise ValueError('Invalid/duplicate RPC batch ID')
        if item.get('error') or 'result' not in item:raise ValueError('RPC batch item failed: '+str(item.get('error','missing result'))[:300])
        results[item['id']]=item['result']
    if set(results)!=wanted:raise ValueError('Missing RPC batch IDs')
    return results


def channel_requests(height,channel):
    return [dict(jsonrpc='2.0',id=method,method='abci_query',params=dict(
        path='/ibc.core.channel.v1.Query/'+method,data=query_data('transfer',channel),height=str(height),prove=False))
        for method in ('Channel','ChannelClientState')]


def topology_tasks(records):
    return sorted({(r['height'],r[k]) for r in records for k in ('incoming_channel','outgoing_channel')},key=lambda x:(int(x[0]),x[1]))


_STORE_CACHE={}

def store(folder):
    path=Path(folder)/'observations.json.gz'
    stamp=path.stat().st_mtime_ns if path.exists() else None
    cached=_STORE_CACHE.get(str(path))
    if cached and cached[0]==stamp:return cached[1]
    value=read(path) if stamp is not None else dict(transactions={},channels={})
    if set(value)!={'transactions','channels'}:raise ValueError('Invalid transit observation bundle')
    _STORE_CACHE[str(path)]=(stamp,value)
    return value


def save_store(folder,value):
    path=Path(folder)/'observations.json.gz'
    previous=read(path) if path.exists() else dict(transactions={},channels={})
    for kind in previous:
        if any(k not in value[kind] or value[kind][k]!=v for k,v in previous[kind].items()):raise ValueError('Transit proof history regressed')
    atomic(path,value,compressed=True)
    _STORE_CACHE[str(path)]=(path.stat().st_mtime_ns,value)


def raw_links(cohort,root=TRANSIT_ROOT):
    folder=directory(cohort,root);records=[];diagnostics=[];saved=0
    if (folder/'plan.json').exists() and read(folder/'plan.json')!=cohort['plan']:raise ValueError('Transit input snapshot changed')
    for h in cohort['plan']['candidate_hashes']:
        tx=store(folder)['transactions'].get(h)
        if tx is None:continue
        linked,unknown=links(tx,cohort['parents'][h],cohort['children'][h]);saved+=1
        records.extend(linked);diagnostics.extend(unknown)
    return records,diagnostics,saved


def channel_proof(folder,height,channel):
    r=store(folder)['channels'].get(str(height)+'_'+channel)
    if r is None:return None
    if r['height']!=str(height) or r['channel']!=channel:raise ValueError('Channel cache key mismatch')
    return topology(r['Channel'],r['ChannelClientState'],height)


def collect(root=TRANSIT_ROOT,*,budget=300):
    deadline=time.monotonic()+budget;token=http.REQUEST_DEADLINE.set(deadline);failed=False
    try:
        for c in cohorts():
            p=c['plan'];folder=directory(c,root);write_once(folder/'plan.json',p)
            cache=store(folder)
            missing=[h for h in p['candidate_hashes'] if h not in cache['transactions']]
            for start in range(0,len(missing),8):
                if time.monotonic()>=deadline:return failed
                group=missing[start:start+8]
                calls=[dict(jsonrpc='2.0',id=h,method='tx',params=dict(hash=base64.b64encode(bytes.fromhex(h)).decode(),prove=False)) for h in group]
                try:
                    results=batch(p['source'],calls)
                    for h in group:
                        tx=results[h];verify_tx(tx,c['parents'][h],c['children'][h])
                    cache['transactions'].update(results)
                    save_store(folder,cache)
                except TimeoutError:return failed
                except (ValueError,RuntimeError) as exc:
                    failed=True;print('Transit transaction batch unavailable: '+str(exc)[:350],flush=True)
                print(f"Transit {p['date']}: candidate transactions {min(start+8,len(missing))}/{len(missing)} requested (cached proofs retained)",flush=True)
            records,_,saved=raw_links(c,root)
            pending=[(h,ch) for h,ch in topology_tasks(records) if h+'_'+ch not in cache['channels']]
            for start in range(0,len(pending),4):
                if time.monotonic()>=deadline:return failed
                group=pending[start:start+4];calls=[]
                for i,(h,ch) in enumerate(group):
                    for call in channel_requests(h,ch):call=dict(call,id=str(i)+'-'+call['id']);calls.append(call)
                try:
                    results=batch(p['source'],calls)
                    validated={}
                    for i,(h,ch) in enumerate(group):
                        a=results[str(i)+'-Channel'];b=results[str(i)+'-ChannelClientState']
                        topology(a,b,h) # Historical proof must validate before becoming immutable.
                        validated[h+'_'+ch]=dict(height=h,channel=ch,Channel=a,ChannelClientState=b)
                    cache['channels'].update(validated)
                    save_store(folder,cache)
                except TimeoutError:return failed
                except (ValueError,RuntimeError) as exc:
                    failed=True;print('Transit historical topology unavailable: '+str(exc)[:350],flush=True)
                if start%40==0:print(f"Transit {p['date']}: historical channels {min(start+4,len(pending))}/{len(pending)} requested",flush=True)
            print(f"Transit {p['date']}: {saved}/{len(p['candidate_hashes'])} transactions inspected; {len(records)} strict PFM links",flush=True)
    finally:http.REQUEST_DEADLINE.reset(token)
    return failed


if __name__=='__main__':
    raise SystemExit(1 if collect(budget=int(os.environ.get('TRANSIT_BUDGET_SECONDS','300'))) else 0)
