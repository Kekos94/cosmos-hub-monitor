"""Adaptive indexed height ranges; verify a single block when its index is incomplete."""
import base64
import hashlib
import time
from datetime import datetime,timezone
from pathlib import Path

from criterion2_core import attributes,digest,flow_packets,utc,verify_search_tx,day_bounds,search_query,event_spec,SCOPE,IN_SCOPE


def query(first,end,scope=SCOPE):
    return search_query(first,end,scope)


def read_fragment(client,plan,first,end):
    result=client.search(query(first,end,plan['scope']),1,100)
    total=int(result['total_count']);txs=result.get('txs') or []
    if not 0<=total<=100 or len(txs)!=total:
        return None
    if len({(int(t['height']),int(t['index'])) for t in txs})!=total or len({t['hash'].upper() for t in txs})!=total:
        return None
    return fragment(plan,first,end,txs,'indexed_single_response',total)


def fragment(plan,first,end,txs,method,total):
    keys=[];flows=[];failed=0
    subplan=dict(plan,start_height=first,end_height=end)
    # RPC ordering by height need not order ties by the transaction index.
    for tx in sorted(txs,key=lambda t:(int(t['height']),int(t['index']))):
        keys.append(list(verify_search_tx(tx,subplan)))
        failed+=int(int(tx['tx_result']['code'])!=0)
        flows.extend(flow_packets(plan,tx))
    return dict(schema_version=1,plan_sha256=digest(plan),start_height=first,end_height=end,
                query=query(first,end,plan['scope']),method=method,reported_total=total,tx_keys=keys,packets=flows,
                failed_indexed_txs=failed,observed_at=datetime.now(timezone.utc).isoformat())


def block_fragment(client,plan,height):
    block_response=client.rpc('block',{'height':str(height)})
    block=block_response['block'];header=block['header']
    begin,finish=day_bounds(plan['date'])
    if header['chain_id']!=plan['chain_id'] or int(header['height'])!=height or not begin<=utc(header['time'])<finish:
        raise ValueError('Fallback block has wrong identity or UTC day')
    executed=client.rpc('block_results',{'height':str(height)})
    raw=block['data'].get('txs') or [];results=executed.get('txs_results') or []
    if int(executed['height'])!=height or len(results)!=len(raw):
        raise ValueError('Fallback block results are incomplete')
    txs=[]
    for index,(encoded,execution) in enumerate(zip(raw,results)):
        # Normalize packet attributes only; the raw result still comes from this tx index.
        event,port=event_spec(plan['scope'])
        if not any(e['type']==event and attributes(e).get(port)=='transfer'
                   for e in execution.get('events') or []):continue
        txs.append(dict(height=str(height),index=index,hash=hashlib.sha256(base64.b64decode(encoded,validate=True)).hexdigest().upper(),
                        tx=encoded,tx_result=execution))
    record=fragment(plan,height,height,txs,'block_results_verified',len(txs))
    record.update(block_header=header,block_hash=block_response['block_id']['hash'],block_tx_count=len(raw),
                  execution_result_count=len(results),block_document_sha256=digest(block_response),execution_document_sha256=digest(executed))
    print(f'Criterion 2 {plan["chain"]}: index incomplete at {height}; verified all {len(raw)} transactions directly in block results.',flush=True)
    return record


def validate_fragments(plan,records):
    cursor=plan['start_height'];keys=[];flows=[];ids=set();hashes=set()
    begin,finish=day_bounds(plan['date'])
    for r in records:
        first,end=r['start_height'],r['end_height']
        if first!=cursor or end<first or end>plan['end_height'] or r['plan_sha256']!=digest(plan) or r['query']!=query(first,end,plan['scope']):
            raise ValueError('Missing/overlapping adaptive query range')
        if r['method']=='indexed_single_response':
            if not 0<=r['reported_total']<=100 or r['reported_total']!=len(r['tx_keys']):
                raise ValueError('Incomplete single-response index range')
        elif r['method']=='block_results_verified':
            h=r['block_header']
            if first!=end or int(h['height'])!=first or h['chain_id']!=plan['chain_id'] or not begin<=utc(h['time'])<finish:
                raise ValueError('Invalid block fallback proof')
            if r['block_tx_count']!=r['execution_result_count'] or r['block_tx_count']<len(r['tx_keys']):
                raise ValueError('Truncated block fallback')
        else:raise ValueError('Unknown index coverage method')
        valid=set()
        for key in r['tx_keys']:
            if not first<=int(key[0])<=end or int(key[1])<0 or (keys and tuple(key[:2])<=tuple(keys[-1][:2])) or key[2] in hashes:
                raise ValueError('Duplicate/out-of-range indexed transaction')
            hashes.add(key[2]);valid.add(key[2]);keys.append(key)
        for p in r['packets']:
            valid_amount=p['amount_raw'].isdigit() and int(p['amount_raw'])>0
            rejected_payload=(plan['scope']==IN_SCOPE and p.get('receipt_status') in ('error_ack_in_receive_tx','unconfirmed_ack_in_receive_tx')
                              and p.get('payload_status')=='invalid_or_unsupported' and p['amount_raw']=='')
            if p['tx_hash'] not in valid or p['chain_id']!=plan['chain_id'] or p['packet_id'] in ids or not (valid_amount or rejected_payload):
                raise ValueError('Duplicate or invalid packet in adaptive range')
            ids.add(p['packet_id']);flows.append(p)
        cursor=end+1
    return cursor,keys,flows


def inspect_ranges(directory,plan):
    from criterion2_pipeline import read
    directory=Path(directory);records=[]
    for p in sorted((directory/'ranges').glob('*.json.gz')):
        r=read(p)
        if p.name!=f'{r["start_height"]:012d}-{r["end_height"]:012d}.json.gz':raise ValueError('Wrong range checkpoint filename')
        records.append(r)
    cursor,keys,flows=validate_fragments(plan,records)
    complete=False;marker=directory/'complete.json'
    if marker.exists():
        m=read(marker)
        if cursor!=plan['end_height']+1 or m['plan_sha256']!=digest(plan) or m['total_count']!=len(keys) or m['ranges_sha256']!=[digest(r) for r in records]:
            raise ValueError('False adaptive index completion')
        complete=True
    return dict(plan=plan,pages=records,packets=flows,total_count=len(keys) if complete else None,
                scanned_txs=len(keys),complete=complete,next_height=cursor)


def collect_ranges(directory,client,*,deadline):
    from criterion2_pipeline import read,write_once
    directory=Path(directory);plan=read(directory/'plan.json');d=inspect_ranges(directory,plan)
    if d['complete']:return
    if client.source!=plan['source'] or client.chain['chain_id']!=plan['chain_id']:
        raise ValueError('Adaptive resume provider changed')
    previous_span=(d['pages'][-1]['end_height']-d['pages'][-1]['start_height']+1) if d['pages'] else 5000
    window=min(10000,max(1,previous_span*2))
    while d['next_height']<=plan['end_height'] and time.monotonic()<deadline:
        first=d['next_height'];end=min(plan['end_height'],first+window-1)
        while True:
            r=read_fragment(client,plan,first,end)
            if r is not None:break
            if first==end:
                r=block_fragment(client,plan,first)
                break
            end=(first+end)//2
        window=min(10000,max(1,(end-first+1)*2))
        validate_fragments(plan,d['pages']+[r])
        write_once(directory/'ranges'/f'{first:012d}-{end:012d}.json.gz',r,compressed=True)
        d=inspect_ranges(directory,plan)
        print(f'Criterion 2 {plan["chain"]} {plan["date"]}: indexed ranges cover {d["next_height"]-plan["start_height"]}/{plan["end_height"]-plan["start_height"]+1} block heights; {len(d["packets"])} packets.',flush=True)
    if d['next_height']==plan['end_height']+1:
        write_once(directory/'complete.json',dict(plan_sha256=digest(plan),total_count=d['scanned_txs'],
                   ranges_sha256=[digest(r) for r in d['pages']],completed_at=datetime.now(timezone.utc).isoformat()))
        inspect_ranges(directory,plan)
