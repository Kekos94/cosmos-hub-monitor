"""Resumable daily lifecycle journals, separate from immutable send/receive ledgers."""
import argparse
import copy
import os
import time
from datetime import date,datetime,timedelta,timezone
from pathlib import Path

import hub_fee_monitor_v41 as http
from criterion2_core import LIFE_EVENTS,digest,search_query,utc
from criterion2_pipeline import ROOT,DATA,IN_DATA,Client,atomic,read,load_config,build_plan,validate_plan
from criterion2_ranges import read_fragment,block_fragment,validate_fragments

LIFE_ROOT=ROOT/'data/criterion2_lifecycle'
ORDER=['ics20_v1_timeout','ics20_v1_timeout_close','ics20_v1_write_ack','ics20_v1_source_ack']


def config(path=None):
    cfg,_=load_config() if path is None else load_config(path)
    uid=digest(dict(scope='ics20_v1_lifecycle_v1',chains=sorted((c['name'],c['chain_id']) for c in cfg['chains'])))[:16]
    return cfg,uid


def inspect(state):
    if state['schema_version']!=1:raise ValueError('Unknown lifecycle journal schema')
    p=state['plan'];validate_plan(p)
    if p['scope'] not in LIFE_EVENTS:raise ValueError('Wrong lifecycle journal scope')
    cursor,keys,records=validate_fragments(p,state['ranges'])
    if state['complete']!=(cursor==p['end_height']+1):raise ValueError('False lifecycle completion flag')
    for r in records:
        if r['execution_status'] not in ('executed','noop','unverified','committed_write_ack') or r['ack_status'] not in ('success','error','unknown'):
            raise ValueError('Invalid lifecycle evidence status')
        if r['refund_status'] not in ('not_verified','verified_bank_credit'):raise ValueError('Invalid refund evidence status')
        if r['execution_status']=='committed_write_ack' and r['event_type']!='write_acknowledgement':raise ValueError('Wrong write acknowledgement scope')
        if r['refund_status']=='verified_bank_credit':
            if r['execution_status']!='executed' or not (r['ack_status']=='error' or r['event_type'].startswith('timeout')):raise ValueError('Refund without executed failure')
            if not r['refund_amount_raw'].isascii() or not r['refund_amount_raw'].isdigit() or int(r['refund_amount_raw'])<=0 or len(r['refund_evidence_sha256'])!=64:
                raise ValueError('Invalid bank refund proof')
    return dict(plan=p,next_height=cursor,keys=keys,records=records,complete=state['complete'])


def save(path,state):
    inspect(state);path=Path(path)
    if path.exists():
        old=read(path);inspect(old)
        if old==state:return
        if old['complete'] or old['plan']!=state['plan'] or state['ranges'][:len(old['ranges'])]!=old['ranges']:
            raise ValueError('Cannot rewrite lifecycle evidence history')
    atomic(path,state,compressed=True)


def collect(path,client,*,deadline):
    state=read(path);d=inspect(state);p=d['plan']
    if d['complete']:return
    if client.source!=p['source'] or client.chain['chain_id']!=p['chain_id']:raise ValueError('Lifecycle provider changed')
    # Begin with the whole day: an empty or rare event stream needs one query.
    span=(state['ranges'][-1]['end_height']-state['ranges'][-1]['start_height']+1)*2 if state['ranges'] else p['end_height']-p['start_height']+1
    while d['next_height']<=p['end_height'] and time.monotonic()<deadline:
        first=d['next_height'];end=min(p['end_height'],first+max(1,span)-1)
        while True:
            r=read_fragment(client,p,first,end)
            if r is not None:break
            if first==end:r=block_fragment(client,p,first);break
            end=(first+end)//2
        state['ranges'].append(r);state['complete']=end==p['end_height'];save(path,state)
        d=inspect(state);span=min(p['end_height']-p['start_height']+1,(end-first+1)*2)
        print(f'Lifecycle {p["chain"]} {p["date"]} {LIFE_EVENTS[p["scope"]][0]}: {end-p["start_height"]+1}/{p["end_height"]-p["start_height"]+1} heights; {len(d["records"])} events',flush=True)


def reference_plans():
    refs={}
    for root in (DATA,IN_DATA):
        for path in sorted(root.glob('*/*/*/plan.json')):
            p=read(path);validate_plan(p);refs.setdefault((p['chain_id'],p['date']),p)
    return refs


def make_plan(client,day,uid,scope,members,refs,cache):
    key=(client.chain['chain_id'],day,client.source)
    if key not in cache:
        status=client.rpc('status')
        if status['node_info']['network']!=client.chain['chain_id'] or status['sync_info']['catching_up'] or status['node_info'].get('other',{}).get('tx_index')!='on':
            raise ValueError('Wrong chain, unsynchronised node or unavailable event index')
        assets=dict(mapping={},method='lifecycle journal; asset units remain in original cohort plan')
        ref=refs.get((client.chain['chain_id'],day))
        if ref:
            p=copy.deepcopy(ref)
            for boundary in ('previous_block','following_block'):
                observed=client.block(p[boundary]['height'])
                if observed['chain_id']!=p[boundary]['chain_id'] or utc(observed['time'])!=utc(p[boundary]['time']):raise ValueError('Lifecycle UTC boundary mismatch')
            p.update(boundary_reference_sha256=digest(ref),assets=assets,assets_sha256=digest(assets))
        else:p=build_plan(client,day,uid,assets,scope=scope,members=members)
        cache[key]=p
    p=copy.deepcopy(cache[key]);p.update(universe_id=uid,source=client.source,scope=scope,members=members,
       query=search_query(p['start_height'],p['end_height'],scope),query_mode='height_ranges',page_size=100,observed_at=datetime.now(timezone.utc).isoformat())
    validate_plan(p);return p


def journals(root=LIFE_ROOT):
    for path in sorted(Path(root).glob('*/*/*/*.json.gz')):
        state=read(path);d=inspect(state);p=d['plan']
        if path.name!=p['scope']+'.json.gz' or path.parent.name!=p['date'] or path.parent.parent.name!=p['chain'] or path.parent.parent.parent.name!=p['universe_id']:
            raise ValueError('Lifecycle journal path does not match plan')
        yield d


def run(*,budget=600,start_date=None,root=LIFE_ROOT):
    if budget<=0:raise ValueError('Invalid lifecycle budget')
    cfg,uid=config();refs=reference_plans();yesterday=str(datetime.now(timezone.utc).date()-timedelta(days=1))
    first=start_date or min((day for _,day in refs),default=yesterday)
    if date.fromisoformat(first)>date.fromisoformat(yesterday):raise ValueError('Lifecycle day is not closed')
    members=sorted([dict(name=c['name'],chain_id=c['chain_id']) for c in cfg['chains']],key=lambda c:c['name'])
    work=[(scope,chain) for scope in ORDER for chain in cfg['chains']];deadline=time.monotonic()+budget;cache={};errors=[]
    for index,(scope,chain) in enumerate(work):
        if time.monotonic()>=deadline:break
        slice_end=min(deadline,time.monotonic()+max(30,(deadline-time.monotonic())/(len(work)-index)))
        token=http.REQUEST_DEADLINE.set(slice_end)
        try:
            day=first
            while day<=yesterday and time.monotonic()<slice_end:
                path=Path(root)/uid/chain['name']/day/(scope+'.json.gz')
                if path.exists():
                    d=inspect(read(path))
                    if d['complete']:day=str(date.fromisoformat(day)+timedelta(days=1));continue
                    client=Client(chain,d['plan']['source'])
                else:
                    failures=[]
                    ref=refs.get((chain['chain_id'],day));sources=list(dict.fromkeys(([ref['source']] if ref and ref['source'] in chain['rpc'] else [])+chain['rpc']))
                    for source in sources:
                        try:
                            client=Client(chain,source);p=make_plan(client,day,uid,scope,members,refs,cache)
                            # A real query is required: HTTP 400/unsupported filters never mean zero.
                            probe=client.search(p['query'],1,1)
                            if int(probe['total_count'])<0 or len(probe.get('txs') or [])!=min(1,int(probe['total_count'])):raise ValueError('Unsupported lifecycle index query')
                            break
                        except (RuntimeError,ValueError,KeyError) as exc:failures.append(str(exc))
                    else:raise RuntimeError('; '.join(failures))
                    save(path,dict(schema_version=1,plan=p,ranges=[],complete=p['start_height']>p['end_height']))
                collect(path,client,deadline=slice_end)
                if not inspect(read(path))['complete']:break
                day=str(date.fromisoformat(day)+timedelta(days=1))
        except TimeoutError:print(f'Lifecycle time slice reached: {chain["name"]} {scope}; evidence retained.',flush=True)
        except Exception as exc:
            errors.append(f'{chain["name"]} {scope}: {exc}');print('Lifecycle error: '+errors[-1],flush=True)
        finally:http.REQUEST_DEADLINE.reset(token)
    if errors:raise RuntimeError('\n'.join(errors))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--budget',type=int,default=int(os.environ.get('LIFECYCLE_BUDGET_SECONDS','600')))
    parser.add_argument('--start-date',default=None);args=parser.parse_args();run(budget=args.budget,start_date=args.start_date)


if __name__=='__main__':main()
