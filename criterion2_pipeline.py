"""Independent four-chain ICS20 indexed-day collector and checkpoint store."""
import argparse
import csv
import gzip
import io
import json
import os
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import hub_fee_monitor_v41 as http
from criterion2_core import SCOPE, IN_SCOPE, LIFE_EVENTS, asset_index, day_bounds, digest, flow_packets, search_query, utc, verify_search_tx

ROOT = Path(__file__).resolve().parent
DATA = ROOT / 'data' / 'criterion2'
IN_DATA = ROOT / 'data' / 'criterion2_incoming'
CONFIG = ROOT / 'config' / 'criterion2_chains.json'


class IndexInconsistency(ValueError):
    """A live provider response cannot complete the pinned index snapshot."""


def quarantine(directory,reason,root=DATA):
    directory=Path(directory);p=read(directory/'plan.json')
    target=Path(root)/'rejected'/p['universe_id']/p['chain']/p['date']/str(time.time_ns())
    target.parent.mkdir(parents=True,exist_ok=True)
    os.replace(directory,target)
    atomic(target/'rejection.json',dict(reason=str(reason),source=p['source'],observed_at=datetime.now(timezone.utc).isoformat()))
    print(f'Criterion 2 index snapshot archived for review: {p["chain"]} {p["date"]}: {reason}',flush=True)
    return target


def atomic(path, value, *, compressed=False):
    path = Path(path)
    raw = (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)+'\n').encode()
    if compressed:
        raw = gzip.compress(raw, mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(raw);f.flush();os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)


def read(path):
    path = Path(path)
    data = path.read_bytes()
    return json.loads(gzip.decompress(data) if path.suffix == '.gz' else data)


def write_once(path, value, *, compressed=False):
    if Path(path).exists():
        if read(path) != value:
            raise ValueError('Conflicting replay of immutable checkpoint')
        return
    atomic(path, value, compressed=compressed)


def load_config(path=CONFIG):
    cfg = read(path)
    if cfg['schema_version'] != 1 or cfg['scope'] != SCOPE:
        raise ValueError('Unsupported Criterion 2 configuration')
    chains = cfg['chains']
    if len(chains) < 2 or len({c['name'] for c in chains}) != len(chains) or len({c['chain_id'] for c in chains}) != len(chains):
        raise ValueError('Invalid chain universe')
    if not any(c['name']=='cosmoshub' for c in chains):
        raise ValueError('Cosmos Hub must be part of the comparison')
    for c in chains:
        if not c['rpc'] or not all(url.startswith('https://') for url in c['rpc']):
            raise ValueError('Missing secure RPC endpoints')
        if not c['name'].isalnum():
            raise ValueError('Invalid registry chain name')
    uid = digest({'scope':cfg['scope'],'chains':sorted((c['name'],c['chain_id']) for c in chains)})[:16]
    return cfg, uid


def incoming_config(path=CONFIG):
    cfg,_=load_config(path)
    cfg=dict(cfg,scope=IN_SCOPE,chains=[dict(c,query_mode='height_ranges') for c in cfg['chains']])
    uid=digest({'scope':IN_SCOPE,'chains':sorted((c['name'],c['chain_id']) for c in cfg['chains'])})[:16]
    return cfg,uid


class Client:
    def __init__(self, chain, source):
        self.chain, self.source = chain, source.rstrip('/')

    def rpc(self, method, params=None):
        encoded = {}
        for key,value in (params or {}).items():
            if key in ('query','order_by'):
                encoded[key]=json.dumps(value)
            elif isinstance(value,bool):
                encoded[key]='true' if value else 'false'
            else:
                encoded[key]=value
        answer, _ = http.get_any([self.source], '/'+method, params=encoded)
        result = answer.get('result')
        if not isinstance(result, dict):raise ValueError('Missing RPC result')
        return result

    def block(self, height):
        b = self.rpc('block', {'height':str(height)})['block']
        h = b['header']
        if h['chain_id'] != self.chain['chain_id'] or int(h['height']) != height:
            raise ValueError('Wrong chain or block height')
        utc(h['time'])
        return dict(height=height,time=h['time'],chain_id=h['chain_id'])

    def search(self, query, page, page_size):
        return self.rpc('tx_search', {'query':query,'prove':False,'page':str(page),
                                     'per_page':str(page_size),'order_by':'asc'})


def build_plan(client, day, universe_id, assets, *, page_size=50, now=None, members=None, scope=SCOPE):
    now = now or datetime.now(timezone.utc)
    begin, finish = day_bounds(day)
    if finish > now:raise ValueError('UTC day is not closed')
    status = client.rpc('status')
    if status['node_info']['network'] != client.chain['chain_id'] or status['sync_info']['catching_up']:
        raise ValueError('RPC node is on wrong chain or catching up')
    if status['node_info'].get('other',{}).get('tx_index') != 'on':
        raise ValueError('Event index must be explicitly enabled')
    latest = int(status['sync_info']['latest_block_height'])
    earliest = max(1,int(status['sync_info'].get('earliest_block_height',1)))
    cache = {}
    def h(n):
        if n not in cache:cache[n]=client.block(n)
        return cache[n]
    if utc(h(latest)['time']) < finish:raise ValueError('No finalized block after next midnight')
    low, distance = latest, 1
    while utc(h(low)['time']) >= begin:
        if low == earliest:raise ValueError('Archive does not cover start of day')
        low=max(earliest,latest-distance);distance*=2
    def bound(target):
        a,b=low,latest
        while a<b:
            m=(a+b)//2
            if utc(h(m)['time'])<target:a=m+1
            else:b=m
        return a
    first,following=bound(begin),bound(finish)
    query=search_query(first,following-1,scope)
    # A halted day has an empty block range. Querying tx.height>=first AND <=first-1 is safe.
    plan=dict(schema_version=1,scope=scope,date=day,universe_id=universe_id,
              chain=client.chain['name'],chain_id=client.chain['chain_id'],source=client.source,
              members=members or [dict(name=client.chain['name'],chain_id=client.chain['chain_id'])],
              query_mode=client.chain.get('query_mode','paged'),
              start_height=first,end_height=following-1,previous_block=h(first-1),
              first_block=h(first),last_block=h(following-1),following_block=h(following),
              query=query,page_size=page_size,assets=assets,assets_sha256=digest(assets),
              observed_at=datetime.now(timezone.utc).isoformat(),coverage_method='complete_index_query_not_block_scan')
    validate_plan(plan)
    return plan


def validate_plan(p):
    begin,finish=day_bounds(p['date'])
    first,end=int(p['start_height']),int(p['end_height'])
    if p['schema_version']!=1 or p['scope'] not in (SCOPE,IN_SCOPE,*LIFE_EVENTS) or first<2 or end<first-1:
        raise ValueError('Invalid IBC day plan')
    if not 1<=p['page_size']<=100:raise ValueError('Invalid pagination size')
    if p.get('query_mode','paged') not in ('paged','height_ranges'):
        raise ValueError('Unknown index query mode')
    for key,height in [('previous_block',first-1),('first_block',first),('last_block',end),('following_block',end+1)]:
        b=p[key]
        if b['height']!=height or b['chain_id']!=p['chain_id']:raise ValueError('Invalid boundary identity')
    if not utc(p['previous_block']['time'])<begin<=utc(p['first_block']['time']):raise ValueError('Invalid start boundary')
    if not utc(p['last_block']['time'])<finish<=utc(p['following_block']['time']):raise ValueError('Invalid end boundary')
    if first<=end and not begin<=utc(p['first_block']['time'])<=utc(p['last_block']['time'])<finish:
        raise ValueError('Day plan contains blocks outside UTC day')
    expected=search_query(first,end,p['scope'])
    if p['query']!=expected or p['assets_sha256']!=digest(p['assets']):raise ValueError('Plan query/metadata changed')


def make_page(plan, response, page_number):
    total=int(response['total_count']);txs=response.get('txs') or []
    size=plan['page_size'];expected=min(size,max(0,total-(page_number-1)*size))
    if total<0 or len(txs)!=expected:raise IndexInconsistency('Search pagination truncated or inconsistent')
    if page_number>max(1,(total+size-1)//size):raise ValueError('Unexpected page after end of results')
    keys=[];flows=[];failed=0
    for tx in txs:
        identity=verify_search_tx(tx,plan)
        keys.append(list(identity))
        failed+=int(int(tx['tx_result']['code'])!=0)
        flows.extend(flow_packets(plan,tx))
    return dict(schema_version=1,plan_sha256=digest(plan),page=page_number,total_count=total,
                tx_keys=keys,packets=flows,failed_indexed_txs=failed,observed_at=datetime.now(timezone.utc).isoformat())


def inspect_day(directory):
    directory=Path(directory);plan=read(directory/'plan.json');validate_plan(plan)
    if plan.get('query_mode')=='height_ranges':
        from criterion2_ranges import inspect_ranges
        return inspect_ranges(directory,plan)
    pages=[];keys=[];packet_ids=set();flows=[];total=None
    for n,path in enumerate(sorted((directory/'pages').glob('*.json.gz')),1):
        page=read(path)
        if path.name!=f'{n:06d}.json.gz' or page['page']!=n or page['plan_sha256']!=digest(plan):
            raise ValueError('Missing, reordered or conflicting page checkpoint')
        if total is not None and page['total_count']!=total:raise ValueError('Index total changed between pages')
        total=page['total_count']
        if total<0 or n>max(1,(total+plan['page_size']-1)//plan['page_size']):raise ValueError('Unexpected page count')
        expected=min(plan['page_size'],max(0,total-(n-1)*plan['page_size']))
        if len(page['tx_keys'])!=expected:raise ValueError('Incomplete page transaction coverage')
        for key in page['tx_keys']:
            if not plan['start_height']<=int(key[0])<=plan['end_height'] or int(key[1])<0:
                raise ValueError('Checkpoint transaction outside day')
            if keys and tuple(key[:2])<=tuple(keys[-1][:2]):raise ValueError('Transactions duplicated or unsorted')
            keys.append(key)
        valid_hashes={k[2] for k in page['tx_keys']}
        for packet in page['packets']:
            if packet['tx_hash'] not in valid_hashes or packet['chain_id']!=plan['chain_id']:
                raise ValueError('Packet does not belong to checked page')
            if packet['packet_id'] in packet_ids:raise ValueError('Duplicated packet across pages')
            if not packet['amount_raw'].isdigit() or int(packet['amount_raw'])<=0:raise ValueError('Invalid stored amount')
            packet_ids.add(packet['packet_id']);flows.append(packet)
        pages.append(page)
    if len({k[2] for k in keys})!=len(keys):raise ValueError('Duplicated transaction hash')
    complete=False
    marker=directory/'complete.json'
    if marker.exists():
        record=read(marker)
        if not pages or len(keys)!=total or record['total_count']!=total or record['plan_sha256']!=digest(plan):
            raise ValueError('False indexed-day completion marker')
        if record['pages_sha256']!=[digest(p) for p in pages]:raise ValueError('Completed page changed')
        complete=True
    return dict(plan=plan,pages=pages,packets=flows,total_count=total,scanned_txs=len(keys),complete=complete)


def finalize_day(directory, client, d):
    plan=d['plan']
    check=client.search(plan['query'],1,1)
    if int(check['total_count'])!=d['total_count']:raise IndexInconsistency('Index changed during final completeness check')
    expected_first=d['pages'][0]['tx_keys'][0] if d['scanned_txs'] else None
    if expected_first:
        first=verify_search_tx(check['txs'][0],plan)
        if list(first)!=expected_first:raise IndexInconsistency('Index first transaction changed')
    elif check.get('txs'):
        raise ValueError('Index returned transactions despite zero total')
    write_once(Path(directory)/'complete.json',dict(plan_sha256=digest(plan),total_count=d['total_count'],
               pages_sha256=[digest(p) for p in d['pages']],completed_at=datetime.now(timezone.utc).isoformat()))
    inspect_day(directory)


def collect_pages(directory, client, *, deadline):
    d=inspect_day(directory);plan=d['plan']
    if plan.get('query_mode')=='height_ranges':
        from criterion2_ranges import collect_ranges
        return collect_ranges(directory,client,deadline=deadline)
    if client.source!=plan['source'] or client.chain['chain_id']!=plan['chain_id']:
        raise ValueError('Resume must use the original provider index')
    if d['complete']:return
    if d['total_count'] is not None and d['scanned_txs']==d['total_count']:
        finalize_day(directory,client,d)
        return
    n=len(d['pages'])+1
    while time.monotonic()<deadline:
        response=client.search(plan['query'],n,plan['page_size'])
        page=make_page(plan,response,n)
        if d['total_count'] is not None and page['total_count']!=d['total_count']:
            raise IndexInconsistency('Provider index total changed; existing checkpoints preserved for review')
        write_once(Path(directory)/'pages'/f'{n:06d}.json.gz',page,compressed=True)
        d=inspect_day(directory)
        print(f'Criterion 2 {plan["chain"]} {plan["date"]}: {d["scanned_txs"]}/{d["total_count"]} indexed transactions, {len(d["packets"])} packets',flush=True)
        if d['scanned_txs']==d['total_count']:
            finalize_day(directory,client,d)
            return
        n+=1


def registry_assets(chain):
    url=f'https://raw.githubusercontent.com/cosmos/chain-registry/master/{chain["name"]}/assetlist.json'
    base=url.rsplit('/',1)[0];path='/assetlist.json'
    data,_=http.get_any([base],path)
    return dict(source=url,observed_at=datetime.now(timezone.utc).isoformat(),raw_sha256=digest(data),mapping=asset_index(data))


def run_collection(cfg,uid,*,budget=1200,start_date=None,root=DATA):
    if budget<=0:raise ValueError('Invalid collection budget')
    yesterday=str(datetime.now(timezone.utc).date()-timedelta(days=1))
    first=start_date or yesterday
    if date.fromisoformat(first)>date.fromisoformat(yesterday):raise ValueError('Start date is not closed')
    deadline=time.monotonic()+budget;errors=[]
    # Equal time slices: a slow/broken chain cannot consume the entire run.
    slice_seconds=max(1,budget/len(cfg['chains']))
    for chain in cfg['chains']:
        token=http.REQUEST_DEADLINE.set(min(deadline,time.monotonic()+slice_seconds))
        try:
            base=Path(root)/uid/chain['name'];existing=sorted(base.glob('*/plan.json'))
            days=[p.parent for p in existing]
            for prev,nxt in zip(days,days[1:]):
                if date.fromisoformat(nxt.name)!=date.fromisoformat(prev.name)+timedelta(days=1):raise ValueError('Missing historical day')
                if not inspect_day(prev)['complete']:raise ValueError('Cannot skip unfinished day')
            if days and start_date and start_date!=days[0].name:raise ValueError('Cannot change historical START_DATE')
            initial_day=days[0].name if days else first
            excluded_sources=set()
            while time.monotonic()<http.REQUEST_DEADLINE.get():
                if days and not inspect_day(days[-1])['complete']:
                    directory=days[-1];plan=read(directory/'plan.json');client=Client(chain,plan['source'])
                    if plan.get('query_mode','paged')!=chain.get('query_mode','paged'):
                        quarantine(directory,'Upgrade to adaptive ranges after provider pagination inconsistencies',root)
                        days.pop()
                        continue
                else:
                    day=str(date.fromisoformat(days[-1].name)+timedelta(days=1)) if days else initial_day
                    if day>yesterday:break
                    errors_for_source=[]
                    assets=None
                    for source in chain['rpc']:
                        if source in excluded_sources:continue
                        try:
                            client=Client(chain,source)
                            assets=assets or registry_assets(chain)
                            plan=build_plan(client,day,uid,assets,members=sorted([dict(name=c['name'],chain_id=c['chain_id']) for c in cfg['chains']],key=lambda c:c['name']),scope=cfg['scope'])
                            # Check capability before pinning a provider; gateways may forbid range queries.
                            probe=client.search(plan['query'],1,1)
                            if int(probe['total_count'])<0 or len(probe.get('txs') or [])!=min(1,int(probe['total_count'])):raise ValueError('Invalid index capability response')
                            for tx in probe.get('txs') or []:verify_search_tx(tx,plan)
                            break
                        except (RuntimeError,ValueError,KeyError) as exc:errors_for_source.append(str(exc))
                    else:raise RuntimeError('; '.join(errors_for_source))
                    directory=base/day;write_once(directory/'plan.json',plan);days.append(directory)
                try:
                    collect_pages(directory,client,deadline=http.REQUEST_DEADLINE.get())
                except IndexInconsistency as exc:
                    excluded_sources.add(client.source)
                    quarantine(directory,exc,root)
                    days.pop()
                    continue
                if not inspect_day(directory)['complete']:break
        except TimeoutError:
            print(f'Criterion 2 {chain["name"]}: time slice reached; saved pages retained.',flush=True)
        except Exception as exc:
            errors.append(f'{chain["name"]}: {exc}')
            print('Criterion 2 error: '+errors[-1],flush=True)
        finally:http.REQUEST_DEADLINE.reset(token)
    if errors:raise RuntimeError('\n'.join(errors))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--budget',type=int,default=int(os.environ.get('CRITERION2_BUDGET_SECONDS','2400')))
    parser.add_argument('--start-date',default=os.environ.get('START_DATE') or None)
    parser.add_argument('--direction',choices=['out','in','both'],default='both')
    args=parser.parse_args();cfg,uid=load_config()
    errors=[]
    for direction in (['out','in'] if args.direction=='both' else [args.direction]):
        selected,selected_uid=(cfg,uid) if direction=='out' else incoming_config()
        budget = args.budget
        if args.direction == 'both':
            incoming_budget = args.budget // 2
            budget = max(1, args.budget - incoming_budget if direction == 'out' else incoming_budget)
        try:
            run_collection(selected,selected_uid,budget=budget,start_date=args.start_date,root=DATA if direction=='out' else IN_DATA)
        except Exception as exc:
            errors.append(f'{direction}: {exc}')
    if errors:raise RuntimeError('\n'.join(errors))


if __name__=='__main__':main()
