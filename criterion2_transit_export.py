"""Proven first-hop PFM transit; no attribution of manual or delayed transfers."""
import csv
from collections import Counter,defaultdict
from decimal import Decimal,localcontext
from pathlib import Path

from criterion2_core import resolve_asset,decimal_amount
from criterion2_export import write_csv,load_price,PRICES
from criterion2_pipeline import ROOT,DATA,IN_DATA
from criterion2_lifecycle_collect import journals,LIFE_ROOT,config as lifecycle_config
from criterion2_lifecycle_export import coverage,key,reconcile
from criterion2_transit_collect import cohorts,directory,raw_links,channel_proof,TRANSIT_ROOT,topology_tasks,universe_id
from criterion2_transit_evidence import MODEL

DAILY='universe_id date model received_packets candidate_receipts candidate_transactions inspected_transactions linked_attempts confirmed_transit return_to_source failed_forward unresolved topology_unknown noncandidate_receipts unlinked_candidate_receipts topology_proofs topology_expected collection_status evidence_through_utc lower_bound_packet_share_pct'.split()
ROUTE='universe_id date source_chain via_chain next_chain confirmed_packets failed_packets unresolved_packets'.split()
ASSET='universe_id date bank_denom symbol decimals coingecko_id confirmed_packets amount_raw amount_display reference_usd reference_time_utc confirmed_usd valuation_status'.split()
PACKET='universe_id date parent_packet_id child_packet_id tx_hash height source_chain next_chain route_class parent_outcome child_outcome status evidence_through_utc bank_denom amount_raw link_evidence_sha256 incoming_topology_sha256 outgoing_topology_sha256'.split()


def classify_route(record,incoming,outgoing):
    if incoming is None or outgoing is None:return 'unknown','',''
    for info,port,channel,connection in [(incoming,record['source_port'],record['source_channel'],record['incoming_connection']),
                                         (outgoing,record['next_port'],record['next_channel'],record['outgoing_connection'])]:
        if info['height']!=record['height'] or (info['counterparty_port'],info['counterparty_channel'])!=(port,channel):raise ValueError('Channel counterpart differs from packet route')
        if not connection or info['connection_id']!=connection:raise ValueError('Channel connection differs from packet route')
        if info['chain_id']=='cosmoshub-4':return 'unknown','',''
    a,b=incoming['chain_id'],outgoing['chain_id']
    return ('return_to_source' if a==b else 'cross_chain'),a,b


def settlement(route,parent,child):
    if parent=='conflicts' or child=='conflicts':return 'unresolved'
    if parent=='success_confirmed' and child=='success_confirmed':
        return 'confirmed_transit' if route=='cross_chain' else 'return_to_source' if route=='return_to_source' else 'topology_unknown'
    if parent in ('error_acknowledged','timeout_confirmed') and child in ('error_acknowledged','timeout_confirmed'):return 'failed_forward'
    # Parent success after a first-child timeout can mean a retry: never attribute
    # the later success to this initial attempt without a separate retry proof.
    return 'unresolved'


def inspect(cohort,stream_journals,root=TRANSIT_ROOT,prices_root=PRICES):
    p=cohort['plan'];day=p['date'];folder=directory(cohort,root);records,diagnostics,saved=raw_links(cohort,root)
    streams={(d['plan']['chain_id'],d['plan']['scope'],d['plan']['date']):d['complete'] for d in stream_journals}
    ends=[coverage(p['chain_id'],day,streams,d)[1] for d in ('in','out')]
    through=min(ends) if all(ends) else '';evidence=defaultdict(list)
    if through:
        for d in stream_journals:
            if not d['complete'] or not day<=d['plan']['date']<through[:10] or d['plan']['chain_id']!=p['chain_id']:continue
            for e in d['records']:evidence[key(e)].append(dict(e,_journal_date=d['plan']['date']))
    parents={r['packet_id']:r for r in cohort['incoming']['packets']};children={r['packet_id']:r for r in cohort['outgoing']['packets']}
    if len({r['parent_packet_id'] for r in records})!=len(records) or len({r['child_packet_id'] for r in records})!=len(records):raise ValueError('Duplicate transit attribution')
    packets=[];routes={};assets=defaultdict(list);topo_saved=0
    for h,ch in topology_tasks(records):
        if channel_proof(folder,h,ch):topo_saved+=1
    for r in records:
        inc=channel_proof(folder,r['height'],r['incoming_channel']);out=channel_proof(folder,r['height'],r['outgoing_channel'])
        route,source,next_chain=classify_route(r,inc,out)
        statuses=[]
        for packet,direction in [(parents[r['parent_packet_id']],'in'),(children[r['child_packet_id']],'out')]:
            ev=[e for e in evidence[key(packet)] if (e['event_type']=='write_acknowledgement')==(direction=='in')]
            statuses.append(reconcile(packet,ev,direction)['outcome'] if through else 'awaiting_evidence')
        status=settlement(route,*statuses)
        packets.append(dict(universe_id=p['universe_id'],date=day,parent_packet_id=r['parent_packet_id'],child_packet_id=r['child_packet_id'],
            tx_hash=r['tx_hash'],height=r['height'],source_chain=source,next_chain=next_chain,route_class=route,parent_outcome=statuses[0],child_outcome=statuses[1],status=status,
            evidence_through_utc=through,bank_denom=r['bank_denom'],amount_raw=r['amount_raw'],link_evidence_sha256=r['link_evidence_sha256'],
            incoming_topology_sha256=inc['evidence_sha256'] if inc else '',outgoing_topology_sha256=out['evidence_sha256'] if out else ''))
        if source and next_chain and source!=next_chain:
            route_row=routes.setdefault((source,next_chain),dict(universe_id=p['universe_id'],date=day,source_chain=source,via_chain=p['chain_id'],next_chain=next_chain,confirmed_packets=0,failed_packets=0,unresolved_packets=0))
            route_row['confirmed_packets' if status=='confirmed_transit' else 'failed_packets' if status=='failed_forward' else 'unresolved_packets']+=1
        if status=='confirmed_transit':assets[r['bank_denom']].append(r)
    asset_rows=[]
    mapping=cohort['incoming']['plan']['assets']['mapping']
    for denom,rs in sorted(assets.items()):
        meta=resolve_asset(rs[0]['denom'],mapping);raw=sum(int(r['amount_raw']) for r in rs)
        price=load_price(day,meta['coingecko_id'],prices_root) if meta and meta['coingecko_id'] else None
        with localcontext() as ctx:
            ctx.prec=200
            display=str(decimal_amount(raw,meta['decimals'])) if meta else ''
            usd=str(Decimal(display)*Decimal(price['usd'])) if price else ''
        asset_rows.append(dict(universe_id=p['universe_id'],date=day,bank_denom=denom,symbol=meta['symbol'] if meta else '',decimals=meta['decimals'] if meta else '',
            coingecko_id=meta['coingecko_id'] if meta else '',confirmed_packets=len(rs),amount_raw=str(raw),amount_display=display,
            reference_usd=price['usd'] if price else '',reference_time_utc=price['reference_time_utc'] if price else '',confirmed_usd=usd,
            valuation_status='valued' if price else 'price_unavailable' if meta else 'units_unavailable'))
    counts=Counter(r['status'] for r in packets);candidates=sum(len(cohort['parents'][h]) for h in p['candidate_hashes']);received=len(parents)
    topo_expected=len(topology_tasks(records));complete=saved==len(p['candidate_hashes']) and topo_saved==topo_expected
    summary=dict(universe_id=p['universe_id'],date=day,model=MODEL,received_packets=received,candidate_receipts=candidates,
        candidate_transactions=len(p['candidate_hashes']),inspected_transactions=saved,linked_attempts=len(records),
        **{k:counts[k] for k in ('confirmed_transit','return_to_source','failed_forward','unresolved','topology_unknown')},
        noncandidate_receipts=received-candidates,unlinked_candidate_receipts=candidates-len(records),topology_proofs=topo_saved,topology_expected=topo_expected,
        collection_status='candidate_proofs_complete' if complete else 'incomplete',evidence_through_utc=through,
        lower_bound_packet_share_pct=str(Decimal(counts['confirmed_transit'])*100/Decimal(received)) if complete and through and received else '')
    return summary,asset_rows,list(routes.values()),packets


def retain_history(path,rows,fields,identity_fields):
    path=Path(path)
    identity=lambda r:tuple(str(r[f]) for f in identity_fields)
    current={identity(r):r for r in rows}
    if len(current)!=len(rows):raise ValueError('Duplicate transit export row')
    if not path.exists():return rows
    with path.open(newline='',encoding='utf-8') as stream:
        reader=csv.DictReader(stream)
        if reader.fieldnames!=fields:raise ValueError('Unknown transit historical CSV schema')
        previous=list(reader)
    uid=universe_id()
    for r in previous:
        if r['universe_id']!=uid:continue
        now=current.get(identity(r))
        if now is None:raise ValueError('Previously exported transit evidence missing; keeping historical CSV')
        if 'evidence_through_utc' in fields and now['evidence_through_utc']<r['evidence_through_utc']:raise ValueError('Transit confirmation coverage regressed')
        if 'inspected_transactions' in fields and int(now['inspected_transactions'])<int(r['inspected_transactions']):raise ValueError('Transit candidate coverage regressed')
    return sorted([r for r in previous if r['universe_id']!=uid]+rows,key=identity)


def exports(out=ROOT,root=TRANSIT_ROOT,out_root=DATA,in_root=IN_DATA,life_root=LIFE_ROOT,prices_root=PRICES):
    _,life_uid=lifecycle_config();streams=[d for d in journals(life_root) if d['plan']['universe_id']==life_uid]
    daily=[];assets=[];routes=[];packets=[]
    for c in cohorts(out_root,in_root):
        a,b,d,e=inspect(c,streams,root,prices_root);daily.append(a);assets.extend(b);routes.extend(d);packets.extend(e)
    target=Path(out)
    outputs=[]
    for name,rows,fields,keys in [('daily',daily,DAILY,[]),('assets_daily',assets,ASSET,['bank_denom']),('routes_daily',routes,ROUTE,['source_chain','next_chain']),('packets',packets,PACKET,['parent_packet_id'])]:
        path=target/('ibc_transit_'+name+'.csv')
        outputs.append((path,retain_history(path,rows,fields,['universe_id','date']+keys),fields))
    for path,rows,fields in outputs:write_csv(path,rows,fields)
    return daily,assets,routes


if __name__=='__main__':exports()
