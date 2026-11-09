"""Rolling and calendar periods from validated daily nets."""
import csv
from collections import defaultdict
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,localcontext
from pathlib import Path

from criterion2_pipeline import ROOT
from criterion2_core import decimal_amount,utc
from criterion2_export import write_csv
from criterion2_net import config

LENGTHS=(7,30,90)
BASE='universe_id outgoing_universe_id incoming_universe_id date start_date end_date window_days period_kind chain chain_id covered_days missing_days covered_dates missing_dates coverage_status evidence_through_utc evidence_status'.split()
ASSET=BASE+'''denom symbol decimals coingecko_id metadata_status incoming_success_packets outgoing_success_packets excluded_packets unresolved_packets confirmed_in_raw confirmed_out_raw known_net_raw observed_net_raw net_raw confirmed_in_amount confirmed_out_amount observed_net_amount net_amount observed_net_usd net_usd observed_settlement_status valuation_status'''.split()
CHAIN=BASE+'''observed_packets incoming_success_packets outgoing_success_packets excluded_packets unresolved_packets unknown_payload_packets assets valued_assets unpriced_assets valued_in_usd valued_out_usd valued_net_usd total_in_usd total_out_usd total_net_usd net_status'''.split()


def rows(path):
    path=Path(path)
    if not path.exists():return []
    with path.open(encoding='utf-8',newline='') as f:return list(csv.DictReader(f))


def integer(row,key):
    v=str(row[key])
    if not v.isascii() or not v.isdigit():raise ValueError('Invalid daily window count/amount: '+key)
    return int(v)


def number(v):
    n=Decimal(str(v))
    if not n.is_finite():raise ValueError('Non-finite daily valuation')
    return n


def sum_values(values):
    with localcontext() as ctx:
        ctx.prec=400
        return sum((number(v) for v in values),Decimal(0))


def calendar_bounds(day, kind):
    if kind == 'week':
        first = day - timedelta(days=day.weekday())
        return first, first + timedelta(days=6)
    if kind == 'month':
        first = day.replace(day=1)
        following = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
        return first, following - timedelta(days=1)
    return day.replace(month=1, day=1), day.replace(month=12, day=31)


def periods(end, dates, lengths, calendar):
    for length in lengths:
        if length not in LENGTHS:
            raise ValueError('Unsupported rolling window')
        yield 'rolling', end - timedelta(days=length-1), end
    if calendar:
        for kind in ('week', 'month', 'year'):
            bounds = {calendar_bounds(day, kind) for day in dates if day <= end}
            bounds.add(calendar_bounds(end, kind))
            for first, last in sorted(bounds):
                yield kind, first, min(last, end)


def calculate(chain_rows,asset_rows,cfg,uid,*,end_date,lengths=LENGTHS,calendar=False):
    end=date.fromisoformat(end_date);members={c['name']:c['chain_id'] for c in cfg['chains']}
    days={};assets={};by_chain_day=defaultdict(dict)
    for r in chain_rows:
        if r['universe_id']!=uid:continue
        if members.get(r['chain'])!=r['chain_id']:raise ValueError('Window chain universe mismatch')
        date.fromisoformat(r['date']);key=(r['date'],r['chain'])
        if key in days:raise ValueError('Duplicate daily chain row')
        days[key]=r
    for r in asset_rows:
        if r['universe_id']!=uid:continue
        key=(r['date'],r['chain'],r['denom'])
        if key in assets:raise ValueError('Duplicate daily asset row')
        parent=days.get(key[:2])
        if not parent or parent['chain_id']!=r['chain_id']:raise ValueError('Asset without matching daily chain')
        for f in ('outgoing_universe_id','incoming_universe_id','coverage_status','evidence_through_utc'):
            if r[f]!=parent[f]:raise ValueError('Daily asset/chain lineage mismatch')
        if integer(r,'confirmed_in_raw')-integer(r,'confirmed_out_raw')!=int(r['known_net_raw']):raise ValueError('Daily raw net mismatch')
        if r['net_raw']!='' and int(r['net_raw'])!=int(r['known_net_raw']):raise ValueError('Daily settled raw net mismatch')
        if r['metadata_status']=='known':
            decimals=integer(r,'decimals')
            if not 0<=decimals<=36:raise ValueError('Invalid daily asset units')
            for dr in ('in','out'):
                if number(r['confirmed_'+dr+'_amount'])!=decimal_amount(integer(r,'confirmed_'+dr+'_raw'),decimals):raise ValueError('Daily amount/unit mismatch')
        if r['valuation_status']=='valued' and integer(r,'incoming_success_packets')+integer(r,'outgoing_success_packets')>0:
            if utc(r['reference_time_utc'])!=utc(r['date']+'T00:00:00Z'):raise ValueError('Daily price reference date mismatch')
            rate=number(r['reference_price_usd'])
            if rate<=0:raise ValueError('Invalid daily historical price')
            with localcontext() as ctx:
                ctx.prec=400
                for dr in ('in','out'):
                    if number(r['confirmed_'+dr+'_amount'])*rate!=number(r['confirmed_'+dr+'_usd']):raise ValueError('Daily historical valuation mismatch')
        if r['net_usd']!='':
            with localcontext() as ctx:
                ctx.prec=400
                if number(r['confirmed_in_usd'])-number(r['confirmed_out_usd'])!=number(r['net_usd']):raise ValueError('Daily USD net mismatch')
        assets[key]=r;by_chain_day[key[:2]][r['denom']]=r
    for (day,chain),r in days.items():
        if r['coverage_status']!='common_indexed_day':continue
        if not r['evidence_through_utc'] or r['evidence_through_utc'][:10]<=day:raise ValueError('Incomplete daily evidence cutoff')
        children=by_chain_day[(day,chain)]
        if len(children)!=integer(r,'assets'):raise ValueError('Daily asset rows missing from window input')
        if sum(integer(a,'incoming_success_packets') for a in children.values())!=integer(r,'incoming_success_packets') or sum(integer(a,'outgoing_success_packets') for a in children.values())!=integer(r,'outgoing_success_packets'):
            raise ValueError('Daily successful packet counts do not reconcile')
    chain_output=[];asset_output=[]
    dates = {date.fromisoformat(day) for day, _ in days}
    for kind, first, last in periods(end, dates, lengths, calendar):
        length = (last - first).days + 1
        wanted=[str(first+timedelta(days=i)) for i in range(length)];covered=[];cutoffs=set()
        for day in wanted:
            selected=[days.get((day,c)) for c in members]
            if all(r and r['coverage_status']=='common_indexed_day' for r in selected):
                through={r['evidence_through_utc'] for r in selected}
                if len(through)!=1:raise ValueError('Different chain evidence cutoffs in one day')
                covered.append(day);cutoffs.update(through)
        missing=[d for d in wanted if d not in covered];consistent=len(cutoffs)==1;calendar_complete=not missing
        status='inconsistent_evidence' if len(cutoffs)>1 else 'complete_window' if calendar_complete and consistent else 'partial_window'
        for chain,chain_id in members.items():
            selected=[days[(d,chain)] for d in covered]
            out_ids={r['outgoing_universe_id'] for r in selected};in_ids={r['incoming_universe_id'] for r in selected}
            if len(out_ids)>1 or len(in_ids)>1:raise ValueError('Window input universe changed within period')
            base=dict(universe_id=uid,outgoing_universe_id=next(iter(out_ids),'') ,incoming_universe_id=next(iter(in_ids),''),date=str(last),start_date=str(first),end_date=str(last),
              window_days=length,period_kind=kind,chain=chain,chain_id=chain_id,covered_days=len(covered),missing_days=len(missing),covered_dates=';'.join(covered),missing_dates=';'.join(missing),
              coverage_status=status,evidence_through_utc=next(iter(cutoffs)) if consistent else '',evidence_status='common_cutoff' if consistent else 'mixed_cutoffs' if cutoffs else 'unavailable')
            sums={f:sum(integer(r,f) for r in selected) for f in ('observed_packets','incoming_success_packets','outgoing_success_packets','excluded_packets','unresolved_packets','unknown_payload_packets')}
            denoms=sorted({denom for d in covered for denom in by_chain_day[(d,chain)]})
            period_assets=[]
            for denom in denoms:
                parts=[by_chain_day[(d,chain)][denom] for d in covered if denom in by_chain_day[(d,chain)]]
                active=[r for r in parts if integer(r,'incoming_success_packets')+integer(r,'outgoing_success_packets')>0]
                identities={(str(r['decimals']),r['coingecko_id']) for r in parts if r['metadata_status']=='known'}
                conflict=len(identities)>1 or any(r['metadata_status']=='conflict' for r in parts)
                known=not conflict and len(identities)==1 and all(r['metadata_status']=='known' for r in active)
                meta=next((r for r in reversed(parts) if r['metadata_status']=='known'),parts[-1])
                totals={dr:sum(integer(r,'confirmed_'+dr+'_raw') for r in parts) for dr in ('in','out')}
                delta=totals['in']-totals['out'];unresolved=sum(integer(r,'unresolved_packets') for r in parts)
                resolved=consistent and unresolved==0 and sums['unknown_payload_packets']==0 and all(r['settlement_status']=='complete' for r in parts)
                row=dict(base,denom=denom,symbol=meta['symbol'] if known else '',decimals=meta['decimals'] if known else '',coingecko_id=meta['coingecko_id'] if known else '',
                  metadata_status='conflict' if conflict else 'known' if known else 'unknown',
                  **{f:sum(integer(r,f) for r in parts) for f in ('incoming_success_packets','outgoing_success_packets','excluded_packets','unresolved_packets')},
                  confirmed_in_raw=str(totals['in']) if consistent else '',confirmed_out_raw=str(totals['out']) if consistent else '',known_net_raw=str(delta) if consistent else '',
                  observed_net_raw=str(delta) if resolved else '',net_raw=str(delta) if resolved and calendar_complete else '',confirmed_in_amount='',confirmed_out_amount='',
                  observed_net_amount='',net_amount='',observed_net_usd='',net_usd='',observed_settlement_status='complete' if resolved else 'incomplete_evidence' if not consistent else 'unresolved_packets',
                  valuation_status='metadata_conflict' if conflict else 'incomplete_evidence' if not consistent else 'unresolved_packets' if not resolved else 'missing_daily_valuation')
                if known and consistent:
                    for dr in ('in','out'):row['confirmed_'+dr+'_amount']=str(decimal_amount(totals[dr],int(meta['decimals'])))
                    if resolved:row['observed_net_amount']=str(decimal_amount(delta,int(meta['decimals'])))
                    if resolved and calendar_complete:row['net_amount']=row['observed_net_amount']
                price_complete=known and all(r['valuation_status']=='valued' and r['confirmed_in_usd']!='' and r['confirmed_out_usd']!='' for r in active)
                valued=price_complete and resolved
                if valued:
                    incoming=sum_values(r['confirmed_in_usd'] for r in active);outgoing=sum_values(r['confirmed_out_usd'] for r in active)
                    with localcontext() as ctx:
                        ctx.prec=400;net=incoming-outgoing
                    row.update(observed_net_usd=str(net),net_usd=str(net) if calendar_complete else '',valuation_status='sum_daily_historical_valuations')
                    row['_in_usd']=str(incoming);row['_out_usd']=str(outgoing)
                row['_active']=bool(active);row['_price_complete']=price_complete;period_assets.append(row)
            active=[r for r in period_assets if r['_active']];eligible=[r for r in active if r['observed_net_usd']!='']
            unpriced=sum(not r['_price_complete'] for r in active)
            known=consistent and bool(covered) and (bool(eligible) or not active and sums['unresolved_packets']==0)
            complete=calendar_complete and known and sums['unresolved_packets']==0 and sums['unknown_payload_packets']==0 and unpriced==0
            incoming=sum_values(r['_in_usd'] for r in eligible);outgoing=sum_values(r['_out_usd'] for r in eligible)
            with localcontext() as ctx:
                ctx.prec=400;net=incoming-outgoing
            chain_output.append(dict(base,**{f:v if covered else '' for f,v in sums.items()},assets=len(period_assets) if covered else '',valued_assets=len(eligible) if covered else '',
              unpriced_assets=unpriced if covered else '',valued_in_usd=str(incoming) if known else '',valued_out_usd=str(outgoing) if known else '',valued_net_usd=str(net) if known else '',
              total_in_usd=str(incoming) if complete else '',total_out_usd=str(outgoing) if complete else '',total_net_usd=str(net) if complete else '',
              net_status='complete' if complete else status if status!='complete_window' else 'unresolved_packets' if sums['unresolved_packets'] else 'partial_valuation'))
            asset_output.extend({k:v for k,v in r.items() if not k.startswith('_')} for r in period_assets)
    return chain_output,asset_output


def exports(root=ROOT,*,end_date=None):
    root=Path(root);cfg,_,_,uid=config(root/'config/criterion2_chains.json')
    end_date=end_date or str(datetime.now(timezone.utc).date()-timedelta(days=1))
    if date.fromisoformat(end_date)>=datetime.now(timezone.utc).date():raise ValueError('Rolling window must end on a closed UTC day')
    chains,assets=calculate(rows(root/'ibc_net_chains_daily.csv'),rows(root/'ibc_net_assets_daily.csv'),cfg,uid,end_date=end_date,calendar=True)
    write_csv(root/'ibc_net_windows_chains_latest.csv',chains,CHAIN);write_csv(root/'ibc_net_windows_assets_latest.csv',assets,ASSET)
    return chains,assets


if __name__=='__main__':exports()
