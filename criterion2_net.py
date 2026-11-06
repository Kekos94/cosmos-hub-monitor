"""Successful-transfer cohort nets, by exact local denomination and common UTC cutoff.

Not bank-balance changes: original send/receive dates remain the activity dates.
Failed transfers are excluded, refunds are not added a second time.
"""
import csv
from collections import defaultdict
from datetime import date,timedelta
from decimal import Decimal,localcontext
from pathlib import Path

from criterion2_core import SCOPE,IN_SCOPE,digest,decimal_amount,resolve_asset,validate_price
from criterion2_pipeline import ROOT,DATA,IN_DATA,load_config,incoming_config
from criterion2_export import all_days,PRICES,load_price,write_csv
from criterion2_lifecycle_collect import LIFE_ROOT,journals,config as lifecycle_config
from criterion2_lifecycle_export import coverage,key,reconcile
from criterion2_lifecycle_evidence import bank_denom

SCOPE_NET='ics20_v1_successful_transfer_cohort_net_v1'
BASE='universe_id outgoing_universe_id incoming_universe_id date chain chain_id coverage_status evidence_through_utc'.split()
ASSET=BASE+'''denom symbol decimals coingecko_id metadata_status incoming_success_packets outgoing_success_packets excluded_packets unresolved_packets confirmed_in_raw confirmed_out_raw known_net_raw net_raw confirmed_in_amount confirmed_out_amount known_net_amount net_amount reference_price_usd reference_time_utc confirmed_in_usd confirmed_out_usd net_usd settlement_status valuation_status'''.split()
CHAIN=BASE+'''observed_packets incoming_success_packets outgoing_success_packets excluded_packets unresolved_packets unknown_payload_packets assets valued_assets unpriced_assets valued_in_usd valued_out_usd valued_net_usd total_in_usd total_out_usd total_net_usd net_status'''.split()
UNRESOLVED={'awaiting_evidence','unsupported_ack','conflicts'}
FAILURES={'error_acknowledged','timeout_confirmed'}


def config(path=ROOT/'config/criterion2_chains.json'):
    cfg,out_uid=load_config(path);_,in_uid=incoming_config(path)
    uid=digest(dict(scope=SCOPE_NET,outgoing=out_uid,incoming=in_uid))[:16]
    return cfg,out_uid,in_uid,uid


def metadata(denom,out_plan,in_plan):
    a=resolve_asset(denom,out_plan['assets']['mapping']);b=resolve_asset(denom,in_plan['assets']['mapping'])
    if a and b and any(a[k]!=b[k] for k in ('base','decimals','coingecko_id')):return None,'conflict'
    return (a or b),'known' if (a or b) else 'unknown'


def asset_rows(base,cohorts,evidence,prices_root):
    """Both complete cohorts; evidence is restricted to one shared complete cutoff."""
    grouped=defaultdict(list);unknown_payload=0;outcomes=[]
    for direction,d in cohorts.items():
        for packet in d['packets']:
            relevant=[e for e in evidence.get(key(packet),[]) if (e['event_type']=='write_acknowledgement')==(direction=='in')]
            status=reconcile(packet,relevant,direction)['outcome'];outcomes.append((direction,status))
            if status not in UNRESOLVED|FAILURES|{'success_confirmed'}:raise ValueError('Unsupported net outcome')
            amount=packet['amount_raw'];denom=packet['denom']
            if not amount or not denom:
                if status=='success_confirmed':raise ValueError('Successful transfer without usable asset/amount')
                if status in UNRESOLVED:unknown_payload+=1
                continue
            if not amount.isascii() or not amount.isdigit() or int(amount)<=0:raise ValueError('Invalid net amount')
            grouped[bank_denom(denom)].append((direction,packet,status))
    rows=[]
    for denom,packets in sorted(grouped.items()):
        asset,mstatus=metadata(denom,cohorts['out']['plan'],cohorts['in']['plan'])
        totals={direction:sum(int(p['amount_raw']) for dr,p,s in packets if dr==direction and s=='success_confirmed') for direction in ('in','out')}
        counts={direction:sum(dr==direction and s=='success_confirmed' for dr,_,s in packets) for direction in ('in','out')}
        unresolved=sum(s in UNRESOLVED for _,_,s in packets);failed=sum(s in FAILURES for _,_,s in packets)
        raw=totals['in']-totals['out'];resolved=unresolved==0 and unknown_payload==0
        row=dict(base,denom=denom,symbol=asset['symbol'] if asset else '',decimals=asset['decimals'] if asset else '',
                 coingecko_id=asset['coingecko_id'] if asset else '',metadata_status=mstatus,incoming_success_packets=counts['in'],outgoing_success_packets=counts['out'],
                 excluded_packets=failed,unresolved_packets=unresolved,confirmed_in_raw=str(totals['in']),confirmed_out_raw=str(totals['out']),known_net_raw=str(raw),net_raw=str(raw) if resolved else '',
                 confirmed_in_amount='',confirmed_out_amount='',known_net_amount='',net_amount='',reference_price_usd='',reference_time_utc='',
                 confirmed_in_usd='',confirmed_out_usd='',net_usd='',settlement_status='complete' if resolved else 'unknown_payload' if unknown_payload else 'unresolved_packets',
                 valuation_status='metadata_conflict' if mstatus=='conflict' else 'unknown_asset' if not asset else 'missing_historical_price')
        if asset:
            for source,target in [('in','confirmed_in_amount'),('out','confirmed_out_amount')]:row[target]=str(decimal_amount(totals[source],asset['decimals']))
            row['known_net_amount']=str(decimal_amount(raw,asset['decimals']))
            if resolved:row['net_amount']=row['known_net_amount']
            price=load_price(base['date'],asset['coingecko_id'],prices_root) if asset['coingecko_id'] else None
            if price:
                rate=validate_price(price,base['date'],asset['coingecko_id'])
                row.update(reference_price_usd=str(rate),reference_time_utc=price['reference_time_utc'],valuation_status='valued')
                with localcontext() as ctx:
                    ctx.prec=200
                    row['confirmed_in_usd']=str(Decimal(row['confirmed_in_amount'])*rate)
                    row['confirmed_out_usd']=str(Decimal(row['confirmed_out_amount'])*rate)
                    if resolved:row['net_usd']=str(Decimal(row['net_amount'])*rate)
        rows.append(row)
    return rows,outcomes,unknown_payload


def calculate(out_days,in_days,journal_days,cfg,out_uid,in_uid,uid,prices_root=PRICES):
    by_day={};dates=set();members={(c['chain_id'],c['name']) for c in cfg['chains']}
    for direction,days,wanted,scope in [('out',out_days,out_uid,SCOPE),('in',in_days,in_uid,IN_SCOPE)]:
        for d in days:
            p=d['plan']
            if p['universe_id']!=wanted:continue
            if p['scope']!=scope or (p['chain_id'],p['chain']) not in members:raise ValueError('Wrong net cohort scope/chain')
            if sorted((c['chain_id'],c['name']) for c in p['members'])!=sorted(members):raise ValueError('Net member universe mismatch')
            k=(p['date'],p['chain_id'],direction)
            if k in by_day:raise ValueError('Duplicate net cohort')
            by_day[k]=d;dates.add(p['date'])
    stream_days={};complete_journals=[]
    for d in journal_days:
        p=d['plan']
        if (p['chain_id'],p['chain']) not in members:continue
        k=(p['chain_id'],p['scope'],p['date'])
        if k in stream_days:raise ValueError('Duplicate lifecycle journal')
        stream_days[k]=d['complete']
        if d['complete']:complete_journals.append(d)
    chains=[];assets=[]
    for day in sorted(dates):
        paired=all(by_day.get((day,c['chain_id'],dr),{}).get('complete',False) for c in cfg['chains'] for dr in ('in','out'))
        bounds=[coverage(c['chain_id'],day,stream_days,dr)[1] for c in cfg['chains'] for dr in ('in','out')]
        through=min(bounds) if paired and all(bounds) else ''
        evidence=defaultdict(list)
        if through:
            for d in complete_journals:
                p=d['plan']
                if not day<=p['date']<through[:10]:continue
                for e in d['records']:evidence[key(e)].append(dict(e,_journal_date=p['date']))
        for c in cfg['chains']:
            base=dict(universe_id=uid,outgoing_universe_id=out_uid,incoming_universe_id=in_uid,date=day,chain=c['name'],chain_id=c['chain_id'],
                      coverage_status='common_indexed_day' if through else 'incomplete_cohorts' if not paired else 'incomplete_lifecycle',evidence_through_utc=through)
            if not through:
                chains.append(dict(base,**{f:'' for f in CHAIN if f not in base and f!='net_status'},net_status=base['coverage_status']));continue
            cohorts={dr:by_day[(day,c['chain_id'],dr)] for dr in ('in','out')}
            # Same chain/day must refer to exactly the same UTC boundary heights.
            if any(cohorts['in']['plan'][f]!=cohorts['out']['plan'][f] for f in ('start_height','end_height')):raise ValueError('Net UTC height boundaries differ')
            values,outcomes,unknown_payload=asset_rows(base,cohorts,evidence,prices_root);assets.extend(values)
            unresolved=sum(s in UNRESOLVED for _,s in outcomes)
            active=[r for r in values if r['incoming_success_packets']+r['outgoing_success_packets']>0]
            eligible=[r for r in active if r['net_usd']!='']
            unpriced=sum(r['valuation_status']!='valued' for r in active)
            complete=unresolved==0 and unpriced==0 and unknown_payload==0
            known=bool(eligible) or not active and unresolved==0
            with localcontext() as ctx:
                ctx.prec=200
                incoming=sum((Decimal(r['confirmed_in_usd']) for r in eligible),Decimal(0));outgoing=sum((Decimal(r['confirmed_out_usd']) for r in eligible),Decimal(0));net=incoming-outgoing
            chains.append(dict(base,observed_packets=len(outcomes),incoming_success_packets=sum(dr=='in' and s=='success_confirmed' for dr,s in outcomes),
              outgoing_success_packets=sum(dr=='out' and s=='success_confirmed' for dr,s in outcomes),excluded_packets=sum(s in FAILURES for _,s in outcomes),unresolved_packets=unresolved,
              unknown_payload_packets=unknown_payload,assets=len(values),valued_assets=len(eligible),unpriced_assets=unpriced,
              valued_in_usd=str(incoming) if known else '',valued_out_usd=str(outgoing) if known else '',valued_net_usd=str(net) if known else '',
              total_in_usd=str(incoming) if complete else '',total_out_usd=str(outgoing) if complete else '',total_net_usd=str(net) if complete else '',
              net_status='complete' if complete else 'unresolved_packets' if unresolved else 'partial_valuation'))
    return chains,assets


def retain_history(path,rows,fields,uid):
    path=Path(path)
    if not path.exists():return rows
    with path.open(encoding='utf-8',newline='') as stream:
        reader=csv.DictReader(stream)
        if reader.fieldnames!=fields:raise ValueError('Unknown historical net CSV schema')
        previous=list(reader)
    key_fields=['universe_id','date','chain']+(['denom'] if 'denom' in fields else [])
    identity=lambda r:tuple(r[f] for f in key_fields)
    current={identity(r):r for r in rows}
    if len(current)!=len(rows):raise ValueError('Duplicate computed net rows')
    for r in previous:
        if r['universe_id']!=uid:continue
        now=current.get(identity(r))
        if now is None:raise ValueError('Previously exported net cohort/asset missing from source ledgers')
        if r['evidence_through_utc'] and now['evidence_through_utc']<r['evidence_through_utc']:raise ValueError('Net evidence coverage regressed')
    combined=[r for r in previous if r['universe_id']!=uid]+rows
    if len({identity(r) for r in combined})!=len(combined):raise ValueError('Duplicate historical net rows')
    return sorted(combined,key=identity)


def exports(out_root=DATA,in_root=IN_DATA,life_root=LIFE_ROOT,out=ROOT,prices_root=PRICES,config_path=ROOT/'config/criterion2_chains.json'):
    cfg,out_uid,in_uid,uid=config(config_path);_,life_uid=lifecycle_config(config_path)
    life=[d for d in journals(life_root) if d['plan']['universe_id']==life_uid]
    chains,assets=calculate(all_days(out_root),all_days(in_root),life,cfg,out_uid,in_uid,uid,prices_root)
    chains=retain_history(Path(out)/'ibc_net_chains_daily.csv',chains,CHAIN,uid)
    assets=retain_history(Path(out)/'ibc_net_assets_daily.csv',assets,ASSET,uid)
    write_csv(Path(out)/'ibc_net_chains_daily.csv',chains,CHAIN);write_csv(Path(out)/'ibc_net_assets_daily.csv',assets,ASSET)
    return chains,assets


if __name__=='__main__':exports()
