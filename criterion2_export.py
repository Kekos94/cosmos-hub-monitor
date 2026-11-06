"""Historical reference pricing and typed Criterion 2 CSV exports."""
import argparse
import csv
import io
import hashlib
import json
import os
import re
import tempfile
import time
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path

import requests
import hub_fee_monitor_v41 as http
from criterion2_core import compare_day, decimal_amount, digest, resolve_asset, validate_price, valued_packet
from criterion2_pipeline import ROOT, DATA, atomic, inspect_day, read, write_once

PRICES=DATA/'prices'


def price_path(day,asset,root=PRICES):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]*',asset):raise ValueError('Invalid price asset identifier')
    date.fromisoformat(day)
    return Path(root)/day/(asset+'.json')


def load_price(day,asset,root=PRICES):
    path=price_path(day,asset,root)
    if not path.exists():return None
    record=read(path);validate_price(record,day,asset)
    return record


def fetch_price(day,asset,root=PRICES):
    existing=load_price(day,asset,root)
    if existing:return existing
    base='https://api.coingecko.com/api/v3'
    headers={}
    if os.environ.get('COINGECKO_PRO_API_KEY'):
        base='https://pro-api.coingecko.com/api/v3';headers['x-cg-pro-api-key']=os.environ['COINGECKO_PRO_API_KEY']
    elif os.environ.get('COINGECKO_DEMO_API_KEY'):
        headers['x-cg-demo-api-key']=os.environ['COINGECKO_DEMO_API_KEY']
    day_value=date.fromisoformat(day)
    params={'date':day,'localization':'false'}
    source=base+'/coins/'+asset+'/history'
    for attempt in range(3):
        http.wait_for_provider(base)
        deadline=http.REQUEST_DEADLINE.get()
        timeout=min(20,deadline-time.monotonic()) if deadline is not None else 20
        if timeout<=0:raise TimeoutError('Price time budget reached')
        try:
            response=requests.get(source,params=params,headers=headers,timeout=timeout)
            # Errors never contain API keys or become zero-valued prices.
            if not response.ok:
                if response.status_code in http.RETRYABLE_STATUS:
                    http.defer_provider(base,max(6,http.retry_delay(response,attempt)))
                    continue
                raise ValueError(f'Historical price HTTP {response.status_code}')
            data=json.loads(response.text,parse_float=Decimal)
            if data.get('id')!=asset:raise ValueError('Historical price asset mismatch')
            value=((data.get('market_data') or {}).get('current_price') or {}).get('usd')
            if value is None:raise ValueError('No historical USD price')
            record=dict(date=day,coingecko_id=asset,usd=str(value),observed_at=datetime.now(timezone.utc).isoformat(),
                        reference_time_utc=day+'T00:00:00+00:00',method='coingecko_daily_history_reference',
                        source=source,source_date_parameter=params['date'],raw_sha256=hashlib.sha256(response.content).hexdigest())
            validate_price(record,day,asset)
            write_once(price_path(day,asset,root),record)
            http.defer_provider(base,6)
            return record
        except requests.RequestException:
            http.defer_provider(base,2**attempt)
    raise ValueError('Historical price retries exhausted')


def all_days(root=DATA):
    return [inspect_day(p.parent) for p in sorted(Path(root).glob('*/*/*/plan.json'))]


def fetch_needed_prices(root=DATA,*,budget=180,prices_root=PRICES):
    needed=Counter()
    for d in all_days(root):
        if not d['complete']:continue
        for packet in d['packets']:
            asset=resolve_asset(packet['denom'],d['plan']['assets']['mapping'])
            if asset and asset['coingecko_id']:needed[(d['plan']['date'],asset['coingecko_id'])]+=1
    token=http.REQUEST_DEADLINE.set(time.monotonic()+budget)
    try:
        # Spend a limited public API budget on the most frequently observed assets first.
        for day,asset in sorted(needed,key=lambda key:(-needed[key],key)):
            try:fetch_price(day,asset,prices_root)
            except TimeoutError:break
            except (ValueError,KeyError) as exc:
                # Error record is separate from immutable successful historical prices.
                path=price_path(day,asset,prices_root).with_suffix('.error.json')
                atomic(path,dict(date=day,asset=asset,error=str(exc),observed_at=datetime.now(timezone.utc).isoformat()))
                print(f'Criterion 2 historical price unavailable: {day} {asset}: {exc}',flush=True)
    finally:http.REQUEST_DEADLINE.reset(token)


def write_csv(path,rows,fields):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    output=io.StringIO(newline='');writer=csv.DictWriter(output,fieldnames=fields,lineterminator='\n')
    writer.writeheader();writer.writerows(rows)
    fd,tmp=tempfile.mkstemp(dir=path.parent,suffix='.tmp')
    try:
        with os.fdopen(fd,'w',encoding='utf-8',newline='') as f:f.write(output.getvalue());f.flush();os.fsync(f.fileno())
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)


PROGRESS_FIELDS='universe_id date chain chain_id coverage_status coverage_method start_height end_height expected_blocks indexed_transactions_saved indexed_transactions_total sent_packets_saved source'.split()
CHAIN_FIELDS='universe_id date chain chain_id coverage_status scope sent_packets priced_packets unpriced_packets valued_out_usd total_out_usd failed_indexed_transactions price_method'.split()
ASSET_FIELDS='universe_id date chain chain_id denom asset_base symbol decimals coingecko_id amount_raw amount_display sent_packets out_usd valuation_status'.split()
COMPARISON_FIELDS='universe_id date scope chains indexed_chains_complete sent_packets priced_packets unpriced_packets valued_out_usd total_out_usd hub_valued_out_usd hub_share_valued_usd_pct hub_share_total_usd_pct hub_share_packet_count_pct valuation_status index_coverage_caveat settlement_status'.split()


def exports(root=DATA,out=ROOT,prices_root=PRICES):
    progress=[];chains=[];assets=[];groups={};group_members={}
    for d in all_days(root):
        p=d['plan'];uid=p['universe_id'];day=p['date'];name=p['chain'];status='indexed_complete' if d['complete'] else 'incomplete'
        progress.append(dict(universe_id=uid,date=day,chain=name,chain_id=p['chain_id'],coverage_status=status,
                             coverage_method=p['coverage_method'],start_height=p['start_height'],end_height=p['end_height'],
                             expected_blocks=p['end_height']-p['start_height']+1,indexed_transactions_saved=d['scanned_txs'],
                             indexed_transactions_total='' if d['total_count'] is None else d['total_count'],
                             sent_packets_saved=len(d['packets']),source=p['source']))
        if not d['complete']:continue
        prices={};mapping=p['assets']['mapping'];by_denom={};valued=[]
        for packet in d['packets']:
            asset=resolve_asset(packet['denom'],mapping)
            if asset and asset['coingecko_id'] and asset['coingecko_id'] not in prices:
                record=load_price(day,asset['coingecko_id'],prices_root)
                if record is not None:prices[asset['coingecko_id']]=record
            v=valued_packet(packet,mapping,prices,day);valued.append(v)
            by_denom.setdefault(packet['denom'],[]).append(v)
        for denom,packets_for_denom in sorted(by_denom.items()):
            first=packets_for_denom[0];raw=sum(int(x['amount_raw']) for x in packets_for_denom)
            all_priced=all(x['valuation_status']=='valued' for x in packets_for_denom)
            with localcontext() as ctx:
                ctx.prec=200
                usd=sum((Decimal(x['out_usd']) for x in packets_for_denom),Decimal(0)) if all_priced else ''
            assets.append(dict(universe_id=uid,date=day,chain=name,chain_id=p['chain_id'],denom=denom,
                               asset_base=first['asset_base'],symbol=first['asset_symbol'],decimals=first['decimals'],
                               coingecko_id=first['coingecko_id'],amount_raw=str(raw),
                               amount_display=str(decimal_amount(raw,first['decimals'])) if first['decimals']!='' else '',
                               sent_packets=len(packets_for_denom),out_usd=str(usd),valuation_status=first['valuation_status']))
        priced=[v for v in valued if v['valuation_status']=='valued']
        with localcontext() as ctx:
            ctx.prec=200
            usd=sum((Decimal(v['out_usd']) for v in priced),Decimal(0))
        unpriced=len(valued)-len(priced)
        row=dict(universe_id=uid,date=day,chain=name,chain_id=p['chain_id'],coverage_status=status,scope=p['scope'],
                 sent_packets=len(valued),priced_packets=len(priced),unpriced_packets=unpriced,valued_out_usd=str(usd),
                 total_out_usd=str(usd) if not unpriced else '',
                 failed_indexed_transactions=sum(page['failed_indexed_txs'] for page in d['pages']),
                 price_method='historical_daily_reference_not_execution_price')
        chains.append(row);key=(uid,day);groups.setdefault(key,{})[name]=row
        members=sorted(c['name'] for c in p['members'])
        if key in group_members and group_members[key]!=members:raise ValueError('Comparison universe mismatch')
        group_members[key]=members
    comparisons=[]
    for (uid,day),rows in sorted(groups.items()):
        if set(rows)==set(group_members[(uid,day)]):
            comparisons.append(dict(universe_id=uid,**compare_day(day,group_members[(uid,day)],rows)))
    for filename,rows,fields in [('criterion2_progress.csv',progress,PROGRESS_FIELDS),('ibc_out_chains_daily.csv',chains,CHAIN_FIELDS),
                                 ('ibc_out_assets_daily.csv',assets,ASSET_FIELDS),('ecosystem_out_usd_daily.csv',comparisons,COMPARISON_FIELDS)]:
        write_csv(Path(out)/filename,rows,fields)
    return progress,chains,assets,comparisons


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--fetch-prices',action='store_true');args=parser.parse_args()
    if args.fetch_prices:fetch_needed_prices()
    rows=exports();print(f'Criterion 2 exports: {len(rows[0])} chain-days, {len(rows[3])} common-day comparisons.')


if __name__=='__main__':main()
