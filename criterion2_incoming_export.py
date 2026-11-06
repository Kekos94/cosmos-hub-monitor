"""Separate reception exports; unknown/error acknowledgements never become inflows."""
from decimal import Decimal,localcontext
from pathlib import Path

from criterion2_core import IN_SCOPE,decimal_amount,resolve_asset,valued_packet
from criterion2_export import all_days,load_price,write_csv,PRICES
from criterion2_pipeline import ROOT,IN_DATA
from criterion2_receipts import SUCCESS,ERROR,UNKNOWN

PROGRESS='universe_id date chain chain_id coverage_status indexed_transactions_saved indexed_transactions_total observed_packets_saved source'.split()
CHAIN='universe_id date chain chain_id coverage_status scope observed_packets received_packets error_ack_packets unconfirmed_packets priced_packets unpriced_packets valued_in_usd total_in_usd receipt_evidence_status'.split()
ASSET='universe_id date chain chain_id denom asset_base symbol decimals coingecko_id amount_raw amount_display received_packets in_usd valuation_status'.split()
COMPARISON='universe_id date scope chains indexed_chains_complete observed_packets received_packets error_ack_packets unconfirmed_packets priced_packets unpriced_packets valued_in_usd total_in_usd hub_valued_in_usd hub_share_valued_usd_pct hub_share_total_usd_pct hub_share_confirmed_packet_count_pct valuation_status receipt_evidence_status index_coverage_caveat'.split()


def sum_usd(values):
    with localcontext() as ctx:
        ctx.prec=200
        return sum((Decimal(v['out_usd']) for v in values if v['valuation_status']=='valued'),Decimal(0))


def exports(root=IN_DATA,out=ROOT,prices_root=PRICES):
    progress=[];chains=[];assets=[];groups={};members={}
    for d in all_days(root):
        p=d['plan']
        if p['scope']!=IN_SCOPE:raise ValueError('Outgoing scope in incoming ledger')
        base=dict(universe_id=p['universe_id'],date=p['date'],chain=p['chain'],chain_id=p['chain_id'])
        progress.append(dict(base,coverage_status='indexed_complete' if d['complete'] else 'incomplete',
                             indexed_transactions_saved=d['scanned_txs'],indexed_transactions_total=d['total_count'] if d['total_count'] is not None else '',
                             observed_packets_saved=len(d['packets']),source=p['source']))
        if not d['complete']:continue
        packets=d['packets'];accepted=[r for r in packets if r['receipt_status']==SUCCESS]
        if any(r['receipt_status'] not in (SUCCESS,ERROR,UNKNOWN) for r in packets):raise ValueError('Unknown receipt evidence status')
        errors=sum(r['receipt_status']==ERROR for r in packets);unknown=sum(r['receipt_status']==UNKNOWN for r in packets)
        prices={};valued=[];by_denom={};mapping=p['assets']['mapping']
        for packet in accepted:
            asset=resolve_asset(packet['denom'],mapping)
            if asset and asset['coingecko_id'] and asset['coingecko_id'] not in prices:
                record=load_price(p['date'],asset['coingecko_id'],prices_root)
                if record is not None:prices[asset['coingecko_id']]=record
            v=valued_packet(packet,mapping,prices,p['date']);valued.append(v)
            by_denom.setdefault(packet['denom'],[]).append(v)
        for denom,values in sorted(by_denom.items()):
            first=values[0];raw=sum(int(r['amount_raw']) for r in values)
            all_priced=all(r['valuation_status']=='valued' for r in values)
            assets.append(dict(base,denom=denom,asset_base=first['asset_base'],symbol=first['asset_symbol'],decimals=first['decimals'],
                               coingecko_id=first['coingecko_id'],amount_raw=str(raw),
                               amount_display=str(decimal_amount(raw,first['decimals'])) if first['decimals']!='' else '',
                               received_packets=len(values),in_usd=str(sum_usd(values)) if all_priced else '',valuation_status=first['valuation_status']))
        priced=sum(r['valuation_status']=='valued' for r in valued);missing=len(valued)-priced;usd=sum_usd(valued)
        row=dict(base,coverage_status='indexed_complete',scope=IN_SCOPE,observed_packets=len(packets),received_packets=len(accepted),
                 error_ack_packets=errors,unconfirmed_packets=unknown,priced_packets=priced,unpriced_packets=missing,
                 valued_in_usd=str(usd),total_in_usd=str(usd) if missing==0 and unknown==0 else '',
                 receipt_evidence_status='receive_transaction_ack_only')
        chains.append(row);key=(p['universe_id'],p['date']);groups.setdefault(key,{})[p['chain']]=row
        expected=sorted(r['name'] for r in p['members'])
        if key in members and members[key]!=expected:raise ValueError('Incoming universe mismatch')
        members[key]=expected
    comparisons=[]
    with localcontext() as ctx:
        ctx.prec=200
        for (uid,day),rows in sorted(groups.items()):
            if set(rows)!=set(members[(uid,day)]):continue
            sums={k:sum(int(r[k]) for r in rows.values()) for k in ['observed_packets','received_packets','error_ack_packets','unconfirmed_packets','priced_packets','unpriced_packets']}
            usd=sum((Decimal(r['valued_in_usd']) for r in rows.values()),Decimal(0));hub=rows['cosmoshub']
            complete=sums['unconfirmed_packets']==0 and sums['unpriced_packets']==0
            comparisons.append(dict(universe_id=uid,date=day,scope=IN_SCOPE,chains=';'.join(members[(uid,day)]),indexed_chains_complete=len(rows),**sums,
                valued_in_usd=str(usd),total_in_usd=str(usd) if complete else '',hub_valued_in_usd=hub['valued_in_usd'],
                hub_share_valued_usd_pct=str(Decimal(hub['valued_in_usd'])/usd*100) if usd else '',
                hub_share_total_usd_pct=str(Decimal(hub['valued_in_usd'])/usd*100) if usd and complete else '',
                hub_share_confirmed_packet_count_pct=str(Decimal(hub['received_packets'])/sums['received_packets']*100) if sums['received_packets'] else '',
                valuation_status='complete' if complete else 'partial_confirmed_valued_subset',
                receipt_evidence_status='receive_transaction_ack_only',index_coverage_caveat='completeness_depends_on_provider_event_index'))
    for name,records,fields in [('criterion2_incoming_progress.csv',progress,PROGRESS),('ibc_in_chains_daily.csv',chains,CHAIN),
                                ('ibc_in_assets_daily.csv',assets,ASSET),('ecosystem_in_usd_daily.csv',comparisons,COMPARISON)]:
        write_csv(Path(out)/name,records,fields)
    return progress,chains,assets,comparisons


if __name__=='__main__':exports()
