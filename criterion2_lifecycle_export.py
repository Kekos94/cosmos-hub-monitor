"""Packet cohorts reconciled against later local-chain evidence; never net flows."""
import csv
import gzip
import io
from collections import defaultdict
from datetime import date,timedelta
from pathlib import Path

from criterion2_export import all_days,write_csv
from criterion2_pipeline import ROOT,DATA,IN_DATA
from criterion2_lifecycle_collect import LIFE_ROOT,journals,config
from criterion2_lifecycle_evidence import bank_denom
from criterion2_receipts import SUCCESS,ERROR

DAILY='universe_id date chain chain_id direction tracked_packets success_confirmed error_acknowledged timeout_confirmed awaiting_evidence unsupported_ack conflicts refunds_verified refunds_unverified coverage_complete complete_through_utc latest_evidence_day'.split()
PACKET='universe_id date chain chain_id direction packet_id height outcome evidence_ids refund_status refund_amount_raw refund_denom_local refund_evidence_sha256 coverage_complete complete_through_utc latest_evidence_day'.split()
PROGRESS='universe_id date chain chain_id scope coverage_status scanned_heights expected_heights events source'.split()
REQUIRED={'out':('ics20_v1_source_ack','ics20_v1_timeout','ics20_v1_timeout_close'),'in':('ics20_v1_write_ack',)}


def key(p):
    return (p['chain_id'],p.get('source_port','transfer'),p['source_channel'],p['destination_port'],p['destination_channel'],str(int(p['sequence'])))


def reconcile(packet,evidence,direction):
    """Only executed source operations or committed destination acknowledgements count."""
    eligible=[];conflict=False
    for e in evidence:
        if key(e)!=key(packet):continue
        if e['execution_status'] not in ('executed','committed_write_ack'):continue
        if (int(e['height']),int(e['tx_index']),int(e['event_index'])) < (int(packet['height']),int(packet['tx_index']),int(packet['event_index'])):
            continue # An earlier, unrelated/replayed event cannot settle this observation.
        if not packet.get('packet_data_sha256') or not e['packet_data_sha256']:continue
        if e['packet_data_sha256']!=packet['packet_data_sha256']:conflict=True;continue
        eligible.append(e)
    outcomes=set()
    if direction=='in':
        if packet.get('receipt_status')==SUCCESS:outcomes.add('success_confirmed')
        if packet.get('receipt_status')==ERROR:outcomes.add('error_acknowledged')
    for e in eligible:
        if e['event_type'].startswith('timeout'):outcomes.add('timeout_confirmed')
        elif e['ack_status']=='success':outcomes.add('success_confirmed')
        elif e['ack_status']=='error':outcomes.add('error_acknowledged')
    hashes={e['acknowledgement_sha256'] for e in eligible if e['acknowledgement_sha256']}
    # A packet has one acknowledgement; multiple hashes are contradictory evidence.
    if len(outcomes)>1 or len(hashes)>1:conflict=True
    outcome='conflicts' if conflict else next(iter(outcomes)) if outcomes else 'unsupported_ack' if eligible else 'awaiting_evidence'
    refund=dict(refund_status='not_applicable',refund_amount_raw='',refund_denom_local='',refund_evidence_sha256='')
    if outcome in ('error_acknowledged','timeout_confirmed'):
        refund['refund_status']='source_chain_not_observed' if direction=='in' else 'not_verified'
        proofs=[e for e in eligible if e['refund_status']=='verified_bank_credit' and
                e['refund_amount_raw']==packet['amount_raw'] and e['refund_denom_local']==bank_denom(packet['denom'])]
        # A duplicated proof is fine; two different bank credits are ambiguous.
        unique={e['refund_evidence_sha256'] for e in proofs}
        if direction=='out' and len(unique)==1:
            e=proofs[0]
            refund={k:e[k] for k in refund}
    if outcome=='conflicts':refund['refund_status']='conflicting_evidence'
    return dict(latest_evidence_day=max((e.get('_journal_date','') for e in eligible),default=''),outcome=outcome,evidence_ids=';'.join(sorted({e['packet_id'] for e in eligible})),**refund)


def coverage(chain_id,cohort,stream_days,direction):
    ends=[]
    for scope in REQUIRED[direction]:
        day=cohort;last=''
        while stream_days.get((chain_id,scope,day),False):
            last=str(date.fromisoformat(day)+timedelta(days=1))+'T00:00:00Z'
            day=str(date.fromisoformat(day)+timedelta(days=1))
        ends.append(last)
    return ('true',min(ends)) if all(ends) else ('false','')


def exports(out_root=DATA,in_root=IN_DATA,life_root=LIFE_ROOT,out=ROOT):
    _,uid=config();evidence=defaultdict(list);stream_days={};progress=[]
    for d in journals(life_root):
        p=d['plan']
        if p['universe_id']!=uid:continue
        stream_days[(p['chain_id'],p['scope'],p['date'])]=d['complete']
        progress.append(dict(universe_id=uid,date=p['date'],chain=p['chain'],chain_id=p['chain_id'],scope=p['scope'],
          coverage_status='indexed_complete' if d['complete'] else 'incomplete',scanned_heights=d['next_height']-p['start_height'],
          expected_heights=max(0,p['end_height']-p['start_height']+1),events=len(d['records']),source=p['source']))
        for e in d['records']:evidence[key(e)].append(dict(e,_journal_date=p['date']))
    packet_rows=[];daily=[]
    for direction,root in [('out',out_root),('in',in_root)]:
        for d in all_days(root):
            if not d['complete']:continue
            p=d['plan'];covered,through=coverage(p['chain_id'],p['date'],stream_days,direction)
            base=dict(universe_id=p['universe_id'],date=p['date'],chain=p['chain'],chain_id=p['chain_id'],direction=direction)
            row=dict(base,tracked_packets=len(d['packets']),coverage_complete=covered,complete_through_utc=through,
                     latest_evidence_day='',**{k:0 for k in DAILY[6:14]})
            for packet in d['packets']:
                relevant=[e for e in evidence[key(packet)] if (e['event_type']=='write_acknowledgement')==(direction=='in')]
                status=reconcile(packet,relevant,direction)
                row[status['outcome']]+=1
                row['latest_evidence_day']=max(row['latest_evidence_day'],status['latest_evidence_day'])
                if status['refund_status']=='verified_bank_credit':row['refunds_verified']+=1
                elif status['outcome'] in ('error_acknowledged','timeout_confirmed'):row['refunds_unverified']+=1
                packet_rows.append(dict(base,packet_id=packet['packet_id'],height=packet['height'],coverage_complete=covered,
                                        complete_through_utc=through,**status))
            if sum(row[k] for k in ('success_confirmed','error_acknowledged','timeout_confirmed','awaiting_evidence','unsupported_ack','conflicts'))!=row['tracked_packets']:
                raise ValueError('Lifecycle cohort counts do not reconcile')
            daily.append(row)
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    write_csv(out/'ibc_lifecycle_daily.csv',daily,DAILY)
    write_csv(out/'ibc_lifecycle_progress.csv',progress,PROGRESS)
    buffer=io.StringIO(newline='');writer=csv.DictWriter(buffer,fieldnames=PACKET);writer.writeheader();writer.writerows(packet_rows)
    (out/'ibc_packet_lifecycle.csv.gz').write_bytes(gzip.compress(buffer.getvalue().encode(),mtime=0))
    return daily,progress


if __name__=='__main__':exports()
