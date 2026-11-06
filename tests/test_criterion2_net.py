import copy
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from criterion2_core import SCOPE,IN_SCOPE
from criterion2_net import calculate,metadata,retain_history,CHAIN
from test_criterion2 import MEMBERS,mapping,price
from criterion2_pipeline import atomic

DAY='2026-11-05';OUT='out';IN='in';UID='net'
SCOPES=['ics20_v1_source_ack','ics20_v1_timeout','ics20_v1_timeout_close','ics20_v1_write_ack']


def packet(chain,seq,denom='uatom',amount='1000000',direction='out'):
    return dict(chain_id=chain['chain_id'],packet_id=chain['chain_id']+'/'+direction+'/'+str(seq),height='11',tx_index=0,event_index=1,
                source_port='transfer',source_channel='channel-1',destination_port='transfer',destination_channel='channel-2',sequence=str(seq),
                packet_data_sha256=hashlib.sha256(str(seq).encode()).hexdigest(),amount_raw=amount,denom=denom,
                receipt_status='unconfirmed_ack_in_receive_tx')


def inputs():
    cfg=dict(chains=copy.deepcopy(MEMBERS));outs=[];ins=[];journals=[]
    for c in MEMBERS:
        for dr,dest,uid,scope in [('out',outs,OUT,SCOPE),('in',ins,IN,IN_SCOPE)]:
            p=dict(universe_id=uid,date=DAY,chain=c['name'],chain_id=c['chain_id'],scope=scope,members=MEMBERS,start_height=11,end_height=13,assets=dict(mapping=mapping()))
            dest.append(dict(plan=p,complete=True,packets=[]))
        for scope in SCOPES:journals.append(dict(plan=dict(chain=c['name'],chain_id=c['chain_id'],date=DAY,scope=scope),complete=True,records=[]))
    return cfg,outs,ins,journals


def add(d,journals,seq,amount='1000000',denom='uatom',status='success',refund=False):
    p=d['plan'];direction='out' if p['scope']==SCOPE else 'in';r=packet(p,seq,denom,amount,direction);d['packets'].append(r)
    if status=='unknown':return r
    scope='ics20_v1_source_ack' if direction=='out' else 'ics20_v1_write_ack'
    journal=next(j for j in journals if j['plan']['chain_id']==p['chain_id'] and j['plan']['scope']==scope and j['plan']['date']==DAY)
    e=dict(r,packet_id='evidence/'+r['packet_id'],height='12',event_index=3,
           event_type='acknowledge_packet' if direction=='out' else 'write_acknowledgement',execution_status='executed' if direction=='out' else 'committed_write_ack',
           ack_status=status,acknowledgement_sha256=hashlib.sha256(status.encode()).hexdigest(),refund_status='verified_bank_credit' if refund else 'not_verified',
           refund_amount_raw=amount if refund else '',refund_denom_local=denom if refund else '',refund_evidence_sha256='a'*64 if refund else '')
    journal['records'].append(e);return r


class NetTests(unittest.TestCase):
    def setUp(self):
        self.cfg,self.outs,self.ins,self.journals=inputs();self.tmp=tempfile.TemporaryDirectory();self.prices=Path(self.tmp.name)
        for asset in ['cosmos','injective-protocol']:atomic(self.prices/DAY/(asset+'.json'),price(asset,'2'))
    def tearDown(self):self.tmp.cleanup()
    def result(self):return calculate(self.outs,self.ins,self.journals,self.cfg,OUT,IN,UID,self.prices)
    def test_success_in_minus_success_out_and_true_zero_absent_direction(self):
        add(self.ins[0],self.journals,101,'3000000');add(self.outs[0],self.journals,1,'1000000')
        chains,assets=self.result();r=assets[0];self.assertEqual(r['net_raw'],'2000000');self.assertEqual(r['net_amount'],'2');self.assertEqual(r['net_usd'],'4')
        self.assertEqual(chains[0]['total_net_usd'],'4');self.assertEqual(chains[1]['total_net_usd'],'0')
        self.assertEqual(chains[0]['evidence_through_utc'],'2026-11-06T00:00:00Z')
    def test_failed_send_never_subtracted_or_refunded_twice(self):
        add(self.ins[0],self.journals,101,'3000000');add(self.outs[0],self.journals,1,'1000000',status='error',refund=True)
        chains,assets=self.result();self.assertEqual(assets[0]['net_amount'],'3');self.assertEqual(chains[0]['excluded_packets'],1)
        self.assertEqual(chains[0]['total_net_usd'],'6')
    def test_unknown_packet_blocks_only_its_asset_and_total_usd(self):
        add(self.ins[0],self.journals,101,'3000000');add(self.outs[0],self.journals,1,status='unknown')
        add(self.ins[0],self.journals,102,'1000000000000000000',denom='inj')
        chains,assets=self.result();atom=next(r for r in assets if r['denom']=='uatom');inj=next(r for r in assets if r['denom']=='inj')
        self.assertEqual(atom['net_raw'],'');self.assertEqual(atom['known_net_raw'],'3000000');self.assertEqual(atom['net_usd'],'')
        self.assertEqual(inj['net_amount'],'1');self.assertEqual(chains[0]['valued_net_usd'],'2');self.assertEqual(chains[0]['total_net_usd'],'')
    def test_18_decimals_and_cancellation_above_float_precision(self):
        big=10**70
        add(self.ins[0],self.journals,101,str(big+1),denom='inj');add(self.outs[0],self.journals,1,str(big),denom='inj')
        _,assets=self.result();self.assertEqual(assets[0]['net_raw'],'1');self.assertEqual(assets[0]['net_amount'],'1E-18');self.assertEqual(assets[0]['net_usd'],'2E-18')
    def test_different_denominations_with_same_symbol_are_never_combined(self):
        for d in [self.ins[0],self.outs[0]]:
            d['plan']['assets']['mapping']['inj']['symbol']='ATOM'
        add(self.ins[0],self.journals,101,'1000000');add(self.outs[0],self.journals,1,'1000000000000000000',denom='inj')
        _,assets=self.result();self.assertEqual(len(assets),2);self.assertEqual({r['net_raw'] for r in assets},{'1000000','-1000000000000000000'})
    def test_trace_and_hashed_local_bank_denom_match_without_symbol_guess(self):
        trace='transfer/channel-7/uatom';denom='ibc/'+hashlib.sha256(trace.encode()).hexdigest().upper()
        for d in [self.ins[0],self.outs[0]]:d['plan']['assets']['mapping'][denom]=dict(base=denom,symbol='ATOM',decimals=6,coingecko_id='cosmos')
        add(self.ins[0],self.journals,101,'2000000',denom=trace);add(self.outs[0],self.journals,1,'1000000',denom=denom)
        _,assets=self.result();self.assertEqual(len(assets),1);self.assertEqual(assets[0]['net_amount'],'1')
    def test_missing_price_does_not_remove_one_side_from_subtraction(self):
        add(self.ins[0],self.journals,101,'3000000',denom='inj');add(self.outs[0],self.journals,1,'2000000000000000000',denom='inj')
        (self.prices/DAY/'injective-protocol.json').unlink()
        chains,assets=self.result();self.assertEqual(assets[0]['net_raw'],'-1999999999997000000');self.assertEqual(assets[0]['net_usd'],'');self.assertEqual(chains[0]['valued_net_usd'],'');self.assertEqual(chains[0]['total_net_usd'],'')
    def test_units_conflict_is_not_resolved_using_native_token_defaults(self):
        add(self.ins[0],self.journals,101,'3000000');self.ins[0]['plan']['assets']['mapping']['uatom']['decimals']=18
        chains,assets=self.result();self.assertEqual(assets[0]['metadata_status'],'conflict');self.assertEqual(assets[0]['net_amount'],'');self.assertEqual(chains[0]['total_net_usd'],'')
    def test_missing_direction_chain_or_lifecycle_is_not_a_measured_zero(self):
        for what in ['direction','journal']:
            with self.subTest(what=what):
                cfg,outs,ins,js=inputs()
                if what=='direction':ins[-1]['complete']=False
                else:js[-1]['complete']=False
                chains,assets=calculate(outs,ins,js,cfg,OUT,IN,UID,self.prices)
                self.assertEqual(assets,[]);self.assertTrue(all(r['total_net_usd']=='' for r in chains))
    def test_later_proof_is_cut_off_at_common_boundary_then_updates_same_cohort(self):
        r=add(self.outs[0],self.journals,1,status='unknown')
        add(self.ins[0],self.journals,101,'3000000')
        later=dict(self.journals[0]);later['plan']=dict(later['plan'],date='2026-11-06');later['records']=[]
        e=dict(self.journals[3]['records'][0]);e.update(r);e.update(packet_id='later',height='15',event_type='acknowledge_packet',execution_status='executed')
        later['records'].append(e);self.journals.append(later)
        chains,assets=self.result();self.assertEqual(assets[0]['net_amount'],'')
        for j in list(self.journals[:16]):
            p=j['plan']
            if p['chain_id']==later['plan']['chain_id'] and p['scope']==later['plan']['scope']:continue
            self.journals.append(dict(plan=dict(p,date='2026-11-06'),complete=True,records=[]))
        chains,assets=self.result();self.assertEqual(assets[0]['net_amount'],'2');self.assertEqual(assets[0]['date'],DAY);self.assertEqual(chains[0]['evidence_through_utc'],'2026-11-07T00:00:00Z')
    def test_missing_payload_of_unknown_receipt_blocks_all_asset_totals(self):
        add(self.ins[0],self.journals,101,'3000000');r=add(self.ins[0],self.journals,102,status='unknown');r.update(amount_raw='',denom='',packet_data_sha256='')
        chains,assets=self.result();self.assertEqual(chains[0]['unknown_payload_packets'],1);self.assertEqual(assets[0]['net_amount'],'');self.assertEqual(chains[0]['total_net_usd'],'')
    def test_wrong_height_boundaries_fail_closed(self):
        self.ins[0]['plan']['end_height']=14
        with self.assertRaisesRegex(ValueError,'boundaries'):self.result()
    def test_duplicates_and_old_universes_cannot_mix(self):
        self.outs.append(copy.deepcopy(self.outs[0]))
        with self.assertRaisesRegex(ValueError,'Duplicate'):self.result()
        self.outs[-1]['plan']['universe_id']='old'
        chains,_=self.result();self.assertEqual(chains[0]['observed_packets'],0)
    def test_other_universe_csv_history_retained_and_current_coverage_cannot_regress(self):
        from criterion2_export import write_csv
        rows,_=self.result();path=self.prices/'nets.csv'
        old=dict(rows[0],universe_id='old');write_csv(path,[old]+rows,CHAIN)
        combined=retain_history(path,rows,CHAIN,UID)
        self.assertEqual(len(combined),5);self.assertEqual(next(r for r in combined if r['universe_id']=='old')['date'],DAY)
        with self.assertRaisesRegex(ValueError,'missing'):retain_history(path,rows[1:],CHAIN,UID)
        changed=copy.deepcopy(rows);changed[0]['evidence_through_utc']=''
        with self.assertRaisesRegex(ValueError,'regressed'):retain_history(path,changed,CHAIN,UID)
    def test_different_utc_days_cannot_be_subtracted(self):
        self.ins[0]['plan']['date']='2026-11-06'
        chains,assets=self.result();self.assertEqual(assets,[]);self.assertTrue(all(r['total_net_usd']=='' for r in chains))
    def test_price_fetch_can_cover_later_ack_assets_without_confirming_receipt(self):
        from criterion2_export import fetch_needed_prices
        from criterion2_pipeline import DATA
        add(self.ins[0],self.journals,101,status='unknown')
        with patch('criterion2_export.all_days',side_effect=[self.outs,self.ins]),patch('criterion2_export.fetch_price') as fetch:
            fetch_needed_prices(DATA,budget=5,prices_root=self.prices)
        fetch.assert_called_once_with(DAY,'cosmos',self.prices)
        chains,_=self.result();self.assertEqual(chains[0]['total_net_usd'],'')
    def test_historical_price_wrong_day_is_rejected(self):
        add(self.ins[0],self.journals,101);p=price();p['date']='2026-11-04';atomic(self.prices/DAY/'cosmos.json',p)
        with self.assertRaises(ValueError):self.result()


if __name__=='__main__':unittest.main()
