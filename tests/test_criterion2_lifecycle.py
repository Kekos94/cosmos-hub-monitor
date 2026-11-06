import base64
import copy
import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from criterion2_core import packets
from criterion2_lifecycle_evidence import ack_result,records,refund_proof,bank_denom
from criterion2_lifecycle_export import reconcile,coverage,exports
from criterion2_lifecycle_collect import save,inspect
from test_criterion2 import tx,plan


def vi(n):
    out=b''
    while n>=128:out+=bytes([(n&127)|128]);n>>=7
    return out+bytes([n])


def field(n,b):return vi(n*8+2)+vi(len(b))+b

def anymsg(url,b):return field(1,url.encode())+field(2,b)


def terminal(kind='MsgAcknowledgement',ack=b'{"result":"AQ=="}',execution=2,authz=False):
    t=tx(1,height=12);a={i['key']:i['value'] for i in t['tx_result']['events'][0]['attributes']}
    p=vi(8)+vi(1)+b''.join(field(n,a[k].encode()) for n,k in [(2,'packet_src_port'),(3,'packet_src_channel'),(4,'packet_dst_port'),(5,'packet_dst_channel')])+field(6,bytes.fromhex(a['packet_data_hex']))
    url='/ibc.core.channel.v1.'+kind;body=field(1,p)
    if kind=='MsgAcknowledgement':body+=field(2,ack)
    response=vi(8)+vi(execution);request=anymsg(url,body)
    if authz:
        request=anymsg('/cosmos.authz.v1beta1.MsgExec',field(1,b'grantee')+field(2,request));url='/cosmos.authz.v1beta1.MsgExec';response=field(1,response)
    t['tx']=base64.b64encode(field(1,field(1,request))).decode()
    t['tx_result']['data']=base64.b64encode(field(2,anymsg(url+'Response',response))).decode()
    t['tx_result']['events'][0]['type']='acknowledge_packet' if kind=='MsgAcknowledgement' else 'timeout_on_close_packet' if kind=='MsgTimeoutOnClose' else 'timeout_packet'
    scope='ics20_v1_source_ack' if kind=='MsgAcknowledgement' else 'ics20_v1_timeout_close' if kind=='MsgTimeoutOnClose' else 'ics20_v1_timeout'
    return t,dict(scope=scope,chain_id='cosmoshub-4')


def original():return packets('cosmoshub-4',tx(1))[0]


def event(kind,**attrs):return dict(type=kind,attributes=[dict(key=k,value=v) for k,v in attrs.items()])


class LifecycleTests(unittest.TestCase):
    def test_known_ack_schemas_only(self):
        success=b'{"result":"AQ=="}';error=b'{"error":"rejected"}'
        self.assertEqual(ack_result(success)[0],'success');self.assertEqual(ack_result(error)[0],'error')
        for inner in [success,error]:
            hook=json.dumps(dict(result=base64.b64encode(json.dumps(dict(contract_result=None,ibc_ack=base64.b64encode(inner).decode())).encode()).decode())).encode()
            self.assertEqual(ack_result(hook)[0],'success' if inner==success else 'unknown')
            fee=json.dumps(dict(app_acknowledgement=base64.b64encode(inner).decode(),forward_relayer_address='r',underlying_app_success=inner==success)).encode()
            self.assertEqual(ack_result(fee)[0],'success' if inner==success else 'error')
        for raw in [b'{"success":true}',b'{"result":""}',b'[]',b'{"error":""}',b'x'*262145]:self.assertEqual(ack_result(raw)[0],'unknown')
    def test_noop_missing_response_and_failed_tx_cannot_confirm(self):
        for execution in [0,1,3]:
            t,p=terminal(execution=execution);e=records(p,t)
            self.assertEqual(reconcile(original(),e,'out')['outcome'],'awaiting_evidence')
        t,p=terminal();t['tx_result'].pop('data');self.assertEqual(records(p,t)[0]['execution_status'],'unverified')
        t['tx_result']['code']=4;self.assertEqual(records(p,t),[])
    def test_executed_ack_in_later_tx_and_authz(self):
        for authz in [False,True]:
            t,p=terminal(authz=authz);e=records(p,t)
            self.assertEqual(reconcile(original(),e,'out')['outcome'],'success_confirmed')
    def test_identity_payload_height_and_duplicates(self):
        t,p=terminal();e=records(p,t)[0]
        self.assertEqual(reconcile(original(),[e,e],'out')['outcome'],'success_confirmed')
        for key,value in [('chain_id','another'),('source_channel','channel-99'),('destination_port','wasm.remote'),('sequence','2'),('height','10')]:
            changed=dict(e,**{key:value});self.assertEqual(reconcile(original(),[changed],'out')['outcome'],'awaiting_evidence')
        changed=dict(e,packet_data_sha256='0'*64);self.assertEqual(reconcile(original(),[changed],'out')['outcome'],'conflicts')
    def test_contradictory_outcomes_do_not_confirm(self):
        t,p=terminal();e=records(p,t)[0];other=dict(e,ack_status='error',acknowledgement_sha256='f'*64)
        self.assertEqual(reconcile(original(),[e,other],'out')['outcome'],'conflicts')
    def test_timeout_and_error_do_not_imply_refund(self):
        for kind in ['MsgTimeout','MsgTimeoutOnClose','MsgAcknowledgement']:
            t,p=terminal(kind,ack=b'{"error":"rejected"}');r=reconcile(original(),records(p,t),'out')
            self.assertEqual(r['refund_status'],'not_verified');self.assertEqual(r['refund_amount_raw'],'')
    def test_refund_requires_scoped_application_and_bank_movements(self):
        data=dict(sender='sender',amount='1000000',denom='uatom');msg=dict(outer=0,response_result=2)
        ev=[event('timeout',msg_index='0',refund_receiver='sender',refund_amount='1000000',refund_denom='uatom'),
            event('transfer',msg_index='0',sender='escrow',recipient='sender',amount='1000000uatom'),
            event('coin_received',msg_index='0',receiver='sender',amount='1000000uatom'),
            event('coin_spent',msg_index='0',spender='escrow',amount='1000000uatom')]
        proof=lambda es,peers=1:refund_proof(es,msg,data,'unknown','timeout_packet',peers)
        self.assertEqual(proof(ev)['refund_status'],'verified_bank_credit')
        self.assertEqual(proof(ev,2)['refund_status'],'not_verified')
        for i in range(4):self.assertEqual(proof(ev[:i]+ev[i+1:])['refund_status'],'not_verified')
        for attr in ['msg_index','recipient','amount']:
            changed=copy.deepcopy(ev)
            for a in changed[1]['attributes']:
                if a['key']==attr:a['value']='wrong'
            self.assertEqual(proof(changed)['refund_status'],'not_verified')
    def test_incoming_later_write_ack_and_remote_refund_unknown(self):
        packet=dict(original(),source_port='transfer',receipt_status='unconfirmed_ack_in_receive_tx')
        t,p=terminal();e=records(p,t)[0];e.update(event_type='write_acknowledgement',execution_status='committed_write_ack',ack_status='error')
        r=reconcile(packet,[e],'in');self.assertEqual(r['outcome'],'error_acknowledged');self.assertEqual(r['refund_status'],'source_chain_not_observed')
    def test_coverage_requires_all_streams_and_no_gap(self):
        days={('c',s,'2026-11-05'):True for s in ['ics20_v1_source_ack','ics20_v1_timeout','ics20_v1_timeout_close']}
        days[('c','ics20_v1_source_ack','2026-11-07')]=True
        self.assertEqual(coverage('c','2026-11-05',days,'out'),('true','2026-11-06T00:00:00Z'))
        days[('c','ics20_v1_timeout','2026-11-05')]=False
        self.assertEqual(coverage('c','2026-11-05',days,'out'),('false',''))
    def test_journal_cannot_claim_full_coverage_without_ranges(self):
        p=plan();p.update(scope='ics20_v1_timeout',query_mode='height_ranges')
        from criterion2_core import search_query
        p['query']=search_query(p['start_height'],p['end_height'],p['scope'])
        state=dict(schema_version=1,plan=p,ranges=[],complete=False)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'journal.json.gz';save(path,state)
            changed=copy.deepcopy(state);changed['complete']=True
            with self.assertRaises(ValueError):save(path,changed)
            changed=copy.deepcopy(state);changed['plan']['observed_at']='different'
            with self.assertRaises(ValueError):save(path,changed)
    def test_later_day_proof_keeps_original_cohort_and_trace(self):
        t,p=terminal();e=records(p,t)[0];e.update(height='15',_journal_date='2026-11-06')
        packet=original();r=reconcile(packet,[e],'out')
        self.assertEqual(r['outcome'],'success_confirmed');self.assertEqual(r['latest_evidence_day'],'2026-11-06')
        self.assertEqual(packet['height'],'11')
    def test_complete_journal_immutable_and_partial_append_only(self):
        from criterion2_ranges import fragment
        from criterion2_core import search_query
        p=plan();p.update(scope='ics20_v1_timeout',query_mode='height_ranges');p['query']=search_query(p['start_height'],p['end_height'],p['scope'])
        first=fragment(p,11,11,[],'indexed_single_response',0)
        state=dict(schema_version=1,plan=p,ranges=[first],complete=False)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'j.json.gz';save(path,state)
            removed=copy.deepcopy(state);removed['ranges']=[]
            with self.assertRaises(ValueError):save(path,removed)
            state['ranges'].append(fragment(p,12,13,[],'indexed_single_response',0));state['complete']=True;save(path,state)
            changed=copy.deepcopy(state);changed['ranges'][0]['observed_at']='later'
            with self.assertRaises(ValueError):save(path,changed)
            self.assertTrue(inspect(state)['complete'])
    def test_refund_must_also_match_original_units(self):
        t,p=terminal(ack=b'{"error":"rejected"}');e=records(p,t)[0]
        e.update(refund_status='verified_bank_credit',refund_amount_raw='1000000',refund_denom_local='uatom',refund_evidence_sha256='a'*64)
        self.assertEqual(reconcile(original(),[e],'out')['refund_status'],'verified_bank_credit')
        for changes in [dict(refund_amount_raw='999999'),dict(refund_denom_local='inj')]:
            self.assertEqual(reconcile(original(),[dict(e,**changes)],'out')['refund_status'],'not_verified')
    def test_export_empty_evidence_is_not_a_complete_zero(self):
        from criterion2_pipeline import write_once,collect_pages
        from test_criterion2 import FakeClient
        import time
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);client=FakeClient();p=plan(client);folder=root/'out'/'u'/'c'/'d';write_once(folder/'plan.json',p);collect_pages(folder,client,deadline=time.monotonic()+10)
            with patch('criterion2_lifecycle_export.journals',return_value=iter([])):
                daily,_=exports(root/'out',root/'in',root/'life',root)
            self.assertEqual(daily[0]['awaiting_evidence'],3);self.assertEqual(daily[0]['coverage_complete'],'false')
            rows=gzip.decompress((root/'ibc_packet_lifecycle.csv.gz').read_bytes()).decode();self.assertIn('awaiting_evidence',rows)


if __name__=='__main__':unittest.main()
