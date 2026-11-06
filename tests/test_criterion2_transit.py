import base64
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cosmos_tx_decode import repeated,single
from criterion2_core import packets
from criterion2_receipts import received_packets
from criterion2_transit_evidence import links,pfm_sender,strict_json,topology,encode_field
from criterion2_transit_collect import batch,channel_proof,directory,raw_links
from criterion2_transit_export import classify_route,settlement

FIX=Path(__file__).parent/'fixtures'


class TransitProofTests(unittest.TestCase):
    def setUp(self):
        self.tx=json.loads((FIX/'transit_recv_forward.json').read_text())
        self.responses={r['id']:r['result'] for r in json.loads((FIX/'transit_historical_channels.json').read_text())}
    def result(self):
        return links(self.tx,received_packets('cosmoshub-4',self.tx),packets('cosmoshub-4',self.tx))
    def change(self,index,key,value):
        for a in self.tx['tx_result']['events'][index]['attributes']:
            if a['key']==key:a['value']=value;return
        self.fail('Missing fixture attribute')
    def test_known_pfm_address_and_real_first_hop(self):
        self.assertEqual(pfm_sender('channel-569','neutron1pu2dcs4ntj3jfw905g0jqc898yxl7eczshwpqd'),'cosmos1wwwsp476sqgwhfkz6enfk9v22wevnc79j9va9q')
        rows,diags=self.result();self.assertEqual(len(rows),1);self.assertEqual(diags[0]['status'],'linked')
        self.assertEqual(rows[0]['amount_raw'],'4298381');self.assertEqual(rows[0]['bank_denom'],'uatom')
        a=topology(self.responses['channel-569-Channel'],self.responses['channel-569-ChannelClientState'],self.tx['height'])
        b=topology(self.responses['channel-141-Channel'],self.responses['channel-141-ChannelClientState'],self.tx['height'])
        self.assertEqual(classify_route(rows[0],a,b),('cross_chain','neutron-1','osmosis-1'))
    def test_same_amount_in_another_message_is_not_a_link(self):
        self.change(24,'msg_index','2');self.assertEqual(self.result()[0],[])
    def test_unscoped_bank_event_cannot_prove_forward(self):
        for index in (15,18,20,22):
            with self.subTest(index=index):
                original=copy.deepcopy(self.tx);self.change(index,'msg_index','');self.assertEqual(self.result()[0],[]);self.tx=original
    def test_missing_bank_debit_not_replaced_by_amount_match(self):
        self.change(20,'amount','1uatom');self.assertEqual(self.result()[0],[])
    def test_noop_or_missing_receive_response_not_counted(self):
        encoded=base64.b64decode(self.tx['tx_result']['data']);responses=repeated(encoded,2)
        for response in (b'\x08\x01',b''):
            altered=[]
            for item in responses:
                url=single(item,1)
                altered.append(encode_field(1,url)+encode_field(2,response if url.endswith(b'MsgRecvPacketResponse') else single(item,2)))
            self.tx['tx_result']['data']=base64.b64encode(b''.join(encode_field(2,x) for x in altered)).decode()
            self.assertEqual(self.result()[0],[])
    def test_hash_or_failed_execution_rejected(self):
        self.tx['hash']='0'*64
        with self.assertRaises(ValueError):self.result()
        self.setUp();self.tx['tx_result']['code']=1
        with self.assertRaises(ValueError):self.result()
    def test_duplicate_json_rejected(self):
        with self.assertRaises(ValueError):strict_json('{"forward":{},"forward":{}}')
    def test_packet_ledger_mismatch_rejected(self):
        parents=received_packets('cosmoshub-4',self.tx);parents[0]['amount_raw']='999'
        with self.assertRaises(ValueError):links(self.tx,parents,packets('cosmoshub-4',self.tx))
    def test_wrong_historical_height_rejected(self):
        a=self.responses['channel-569-Channel'];a['response']['height']='1'
        with self.assertRaises(ValueError):topology(a,self.responses['channel-569-ChannelClientState'],self.tx['height'])
    def test_same_remote_chain_different_channels_is_return(self):
        r=self.result()[0][0]
        a=topology(self.responses['channel-569-Channel'],self.responses['channel-569-ChannelClientState'],r['height'])
        b=topology(self.responses['channel-141-Channel'],self.responses['channel-141-ChannelClientState'],r['height']);b['chain_id']=a['chain_id']
        self.assertEqual(classify_route(r,a,b)[0],'return_to_source')
        self.assertEqual(settlement('return_to_source','success_confirmed','success_confirmed'),'return_to_source')
        self.assertEqual(classify_route(r,None,b),('unknown','',''))
    def test_wrong_channel_counterpart_or_connection_rejected(self):
        r=self.result()[0][0]
        a=topology(self.responses['channel-569-Channel'],self.responses['channel-569-ChannelClientState'],r['height'])
        b=topology(self.responses['channel-141-Channel'],self.responses['channel-141-ChannelClientState'],r['height'])
        for field in ('counterparty_channel','connection_id'):
            changed=dict(b,**{field:'wrong'})
            with self.assertRaises(ValueError):classify_route(r,a,changed)
    def test_parent_success_after_child_timeout_is_unresolved_retry(self):
        self.assertEqual(settlement('cross_chain','success_confirmed','timeout_confirmed'),'unresolved')
        self.assertEqual(settlement('cross_chain','success_confirmed','success_confirmed'),'confirmed_transit')
        self.assertEqual(settlement('unknown','success_confirmed','success_confirmed'),'topology_unknown')
        self.assertEqual(settlement('cross_chain','error_acknowledged','error_acknowledged'),'failed_forward')
        self.assertEqual(settlement('cross_chain','conflicts','success_confirmed'),'unresolved')
    def test_batch_ids_are_matched_not_position(self):
        calls=[{'id':'a'},{'id':'b'}]
        with patch('criterion2_transit_collect.http.get_any',return_value=([{'id':'b','result':2},{'id':'a','result':1}],'')):
            self.assertEqual(batch('https://rpc',calls),{'a':1,'b':2})
        for rows in ([{'id':'a','result':1}], [{'id':'a','result':1},{'id':'a','result':2}], [{'id':'a','result':1},{'id':'b','error':{'code':-1}}]):
            with patch('criterion2_transit_collect.http.get_any',return_value=(rows,'')):
                with self.assertRaises(ValueError):batch('https://rpc',calls)
    def test_missing_channel_cache_stays_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:self.assertIsNone(channel_proof(Path(tmp),'1','channel-1'))


if __name__=='__main__':unittest.main()

class TransitExportTests(unittest.TestCase):
    def setUp(self):
        from criterion2_transit_export import inspect
        from test_criterion2 import mapping
        self.inspect=inspect
        tx=json.loads((FIX/'transit_recv_forward.json').read_text());self.parent=received_packets('cosmoshub-4',tx)[0];self.child=packets('cosmoshub-4',tx)[0]
        self.records,self.diagnostics=links(tx,[self.parent],[self.child])
        self.c=dict(plan=dict(universe_id='test',date='2026-11-05',chain_id='cosmoshub-4',candidate_hashes=[tx['hash']]),incoming=dict(packets=[self.parent],plan=dict(assets=dict(mapping=mapping()))),outgoing=dict(packets=[self.child]),parents={tx['hash']:[self.parent]})
        self.streams=[]
        for scope in ('ics20_v1_write_ack','ics20_v1_source_ack','ics20_v1_timeout','ics20_v1_timeout_close'):
            records=[]
            packet=self.parent if scope=='ics20_v1_write_ack' else self.child
            if scope in ('ics20_v1_write_ack','ics20_v1_source_ack'):
                records=[dict(packet,packet_id='evidence/'+scope,height=str(int(tx['height'])+1),event_index=1,
                    execution_status='committed_write_ack' if scope=='ics20_v1_write_ack' else 'executed',event_type='write_acknowledgement' if scope=='ics20_v1_write_ack' else 'acknowledge_packet',ack_status='success',acknowledgement_sha256='abc',refund_status='not_applicable')]
            self.streams.append(dict(plan=dict(chain_id='cosmoshub-4',date='2026-11-05',scope=scope),records=records,complete=True))
        responses={r['id']:r['result'] for r in json.loads((FIX/'transit_historical_channels.json').read_text())}
        self.channels={ch:topology(responses[ch+'-Channel'],responses[ch+'-ChannelClientState'],tx['height']) for ch in ('channel-569','channel-141')}
        self.tmp=tempfile.TemporaryDirectory()
    def tearDown(self):self.tmp.cleanup()
    def result(self):
        with patch('criterion2_transit_export.raw_links',return_value=(self.records,self.diagnostics,1)),patch('criterion2_transit_export.channel_proof',side_effect=lambda folder,h,ch:self.channels.get(ch)):
            return self.inspect(self.c,self.streams,root=self.tmp.name,prices_root=self.tmp.name)
    def test_confirmed_amount_once_and_missing_price_not_zero(self):
        summary,assets,routes,packets=self.result();self.assertEqual(summary['confirmed_transit'],1)
        self.assertEqual(assets[0]['amount_raw'],'4298381');self.assertEqual(assets[0]['confirmed_usd'],'');self.assertEqual(assets[0]['amount_display'],'4.298381')
        self.assertEqual(routes[0]['confirmed_packets'],1);self.assertEqual(packets[0]['status'],'confirmed_transit')
    def test_incomplete_lifecycle_stream_blocks_confirmation(self):
        self.streams[-1]['complete']=False
        summary,assets,_,_=self.result();self.assertEqual(summary['confirmed_transit'],0);self.assertEqual(summary['unresolved'],1);self.assertEqual(assets,[]);self.assertEqual(summary['evidence_through_utc'],'')
    def test_late_or_incomplete_journal_evidence_not_applied(self):
        late=copy.deepcopy(self.streams[0]);late['plan']['date']='2026-11-06';late['complete']=False
        self.streams[0]['records']=[];self.streams.append(late)
        summary,assets,_,_=self.result();self.assertEqual(summary['confirmed_transit'],0);self.assertEqual(assets,[])
    def test_unknown_topology_is_not_guessed(self):
        self.channels.pop('channel-141');summary,assets,_,_=self.result()
        self.assertEqual(summary['topology_unknown'],1);self.assertEqual(summary['collection_status'],'incomplete');self.assertEqual(assets,[])
    def test_successful_return_is_separate_from_transit_routes(self):
        self.channels['channel-141']['chain_id']='neutron-1'
        summary,assets,routes,_=self.result()
        self.assertEqual(summary['return_to_source'],1);self.assertEqual(summary['confirmed_transit'],0);self.assertEqual(routes,[]);self.assertEqual(assets,[])
    def test_duplicate_parent_or_child_rejected(self):
        self.records.append(dict(self.records[0]))
        with self.assertRaises(ValueError):self.result()
    def test_append_only_bundle_and_resume(self):
        from criterion2_transit_collect import save_store,store
        folder=Path(self.tmp.name);v=dict(transactions={'a':{'proof':1}},channels={})
        save_store(folder,v);self.assertEqual(store(folder),v)
        with self.assertRaises(ValueError):save_store(folder,dict(transactions={},channels={}))
        v['transactions']['b']={'proof':2};save_store(folder,v);self.assertEqual(store(folder)['transactions']['a'],{'proof':1})
