import base64
import copy
import json
import re
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from criterion2_core import IN_SCOPE,verify_search_tx,resolve_asset,search_query
from criterion2_pipeline import build_plan,write_once,collect_pages,inspect_day,incoming_config,load_config
from criterion2_receipts import received_packets,SUCCESS,ERROR,UNKNOWN
from criterion2_incoming_export import exports
from criterion2_export import price_path
from test_criterion2 import tx,FakeClient,DAY,MEMBERS,mapping,price,TIMES


def received(seq,ack='success',denom='transfer/channel-1/uatom'):
    r=tx(seq,denom=denom);event=r['tx_result']['events'][0];event['type']='recv_packet'
    if ack is not None:
        attrs=copy.deepcopy(event['attributes'])
        value={'result':'AQ=='} if ack=='success' else {'error':'application rejected'} if ack=='error' else {'result':base64.b64encode(b'unknown middleware').decode()}
        attrs.append(dict(key='packet_ack_hex',value=json.dumps(value).encode().hex()))
        r['tx_result']['events'].append(dict(type='write_acknowledgement',attributes=attrs))
    return r


def incoming_plan(client):
    p=build_plan(client,DAY,'incoming-test',dict(mapping=mapping(),source='registry'),scope=IN_SCOPE,members=MEMBERS)
    p['query_mode']='height_ranges'
    return p


class IncomingTests(unittest.TestCase):
    def test_receive_and_ack_success_are_separate_requirements(self):
        for ack,expected in [('success',SUCCESS),('error',ERROR),(None,UNKNOWN),('custom',UNKNOWN)]:
            with self.subTest(ack=ack):
                r=received_packets('cosmoshub-4',received(1,ack))[0]
                self.assertEqual(r['receipt_status'],expected)
                self.assertEqual(r['packet_id'],'cosmoshub-4/transfer/channel-2/1')
    def test_sending_and_failed_receiving_are_not_incoming(self):
        self.assertEqual(received_packets('cosmoshub-4',tx(1)),[])
        r=received(1);r['tx_result']['code']=5
        self.assertEqual(received_packets('cosmoshub-4',r),[])
    def test_invalid_amount_with_error_ack_is_observed_but_never_a_zero_inflow(self):
        r=received(1,'error');a=r['tx_result']['events'][0]['attributes'][-1]
        body=json.loads(bytes.fromhex(a['value']));body['amount']='0';a['value']=json.dumps(body).encode().hex()
        packet=received_packets('noble-1',r)[0]
        self.assertEqual(packet['receipt_status'],ERROR);self.assertEqual(packet['amount_raw'],'')
        self.assertEqual(packet['payload_status'],'invalid_or_unsupported')
    def test_ack_of_another_channel_or_sequence_cannot_confirm_receipt(self):
        r=received(1)
        for a in r['tx_result']['events'][1]['attributes']:
            if a['key']=='packet_sequence':a['value']='999'
        self.assertEqual(received_packets('cosmoshub-4',r)[0]['receipt_status'],UNKNOWN)
    def test_returning_tokens_remove_sender_prefix(self):
        p=received_packets('cosmoshub-4',received(1))[0]
        self.assertEqual(p['packet_denom'],'transfer/channel-1/uatom');self.assertEqual(p['denom'],'uatom')
        self.assertEqual(resolve_asset(p['denom'],mapping())['decimals'],6)
    def test_new_voucher_uses_receiving_channel_not_sender_channel(self):
        p=received_packets('osmosis-1',received(1,denom='uatom'))[0]
        self.assertEqual(p['denom'],'transfer/channel-2/uatom')
        self.assertIsNone(resolve_asset(p['denom'],mapping()))
    def test_remote_contract_port_is_allowed_on_local_transfer_port(self):
        r=received(1,denom='token')
        for event in r['tx_result']['events']:
            for a in event['attributes']:
                if a['key']=='packet_src_port':a['value']='wasm.remote_contract'
        packet=received_packets('osmosis-1',r)[0]
        self.assertEqual(packet['source_port'],'wasm.remote_contract')
        self.assertEqual(packet['denom'],'transfer/channel-2/token')
        self.assertEqual(packet['receipt_status'],SUCCESS)
    def test_base64_attributes_and_injective_18_decimals(self):
        r=received(1,denom='transfer/channel-1/inj')
        for event in r['tx_result']['events']:
            for a in event['attributes']:
                a['key']=base64.b64encode(a['key'].encode()).decode();a['value']=base64.b64encode(a['value'].encode()).decode()
        p=received_packets('injective-1',r)[0]
        self.assertEqual(resolve_asset(p['denom'],mapping())['decimals'],18)
        self.assertEqual(p['receipt_status'],SUCCESS)
    def test_conflicting_acks_and_duplicate_receipts_fail_closed(self):
        r=received(1);r['tx_result']['events'].append(copy.deepcopy(r['tx_result']['events'][0]))
        with self.assertRaises(ValueError):received_packets('cosmoshub-4',r)
        r=received(1);ack=copy.deepcopy(r['tx_result']['events'][1]);ack['attributes'][-1]['value']=json.dumps({'error':'failed'}).encode().hex();r['tx_result']['events'].append(ack)
        with self.assertRaises(ValueError):received_packets('cosmoshub-4',r)
    def test_incoming_filter_rejects_an_outgoing_index_response(self):
        p=incoming_plan(FakeClient())
        with self.assertRaises(ValueError):verify_search_tx(tx(1),p)
        self.assertEqual(p['query'],search_query(11,13,IN_SCOPE))
    def test_incoming_universe_is_distinct_without_reconfiguring_outgoing(self):
        cfg,uid=load_config();inc,in_uid=incoming_config()
        self.assertNotEqual(uid,in_uid);self.assertEqual([c['name'] for c in cfg['chains']],[c['name'] for c in inc['chains']])
    def test_outgoing_error_does_not_skip_incoming_attempt(self):
        import criterion2_pipeline as pipeline
        with patch('sys.argv',['collector','--budget','100']),patch.object(pipeline,'run_collection',side_effect=[RuntimeError('outgoing unavailable'),None]) as collect:
            with self.assertRaises(RuntimeError):pipeline.main()
        self.assertEqual(collect.call_count,2)
        self.assertEqual([call.kwargs['budget'] for call in collect.call_args_list], [50, 50])
        self.assertNotEqual(collect.call_args_list[0].kwargs['root'],collect.call_args_list[1].kwargs['root'])
    def test_four_chain_exports_exclude_errors_and_unknowns_from_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'ledger';out=Path(tmp)/'exports';out.mkdir();prices=Path(tmp)/'prices'
            (out/'ibc_out_chains_daily.csv').write_bytes(b'preserve existing outgoing history\n')
            write_once(price_path(DAY,'cosmos',prices),price())
            for chain in MEMBERS:
                c=FakeClient(chain['name'],transactions=[received(1),received(2,'error'),received(3,None)])
                p=incoming_plan(c);d=root/'incoming-test'/chain['name']/DAY;write_once(d/'plan.json',p)
                collect_pages(d,c,deadline=time.monotonic()+10)
                self.assertTrue(inspect_day(d)['complete'])
            progress,chains,assets,comparisons=exports(root,out,prices)
            row=comparisons[0]
            self.assertEqual((row['observed_packets'],row['received_packets'],row['error_ack_packets'],row['unconfirmed_packets']),(12,4,4,4))
            self.assertEqual(row['valued_in_usd'],'8');self.assertEqual(row['total_in_usd'],'')
            self.assertEqual(row['hub_share_confirmed_packet_count_pct'],'25.00')
            self.assertEqual((out/'ibc_out_chains_daily.csv').read_bytes(),b'preserve existing outgoing history\n')
            self.assertEqual(len(assets),4)
    def test_partial_days_cannot_publish_an_incoming_total(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);c=FakeClient(transactions=[received(1)]);p=incoming_plan(c)
            write_once(root/'ledger'/'incoming-test'/'cosmoshub'/DAY/'plan.json',p)
            progress,chains,assets,comparisons=exports(root/'ledger',root/'exports',root/'prices')
            self.assertEqual(len(progress),1);self.assertEqual(chains,[]);self.assertEqual(comparisons,[])
    def test_incomplete_receiving_index_falls_back_to_all_block_results(self):
        class Client(FakeClient):
            def search(self,query,page,size):
                first=int(re.search(r'tx.height>=(\d+)',query)[1]);end=int(re.search(r'tx.height<=(\d+)',query)[1])
                items=[r for r in self.txs if first<=int(r['height'])<=end]
                return dict(total_count=str(len(items)),txs=[r for r in items if r['height']!='12'])
            def rpc(self,method,params=None):
                if method=='block':return dict(block_id={'hash':'block12'},block=dict(header=dict(chain_id=self.chain['chain_id'],height='12',time=TIMES[12]),data=dict(txs=[received(2)['tx']])))
                if method=='block_results':return dict(height='12',txs_results=[received(2)['tx_result']])
                return super().rpc(method)
        with tempfile.TemporaryDirectory() as tmp:
            c=Client(transactions=[received(1),received(2),received(3)]);p=incoming_plan(c)
            write_once(Path(tmp)/'plan.json',p);collect_pages(tmp,c,deadline=time.monotonic()+10)
            d=inspect_day(tmp);self.assertTrue(d['complete']);self.assertEqual(len(d['packets']),3)
            self.assertTrue(all(r['receipt_status']==SUCCESS for r in d['packets']))


if __name__=='__main__':unittest.main()
