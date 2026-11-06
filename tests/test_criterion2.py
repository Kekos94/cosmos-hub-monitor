import base64
import copy
import hashlib
import json
import tempfile
import time
import unittest
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import Mock,patch

from criterion2_core import packets,asset_index,valued_packet,compare_day,digest,verify_search_tx
from criterion2_pipeline import build_plan,write_once,collect_pages,inspect_day,make_page,Client
from criterion2_export import exports,fetch_price,price_path

DAY='2026-11-05'
TIMES={10:'2026-11-04T23:59:59Z',11:'2026-11-05T00:00:00Z',12:'2026-11-05T06:00:00Z',13:'2026-11-05T23:59:59Z',14:'2026-11-06T00:00:00Z'}
MEMBERS=[dict(name=n,chain_id=c) for n,c in [('cosmoshub','cosmoshub-4'),('osmosis','osmosis-1'),('noble','noble-1'),('injective','injective-1')]]


def tx(seq,height=None,denom='uatom',amount='1000000',code=0):
    raw=f'tx-{seq}'.encode();body=json.dumps(dict(denom=denom,amount=amount,sender='sender',receiver='receiver'))
    attrs=dict(packet_src_port='transfer',packet_src_channel='channel-1',packet_dst_port='transfer',
               packet_dst_channel='channel-2',packet_sequence=str(seq),packet_data_hex=body.encode().hex())
    return dict(hash=hashlib.sha256(raw).hexdigest().upper(),height=str(height or 10+seq),index=0,tx=base64.b64encode(raw).decode(),
                tx_result=dict(code=code,events=[dict(type='send_packet',attributes=[dict(key=k,value=v) for k,v in attrs.items()])]))


def mapping():
    return asset_index({'assets':[dict(base='uatom',display='ATOM',symbol='ATOM',coingecko_id='cosmos',denom_units=[dict(denom='uatom',exponent=0),dict(denom='ATOM',exponent=6)]),
                                 dict(base='inj',display='INJ',symbol='INJ',coingecko_id='injective-protocol',denom_units=[dict(denom='inj',exponent=0),dict(denom='INJ',exponent=18)])]})


def price(asset='cosmos',usd='2'):
    return dict(date=DAY,coingecko_id=asset,usd=usd,method='coingecko_daily_history_reference',observed_at='2026-11-06T08:00:00Z',
                reference_time_utc=DAY+'T00:00:00+00:00',source_date_parameter=DAY,source=f'https://api.coingecko.com/api/v3/coins/{asset}/history')


class FakeClient:
    def __init__(self,name='cosmoshub',transactions=None):
        self.chain=next(x for x in MEMBERS if x['name']==name)
        self.source='https://rpc.test';self.txs=transactions if transactions is not None else [tx(1),tx(2),tx(3)]
        self.calls=[];self.fail_page=None
    def rpc(self,method):
        return dict(node_info=dict(network=self.chain['chain_id'],other=dict(tx_index='on')),
                    sync_info=dict(catching_up=False,latest_block_height='14',earliest_block_height='10'))
    def block(self,h):return dict(height=h,time=TIMES[h],chain_id=self.chain['chain_id'])
    def search(self,query,page,size):
        self.calls.append((page,size))
        if self.fail_page==page and size==2:raise RuntimeError('provider failure')
        return dict(total_count=str(len(self.txs)),txs=copy.deepcopy(self.txs[(page-1)*size:page*size]))


def plan(client=None):
    client=client or FakeClient()
    return build_plan(client,DAY,'test-universe',dict(mapping=mapping(),source='registry',observed_at='2026-11-06T00:00:00Z'),
                      page_size=2,now=datetime(2026,11,6,12,tzinfo=timezone.utc),members=MEMBERS)


class PacketTests(unittest.TestCase):
    def test_actual_send_event_not_generic_bank_transfer(self):
        event=tx(1);flows=packets('cosmoshub-4',event)
        self.assertEqual(len(flows),1);self.assertEqual(flows[0]['amount_raw'],'1000000')
        event['tx_result']['events'][0]['type']='transfer'
        self.assertEqual(packets('cosmoshub-4',event),[])
    def test_failed_tx_does_not_create_gross_send(self):
        self.assertEqual(packets('cosmoshub-4',tx(1,code=5)),[])
    def test_base64_packet_event_attributes(self):
        event=tx(1)
        for a in event['tx_result']['events'][0]['attributes']:
            a['key']=base64.b64encode(a['key'].encode()).decode();a['value']=base64.b64encode(a['value'].encode()).decode()
        self.assertEqual(packets('cosmoshub-4',event)[0]['sequence'],'1')
    def test_invalid_amount_or_unsupported_packet_does_not_become_zero(self):
        for amount in ['-1','1.5','NaN','0']:
            with self.subTest(amount=amount),self.assertRaises(ValueError):packets('cosmoshub-4',tx(1,amount=amount))
        event=tx(1);event['tx_result']['events'][0]['attributes'][-1]['value']='00ff'
        with self.assertRaises(ValueError):packets('cosmoshub-4',event)
    def test_injective_18_decimal_units_and_unknown_assets(self):
        p=packets('injective-1',tx(1,denom='inj',amount='1500000000000000000'))[0]
        result=valued_packet(p,mapping(),{'injective-protocol':price('injective-protocol','10')},DAY)
        self.assertEqual(result['amount_display'],'1.5');self.assertEqual(result['out_usd'],'15.0')
        unknown=packets('injective-1',tx(1,denom='unknown'))[0]
        self.assertEqual(valued_packet(unknown,mapping(),{},DAY)['out_usd'],'')
    def test_voucher_trace_matches_hashed_local_denom(self):
        denom='transfer/channel-7/uatom';key='ibc/'+hashlib.sha256(denom.encode()).hexdigest().upper()
        maps={key:dict(base=key,symbol='ATOM',decimals=6,coingecko_id='cosmos')}
        p=packets('osmosis-1',tx(1,denom=denom))[0]
        self.assertEqual(valued_packet(p,maps,{'cosmos':price()},DAY)['out_usd'],'2')
    def test_wrong_day_price_is_rejected_and_no_spot_fallback(self):
        p=packets('cosmoshub-4',tx(1))[0]
        self.assertEqual(valued_packet(p,mapping(),{},DAY)['valuation_status'],'missing_historical_price')
        wrong=price();wrong['date']='2026-11-06'
        with self.assertRaises(ValueError):valued_packet(p,mapping(),{'cosmos':wrong},DAY)
    def test_same_label_cannot_hide_wrong_price_reference_timestamp(self):
        p=packets('cosmoshub-4',tx(1))[0]
        wrong=price();wrong['reference_time_utc']='2026-11-06T00:00:00Z'
        with self.assertRaises(ValueError):valued_packet(p,mapping(),{'cosmos':wrong},DAY)


class CoverageTests(unittest.TestCase):
    def test_exact_midnight_bounds(self):
        p=plan();self.assertEqual((p['start_height'],p['end_height']),(11,13))
        self.assertEqual(p['first_block']['time'],TIMES[11]);self.assertEqual(p['following_block']['time'],TIMES[14])
    def test_missing_index_or_wrong_chain_is_rejected(self):
        for key,value in [('tx_index','off'),('network','wrong')]:
            c=FakeClient();status=c.rpc('status')
            if key=='tx_index':status['node_info']['other'][key]=value
            else:status['node_info'][key]=value
            c.rpc=lambda *args:status
            with self.assertRaises(ValueError):plan(c)
    def test_pagination_failure_then_resume_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            d=Path(tmp);c=FakeClient();p=plan(c);write_once(d/'plan.json',p);c.fail_page=2
            with self.assertRaises(RuntimeError):collect_pages(d,c,deadline=time.monotonic()+10)
            before=(d/'pages/000001.json.gz').read_bytes()
            self.assertFalse(inspect_day(d)['complete']);self.assertEqual(inspect_day(d)['scanned_txs'],2)
            c.fail_page=None;c.calls=[];collect_pages(d,c,deadline=time.monotonic()+10)
            self.assertEqual(c.calls,[(2,2),(1,1)]);self.assertTrue(inspect_day(d)['complete'])
            self.assertEqual((d/'pages/000001.json.gz').read_bytes(),before)
            c.calls=[];collect_pages(d,c,deadline=time.monotonic()+10);self.assertEqual(c.calls,[])
    def test_resume_after_last_page_before_completion_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            d=Path(tmp);c=FakeClient();p=plan(c);write_once(d/'plan.json',p)
            for n in [1,2]:write_once(d/'pages'/f'{n:06d}.json.gz',make_page(p,c.search(p['query'],n,2),n),compressed=True)
            c.calls=[];collect_pages(d,c,deadline=time.monotonic()+10)
            self.assertEqual(c.calls,[(1,1)]);self.assertTrue(inspect_day(d)['complete'])
    def test_truncated_page_rejected(self):
        p=plan()
        with self.assertRaises(ValueError):make_page(p,dict(total_count='3',txs=[tx(1)]),1)
    def test_query_filter_and_hash_must_match(self):
        p=plan()
        for change in ['height','hash','event']:
            event=tx(1)
            if change=='height':event['height']='99'
            if change=='hash':event['hash']='0'*64
            if change=='event':event['tx_result']['events']=[]
            with self.subTest(change=change),self.assertRaises(ValueError):verify_search_tx(event,p)
    def test_duplicate_transactions_or_packets_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            d=Path(tmp);c=FakeClient(transactions=[tx(1),tx(1)]);p=plan(c);write_once(d/'plan.json',p)
            with self.assertRaises(ValueError):collect_pages(d,c,deadline=time.monotonic()+10)
    def test_index_total_change_does_not_publish_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            d=Path(tmp);c=FakeClient();p=plan(c);write_once(d/'plan.json',p)
            write_once(d/'pages/000001.json.gz',make_page(p,c.search(p['query'],1,2),1),compressed=True)
            c.txs.append(tx(4,height=13));c.txs[-1]['index']=1
            with self.assertRaises(ValueError):collect_pages(d,c,deadline=time.monotonic()+10)
            self.assertFalse((d/'complete.json').exists())
    def test_genuine_indexed_zero_is_separate_from_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            d=Path(tmp);c=FakeClient(transactions=[]);write_once(d/'plan.json',plan(c))
            collect_pages(d,c,deadline=time.monotonic()+10)
            result=inspect_day(d);self.assertTrue(result['complete']);self.assertEqual(result['packets'],[])
    def test_rpc_uri_quotes_query_but_not_numeric_page(self):
        chain=dict(name='cosmoshub',chain_id='cosmoshub-4');c=Client(chain,'https://rpc.test')
        with patch('criterion2_pipeline.http.get_any',return_value=({'result':{'total_count':'0','txs':[]}},'rpc')) as get:
            c.search("send_packet.packet_src_port='transfer'",2,50)
        self.assertEqual(get.call_args.kwargs['params']['query'],json.dumps("send_packet.packet_src_port='transfer'"))
        self.assertEqual(get.call_args.kwargs['params']['page'],'2')


class ComparisonTests(unittest.TestCase):
    def test_no_comparison_until_all_chains_complete_same_day(self):
        r=dict(date=DAY,coverage_status='indexed_complete',valued_out_usd='10',unpriced_packets=0,sent_packets=1)
        with self.assertRaises(ValueError):compare_day(DAY,['cosmoshub','osmosis'],{'cosmoshub':r})
        rows={'cosmoshub':r,'osmosis':dict(r,date='2026-11-04')}
        with self.assertRaises(ValueError):compare_day(DAY,['cosmoshub','osmosis'],rows)
    def test_unpriced_subset_share_is_not_total_market_share(self):
        r=dict(date=DAY,coverage_status='indexed_complete',valued_out_usd='10',unpriced_packets=0,sent_packets=1)
        result=compare_day(DAY,['cosmoshub','osmosis'],{'cosmoshub':r,'osmosis':dict(r,unpriced_packets=1,sent_packets=2)})
        self.assertEqual(result['hub_share_valued_usd_pct'],'50.0');self.assertEqual(result['hub_share_total_usd_pct'],'')
        self.assertEqual(result['total_out_usd'],'')
    def test_complete_four_chain_export_and_immutable_prices(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'data';out=Path(tmp)/'out';prices=Path(tmp)/'prices'
            for member in MEMBERS:
                c=FakeClient(member['name']);d=root/'test-universe'/member['name']/DAY
                write_once(d/'plan.json',plan(c));collect_pages(d,c,deadline=time.monotonic()+10)
            write_once(price_path(DAY,'cosmos',prices),price())
            with patch('criterion2_export.requests.get') as get:
                self.assertEqual(fetch_price(DAY,'cosmos',prices)['usd'],'2');get.assert_not_called()
            progress,chains,assets,comparisons=exports(root,out,prices)
            self.assertEqual(len(progress),4);self.assertEqual(len(comparisons),1)
            self.assertEqual(comparisons[0]['hub_share_total_usd_pct'],'25.00')
            self.assertEqual(comparisons[0]['valued_out_usd'],'24')
            self.assertEqual(len(assets),4)
    def test_partial_days_never_create_daily_volumes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'data';d=root/'test-universe'/'cosmoshub'/DAY
            p=plan();write_once(d/'plan.json',p)
            write_once(d/'pages/000001.json.gz',make_page(p,FakeClient().search(p['query'],1,2),1),compressed=True)
            progress,chains,assets,comparisons=exports(root,Path(tmp)/'out',Path(tmp)/'prices')
            self.assertEqual(len(progress),1);self.assertEqual(chains,[]);self.assertEqual(comparisons,[])


if __name__=='__main__':unittest.main()
