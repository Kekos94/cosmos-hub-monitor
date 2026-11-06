import base64
import re
import tempfile
import time
import unittest
from pathlib import Path

from criterion2_pipeline import inspect_day,write_once,collect_pages
from test_criterion2 import FakeClient,plan,tx,TIMES


class RangeClient(FakeClient):
    def __init__(self,missing=False):
        super().__init__();self.missing=missing;self.direct_reads=0
    def search(self,query,page,size):
        first=int(re.search(r'tx.height>=(\d+)',query)[1]);end=int(re.search(r'tx.height<=(\d+)',query)[1])
        transactions=[t for t in self.txs if first<=int(t['height'])<=end]
        if self.missing and first<=12<=end:
            # Index advertises the transfer but cannot return its transaction.
            return dict(total_count=str(len(transactions)),txs=[t for t in transactions if t['height']!='12'])
        return dict(total_count=str(len(transactions)),txs=transactions)
    def rpc(self,method,params=None):
        self.direct_reads+=1
        if method=='block':
            return dict(block_id={'hash':'BLOCK12'},block=dict(header=dict(height='12',time=TIMES[12],chain_id=self.chain['chain_id']),data=dict(txs=[tx(2)['tx']])))
        if method=='block_results':return dict(height='12',txs_results=[tx(2)['tx_result']])
        raise ValueError(method)


class AdaptiveRangeTests(unittest.TestCase):
    def test_normal_range_completes_without_scanning_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=RangeClient();p=plan();p['query_mode']='height_ranges';write_once(Path(tmp)/'plan.json',p)
            collect_pages(tmp,c,deadline=time.monotonic()+10)
            d=inspect_day(tmp);self.assertTrue(d['complete']);self.assertEqual(len(d['packets']),3)
            self.assertEqual(c.direct_reads,0)
    def test_incomplete_index_isolated_to_one_block_and_verified(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=RangeClient(missing=True);p=plan();p['query_mode']='height_ranges';write_once(Path(tmp)/'plan.json',p)
            collect_pages(tmp,c,deadline=time.monotonic()+10)
            d=inspect_day(tmp);self.assertTrue(d['complete']);self.assertEqual(len(d['packets']),3)
            self.assertEqual(c.direct_reads,2)
            proof=next(x for x in d['pages'] if x['method']=='block_results_verified')
            self.assertEqual(proof['start_height'],12);self.assertEqual(proof['end_height'],12)
    def test_resume_keeps_completed_ranges_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=RangeClient();p=plan();p['query_mode']='height_ranges';write_once(Path(tmp)/'plan.json',p)
            collect_pages(tmp,c,deadline=time.monotonic()+10)
            before=[f.read_bytes() for f in sorted(Path(tmp).glob('ranges/*'))]
            collect_pages(tmp,c,deadline=time.monotonic()+10)
            self.assertEqual(before,[f.read_bytes() for f in sorted(Path(tmp).glob('ranges/*'))])
    def test_incomplete_block_results_do_not_complete_day(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=RangeClient(missing=True);original=c.rpc
            c.rpc=lambda method,params=None: dict(height='12',txs_results=[]) if method=='block_results' else original(method,params)
            p=plan();p['query_mode']='height_ranges';write_once(Path(tmp)/'plan.json',p)
            with self.assertRaises(ValueError):collect_pages(tmp,c,deadline=time.monotonic()+10)
            self.assertFalse(inspect_day(tmp)['complete'])


if __name__=='__main__':unittest.main()
