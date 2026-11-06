import copy
import json
import tempfile
import unittest
from pathlib import Path

from criterion2_pipeline import ROOT,read
from criterion2_lifecycle_evidence import bank_denom
from criterion2_rwa_registry import load_registry,identify


class RwaIdentityTests(unittest.TestCase):
    def setUp(self):
        self.cfg,self.uid=load_registry();self.proofs=ROOT/Path(self.cfg['sources_manifest']).parent
        self.plan=dict(chain_id='noble-1',source='https://noble-rpc.polkachu.com',assets=dict(mapping={'ausdy':{'decimals':18},'uusdc':{'decimals':6},'uusdn':{'decimals':6}}))
        self.packet=dict(chain_id='noble-1',denom='ausdy',height='1',symbol='USDY')
    def identify(self):return identify(self.packet,self.plan,self.cfg,self.proofs)
    def test_native_usdy_exact_issuer_identity(self):
        r=self.identify();self.assertEqual(r['product_id'],'ondo_usdy');self.assertEqual(r['included_in_strict_rwa'],'true')
    def test_same_symbol_is_not_identity(self):
        self.packet['denom']='ufake';self.assertEqual(self.identify()['category'],'unreviewed')
    def test_same_denom_on_wrong_chain_is_not_identity(self):
        self.plan['chain_id']=self.packet['chain_id']='cosmoshub-4';self.assertEqual(self.identify()['category'],'unreviewed')
    def test_usdc_and_usdn_remain_separate(self):
        for denom,category in [('uusdc','fiat_stablecoin'),('uusdn','yield_stablecoin')]:
            self.packet['denom']=denom;r=self.identify();self.assertEqual(r['category'],category);self.assertEqual(r['included_in_strict_rwa'],'false')
    def test_usyc_not_promoted_from_a_denom_hint(self):
        self.packet['denom']='uusyc';self.packet['symbol']='USYC';self.assertEqual(self.identify()['category'],'unreviewed')
        p=next(p for p in self.cfg['products'] if p['id']=='circle_usyc');self.assertEqual(p['identity_status'],'pending_mainnet_identity')
    def test_historical_units_must_match(self):
        self.plan['assets']['mapping']['ausdy']['decimals']=6
        with self.assertRaises(ValueError):self.identify()
    def route(self):
        proof=read(next(self.proofs.glob('injective-1_*_channel-148.json')))
        self.plan['chain_id']=self.packet['chain_id']='injective-1';self.plan['source']=proof['source'];self.packet.update(height=proof['height'],denom='transfer/channel-148/ausdy')
    def test_historical_trace_and_exact_hash_identify_same_product(self):
        self.route();self.assertEqual(self.identify()['identity_status'],'verified_exact_identity')
        self.packet['denom']=bank_denom(self.packet['denom']);self.assertEqual(self.identify()['included_in_strict_rwa'],'true')
    def test_missing_historical_route_stays_pending(self):
        self.route();self.packet['height']='1';r=self.identify();self.assertEqual(r['identity_status'],'pending_historical_route');self.assertEqual(r['included_in_strict_rwa'],'false')
    def test_lookalike_channel_or_extra_hop_not_accepted(self):
        self.route()
        for denom in ['transfer/channel-149/ausdy','transfer/channel-1/transfer/channel-148/ausdy']:
            self.packet['denom']=denom;self.assertEqual(self.identify()['category'],'unreviewed')
    def test_wrong_provider_proof_not_reused(self):
        self.route();self.plan['source']='https://another-provider'
        with self.assertRaises(ValueError):self.identify()
    def test_packet_and_plan_chains_cannot_disagree(self):
        self.plan['chain_id']='osmosis-1'
        with self.assertRaises(ValueError):self.identify()
    def invalid(self,mutate):
        cfg=copy.deepcopy(self.cfg);mutate(cfg)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'registry.json';path.write_text(json.dumps(cfg))
            with self.assertRaises(ValueError):load_registry(path)
    def test_stablecoin_cannot_be_included_by_flag(self):
        self.invalid(lambda c:c['products'][-1].update(include_in_strict_rwa=True))
    def test_ambiguous_identity_rejected(self):
        self.invalid(lambda c:c['representations'].append(dict(c['representations'][0])))
    def test_decimals_must_match_pinned_source(self):
        self.invalid(lambda c:c['products'][0].update(decimals=6))
    def test_wrong_counterparty_in_registry_rejected(self):
        self.invalid(lambda c:c['representations'][-1].update(counterparty_channel='channel-999'))


if __name__=='__main__':unittest.main()
