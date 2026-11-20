import base64
import unittest

from criterion2_rwa_balances import amounts, coin_amount, validate_closing_proof
from criterion2_rwa_registry import load_registry
from criterion2_transit_evidence import encode_field


def response(height, denom, amount, code=0):
    coin = encode_field(1, denom.encode()) + encode_field(2, str(amount).encode())
    return {'response': {'code': code, 'height': str(height),
                         'value': base64.b64encode(encode_field(1, coin)).decode()}}


class RwaBalanceTests(unittest.TestCase):
    def test_historical_amount_requires_exact_height_denom_and_success(self):
        self.assertEqual(coin_amount(response(12, 'ausdy', 0), 12, 'ausdy'), 0)
        for invalid in (response(13, 'ausdy', 0), response(12, 'uatom', 0),
                        response(12, 'ausdy', 0, code=1), response(12, 'ausdy', '-1')):
            with self.subTest(response=invalid), self.assertRaises(ValueError):
                coin_amount(invalid, 12, 'ausdy')

    def test_stock_subtracts_only_verified_escrow_at_same_height(self):
        registry, _ = load_registry()
        product = next(p for p in registry['products'] if p['id'] == 'ondo_usdy')
        rep = next(r for r in registry['representations']
                   if r['product_id'] == product['id'] and r['kind'] == 'native')
        plan = dict(chain_id=rep['chain_id'], end_height=12,
                    assets={'mapping': {'ausdy': {'decimals': 18}}})
        supply = response(12, 'ausdy', 100)
        self.assertEqual(amounts({'supply': supply, 'escrow': response(12, 'ausdy', 75)},
                                 plan, rep, product, registry), (100, 75, 25))
        with self.assertRaises(ValueError):
            amounts({'supply': supply, 'escrow': response(12, 'ausdy', 101)},
                    plan, rep, product, registry)


class ClosingProofTests(unittest.TestCase):
    def test_provider_change_preserves_valid_closing_observation(self):
        expected = dict(registry_id='registry', date='2026-11-17', chain_id='injective-1',
                        denom='ausdy', height=123, block_time_utc='2026-11-17T23:59:59Z',
                        source='https://new.example', plan_sha256='a' * 64)
        proof = dict(expected, source='https://old.example', plan_sha256='b' * 64)
        validate_closing_proof(proof, expected)
        for key, value in [('height', 124), ('denom', 'other'), ('registry_id', 'other'),
                           ('chain_id', 'other'), ('block_time_utc', 'wrong'), ('date', 'wrong')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_closing_proof(dict(proof, **{key: value}), expected)

    def test_invalid_row_does_not_block_other_proofs_or_export_zero(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        import criterion2_rwa_balances as balances
        expected = dict(registry_id='registry', date='2026-11-17', chain_id='injective-1',
                        denom='ausdy', height=123, block_time_utc='2026-11-17T23:59:59Z',
                        source='https://new.example', plan_sha256='a' * 64)
        plan = dict(end_height=123, last_block={'time': expected['block_time_utc']}, source=expected['source'])
        rep = dict(chain_id='injective-1')
        product = dict(id='usdy', symbol='USDY', decimals=18)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proof = dict(expected, source='https://old.example', plan_sha256='b' * 64,
                         results={}, observed_at='2026-11-18T00:01:00Z')
            first, second = root/'bad.json', root/'good.json'
            balances.write_once(first, dict(proof, height=124))
            balances.write_once(second, proof)
            tasks = [(expected['date'], rep, product, 'ausdy', plan, first),
                     (expected['date'], rep, product, 'ausdy', plan, second)]
            with patch.object(balances, 'ROOT', root), patch.object(balances, 'tasks', return_value=tasks), \
                 patch.object(balances, 'load_registry', return_value=({}, 'registry')), \
                 patch.object(balances, 'identity', return_value=expected), \
                 patch.object(balances, 'amounts', return_value=(100, 25, 75)):
                rows = balances.export()
            self.assertEqual(rows[0]['status'], 'invalid_historical_proof')
            self.assertEqual(rows[0]['local_amount'], '')
            self.assertEqual(rows[1]['status'], 'verified')
            self.assertEqual(rows[1]['source'], 'https://old.example')
            self.assertTrue((root/'ibc_rwa_balances_daily.csv').exists())
