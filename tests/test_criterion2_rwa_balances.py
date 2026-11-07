import base64
import unittest

from criterion2_rwa_balances import amounts, coin_amount
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
