import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from criterion1_emission import annual_rates, block_amounts, bounds, checkpoint, compact, interval_seconds, MINTER, summarize


def event(kind, **attributes):
    return dict(type=kind, attributes=[dict(key=k, value=v) for k, v in attributes.items()])


class EmissionAuditTest(unittest.TestCase):
    def test_block_and_transaction_events_are_counted_once(self):
        result = dict(height='101', finalize_block_events=[
            event('mint', amount='100'),
            event('coinbase', minter=MINTER, amount='100uatom'),
            event('coin_received', amount='100uatom'),
        ], txs_results=[
            dict(code=0, events=[event('coinbase', minter='other', amount='9uatom'),
                                 event('burn', amount='2uatom,4ibc/OTHER')]),
            dict(code=1, events=[event('burn', amount='999uatom')]),
        ])
        self.assertEqual(block_amounts(result, 101), [101, 100, 109, 2])
        with self.assertRaises(ValueError):
            block_amounts(result, 102)
        result['finalize_block_events'].pop(0)
        with self.assertRaises(ValueError):
            block_amounts(result, 101)

    def test_duration_retains_nanoseconds(self):
        elapsed = interval_seconds('2026-11-05T23:59:57.044250006Z', '2026-11-06T23:59:59.687653062Z')
        self.assertEqual(str(elapsed), '86402.643403056')

    def test_pending_and_unreconciled_days_withhold_rates(self):
        state = dict(date='2026-11-06', chain_id='cosmoshub-4', start_height=101, end_height=102,
                     previous_block=dict(height=100, time='2026-11-05T23:59:59Z'),
                     last_block=dict(height=102, time='2026-11-06T23:59:59Z'))
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp) / state['date']
            snapshot = dict(bounds=bounds(state), opening=dict(height=100, uatom=1_000_000),
                            closing=dict(height=102, uatom=1_000_190))
            checkpoint(folder / 'supply.json', snapshot)
            self.assertEqual(summarize(state, temp)['status'], 'pending')
            checkpoint(folder / '101.json', dict(bounds=bounds(state), blocks=[[101, 100, 100, 0], [102, 100, 100, 10]]))
            valid = summarize(state, temp)
            self.assertEqual(valid['status'], 'reconciled')
            self.assertEqual(valid['monetary_emission_atom'], '0.0002')
            self.assertEqual(valid['annualized_monetary_inflation_pct'], '7.3000')
            compact(state, folder)
            self.assertFalse((folder / '101.json').exists())
            self.assertEqual(summarize(state, temp), valid)
            snapshot['closing']['uatom'] += 1
            checkpoint(folder / 'supply.json', snapshot)
            invalid = summarize(state, temp)
            self.assertEqual(invalid['status'], 'unreconciled')
            self.assertEqual(invalid['annualized_monetary_inflation_pct'], '')

    def test_annual_threshold_requires_contiguous_reconciled_days(self):
        first = date(2025, 11, 7)
        rows = [dict(date=str(first + timedelta(days=i)), status='reconciled',
                     start_height=i+2, end_height=i+2, opening_supply_atom='7300',
                     closing_supply_atom='7300', monetary_emission_atom='1',
                     monetary_inflation_365d_pct='', criterion3_status='insufficient_data') for i in range(365)]
        annual_rates(rows[:-1])
        self.assertEqual(rows[-2]['criterion3_status'], 'insufficient_data')
        annual_rates(rows)
        self.assertEqual(rows[-1]['monetary_inflation_365d_pct'], '5')
        self.assertEqual(rows[-1]['criterion3_status'], 'at_or_above_5_pct')
        rows[0]['monetary_emission_atom'] = '0.99'
        annual_rates(rows)
        self.assertEqual(rows[-1]['criterion3_status'], 'below_5_pct')
        rows[0]['status'] = 'unreconciled'
        rows[-1]['criterion3_status'] = 'insufficient_data'
        annual_rates(rows)
        self.assertEqual(rows[-1]['criterion3_status'], 'insufficient_data')
