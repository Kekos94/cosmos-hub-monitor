import hashlib
import json
import subprocess
import tempfile
import unittest
from decimal import Decimal
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch, Mock

import criterion1_data as data
import hub_fee_monitor_v41 as monitor
import generate_dashboard_html as dashboard


def fixture(start=11, end=12):
    r = dict(chain_id=data.CHAIN, start_height=str(start), end_height=str(end), block_count=str(end-start+1),
             start_boundary_time='2026-11-04T23:59:30Z', first_block_time='2026-11-04T23:59:45Z',
             end_time='2026-11-05T00:00:30Z', duration_seconds='60.0',
             utc_day_coverage_seconds=json.dumps({'2026-11-04': 30.0, '2026-11-05': 30.0}),
             utc_day_coverage_pct=json.dumps({'2026-11-04': 30 / 86400 * 100, '2026-11-05': 30 / 86400 * 100}),
             coverage_kind='contiguous_window_not_daily_total', collected_at='2026-11-05T00:01:00Z',
             tx_total='2', tx_ibc='1', declared_fee_atom='3', declared_ibc_fee_atom='1',
             collected_fee_atom='3', collected_ibc_fee_atom='1', tx_without_fee_event='0', fee_collection_status='complete',
             supply_start_atom='525600000', supply_end_atom='525600100', inflation_start='0.10', inflation_end='0.11',
             macro_start_height=str(start-1), macro_end_height=str(end),
             macro_start_block_time='2026-11-04T23:59:30Z', macro_end_block_time='2026-11-05T00:00:30Z',
             macro_start_observed_at='2026-11-05T00:01:00Z', macro_end_observed_at='2026-11-05T00:01:00Z',
             atom_price_usd='2', price_observed_at='2026-11-05T00:01:00Z', price_source_updated_at='2026-11-05T00:00:50Z')
    shift = timedelta(seconds=(start - 11) * 30)
    for key in ('start_boundary_time', 'first_block_time', 'end_time', 'macro_start_block_time', 'macro_end_block_time'):
        r[key] = (data.timestamp(r[key]) + shift).isoformat().replace('+00:00', 'Z')
    coverage = data.day_coverage(data.timestamp(r['start_boundary_time']), data.timestamp(r['end_time']))
    r['utc_day_coverage_seconds'] = json.dumps(coverage)
    r['utc_day_coverage_pct'] = json.dumps({k: v / 86400 * 100 for k, v in coverage.items()})
    return data.enrich(r)


class EconomicTests(unittest.TestCase):
    def test_same_duration_and_no_fee_burn_assumption(self):
        r = fixture()
        data.validate([r])
        self.assertEqual(Decimal(r['estimated_window_emission_atom']), Decimal('100'))
        self.assertEqual(Decimal(r['fee_to_estimated_emission_pct']), Decimal('3'))
        self.assertEqual(Decimal(r['ibc_fee_to_estimated_emission_pct']), Decimal('1'))
        self.assertEqual(Decimal(r['net_supply_change_atom']), Decimal('100'))
        self.assertEqual(Decimal(r['fee_to_net_supply_increase_pct']), Decimal('3'))
        self.assertEqual(r['effective_burn_atom'], '')
        self.assertEqual(r['dilution_after_burn_atom'], '')
        self.assertEqual(r['fee_valuation_usd_at_collection'], '6')

    def test_missing_macro_or_price_is_unknown(self):
        r = fixture()
        r.update(supply_start_atom='', supply_end_atom='', inflation_start='', atom_price_usd='')
        r = data.enrich(r)
        data.validate([r])
        for key in ['estimated_window_emission_atom', 'fee_to_estimated_emission_pct', 'net_supply_change_atom',
                    'fee_to_net_supply_increase_pct', 'fee_valuation_usd_at_collection']:
            self.assertEqual(r[key], '')

    def test_unknown_collected_fees_do_not_use_declared_fees(self):
        r = fixture()
        r.update(fee_collection_status='incomplete', collected_fee_atom='', collected_ibc_fee_atom='', tx_without_fee_event='1')
        r = data.enrich(r)
        data.validate([r])
        self.assertEqual(r['fee_to_estimated_emission_pct'], '')
        self.assertEqual(r['fee_to_net_supply_increase_pct'], '')

    def test_zero_inflation_and_negative_supply_change(self):
        r = fixture()
        r.update(inflation_start='0', supply_end_atom='525599999')
        r = data.enrich(r)
        data.validate([r])
        self.assertEqual(r['fee_to_estimated_emission_pct'], '')
        self.assertEqual(r['fee_to_net_supply_increase_pct'], '')
        self.assertEqual(r['net_supply_change_atom'], '-1')
        self.assertEqual(r['effective_burn_atom'], '')

    def test_midnight_and_missing_days_use_elapsed_time(self):
        r = fixture()
        self.assertEqual(sum(json.loads(r['utc_day_coverage_seconds']).values()), 60)
        start = data.timestamp('2026-11-01T12:00:00Z')
        end = data.timestamp('2026-11-04T12:00:00Z')
        coverage = data.day_coverage(start, end)
        self.assertEqual(sum(coverage.values()), 3 * 86400)
        self.assertEqual(coverage['2026-11-02'], 86400)

    def test_validation_rejects_bad_coverage_and_macro_alignment(self):
        for key, value in [('block_count', '3'), ('duration_seconds', '86400'), ('macro_start_height', '11'),
                           ('fee_to_estimated_emission_pct', '999'), ('effective_burn_atom', '3'),
                           ('atom_price_usd', 'NaN'), ('tx_ibc', '3')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                r = fixture()
                r[key] = value
                data.validate([r])


class PersistenceTests(unittest.TestCase):
    def test_retry_idempotency_overlap_and_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'windows.csv'
            row = fixture()
            self.assertTrue(data.append_window(row, path))
            before = path.read_bytes()
            self.assertFalse(data.append_window(row, path))
            self.assertEqual(path.read_bytes(), before)
            conflict = dict(row, atom_price_usd='3')
            with self.assertRaises(ValueError):
                data.append_window(data.enrich(conflict), path)
            with self.assertRaises(ValueError):
                data.append_window(fixture(12, 13), path)
            self.assertEqual(path.read_bytes(), before)

    def test_no_retroactive_price_or_inflation_updates(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'windows.csv'
            first = fixture()
            data.append_window(first, path)
            second = fixture(13, 14)
            second.update(atom_price_usd='9', inflation_start='0.20')
            data.append_window(data.enrich(second), path)
            rows = data.read_rows(path)
            self.assertEqual(rows[0], first)
            self.assertEqual(rows[1]['atom_price_usd'], '9')

    def test_atomic_replace_failure_keeps_previous_cursor_and_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'windows.csv'
            data.append_window(fixture(), path)
            before = path.read_bytes()
            with patch.object(data.os, 'replace', side_effect=OSError('disk failure')):
                with self.assertRaises(OSError):
                    data.append_window(fixture(13, 14), path)
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(list(Path(tmp).glob('*.tmp')), [])

    def test_legacy_csvs_are_exact_original_bytes(self):
        manifest = json.loads((data.ROOT / 'tests/legacy_sha256.json').read_text())
        for name, expected in manifest.items():
            self.assertEqual(hashlib.sha256((data.ROOT / name).read_bytes()).hexdigest(), expected, name)
        self.assertEqual(len(data.read_rows(data.ROOT / 'hub_revenue_daily.csv')), 253)

    def test_dashboard_refreshes_copies_without_moving_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'site').mkdir()
            (root / 'hub_revenue_daily.csv').write_text('date,tx_total\n2026-11-05,7\n')
            (root / 'site/hub_revenue_daily.csv').write_text('stale')
            with patch.object(dashboard, 'ROOT', root), patch.object(dashboard, 'WINDOWS', root / 'hub_criterion1_windows.csv'), patch('build_web_dashboard.main') as build_public:
                dashboard.main()
            build_public.assert_called_once_with(root)
            self.assertEqual((root / 'hub_revenue_daily.csv').read_bytes(), (root / 'site/hub_revenue_daily.csv').read_bytes())
            html = (root / 'site/criterion1.html').read_text()
            self.assertIn('Aucune fenêtre mesurée', html)
            self.assertIn('non validés', html)


class CollectionTests(unittest.TestCase):
    def test_fee_market_collects_base_and_tip_once(self):
        events = [{'type': 'fee_pay', 'attributes': [{'key': 'fee', 'value': '1422uatom'}]},
                  {'type': 'tip_pay', 'attributes': [{'key': 'tip', 'value': '468uatom'}]}]
        tx = {'tx_response': {'code': 0, 'events': events}}
        self.assertEqual(monitor.collected_fee_components(tx), (1890, 468))
        events.append({'type': 'tx', 'attributes': [{'key': 'fee', 'value': '1890uatom'}]})
        self.assertEqual(monitor.collected_fee(tx), 1890)
        events[-1]['attributes'][0]['value'] = '100uatom'
        self.assertIsNone(monitor.collected_fee(tx))

    def test_complete_collection_measures_exact_blocks_and_ignores_other_denoms(self):
        def b(time, txs):
            return {'header': {'time': time}, 'data': {'txs': txs}}, 'rpc'
        tx_hash = hashlib.sha256(b'tx').hexdigest().upper()
        tx = {'tx_response': {'height': '11', 'txhash': tx_hash, 'code': 5,
                             'events': [{'type': 'fee_pay', 'attributes': [{'key': 'fee', 'value': '1400uatom,99other'}]},
                                        {'type': 'tip_pay', 'attributes': [{'key': 'tip', 'value': '400uatom'}]}]},
              'tx': {'body': {'messages': [{'@type': '/ibc.core.client.v1.MsgUpdateClient'}]},
                     'auth_info': {'fee': {'amount': [{'denom': 'uatom', 'amount': '1900'}]}}}}
        with patch.object(monitor, 'block', side_effect=[b('2026-11-04T23:59:30Z', []), b('2026-11-04T23:59:45Z', ['dHg=']), b('2026-11-05T00:00:30Z', [])]), patch.object(monitor, 'get_any', return_value=(tx, 'lcd')), patch.object(monitor, 'macro', return_value={}), patch.object(monitor, 'price_snapshot', return_value={}):
            row = monitor.collect(11, 12)
        data.validate([row])
        self.assertEqual(row['block_count'], 2)
        self.assertEqual(row['duration_seconds'], 60)
        self.assertEqual(row['tx_total'], 1)
        self.assertEqual(row['collected_fee_atom'], '0.0018')
        self.assertEqual(row['collected_tip_atom'], '0.0004')
        self.assertEqual(row['declared_fee_atom'], '0.0019')
        self.assertEqual(row['collected_ibc_fee_atom'], row['collected_fee_atom'])
        self.assertEqual(row['estimated_window_emission_atom'], '')

    def test_stale_price_remains_unavailable(self):
        response = Mock()
        response.json.return_value = {'cosmos': {'usd': 2, 'last_updated_at': 1}}
        with patch.object(monitor.requests, 'get', return_value=response):
            row = monitor.price_snapshot()
        self.assertEqual(row['atom_price_usd'], '')
        self.assertIn('stale', row['price_error'])

    def test_wrong_chain_or_height_is_rejected(self):
        for chain, height in [('other', '11'), (data.CHAIN, '12')]:
            response = {'result': {'block': {'header': {'chain_id': chain, 'height': height}}}}
            with patch.object(monitor, 'get_any', return_value=(response, 'rpc')):
                with self.assertRaises(ValueError):
                    monitor.block(11)

    def test_fee_events_include_failed_message_fees_but_not_declared_only(self):
        tx = {'tx_response': {'code': 5, 'events': [{'type': 'tx', 'attributes': [{'key': 'fee', 'value': '2uatom,10other'}]}]}}
        self.assertEqual(monitor.collected_fee(tx), 2)
        tx['tx_response']['events'] = []
        self.assertIsNone(monitor.collected_fee(tx))
        tx['tx_response']['events'] = [{'type': 'tx', 'attributes': [{'key': 'fee', 'value': ''}]}]
        self.assertEqual(monitor.collected_fee(tx), 0)

    def test_historical_response_must_confirm_requested_height(self):
        response = Mock(headers={'x-cosmos-block-height': '999'})
        response.json.return_value = {'inflation': '0.10'}
        with patch.object(monitor.requests, 'get', return_value=response):
            with self.assertRaises(RuntimeError):
                monitor.get_any(['https://example.test'], '/inflation', height=10)
        response.headers = {'grpc-metadata-x-cosmos-block-height': '10'}
        with patch.object(monitor.requests, 'get', return_value=response):
            self.assertEqual(monitor.get_any(['https://example.test'], '/inflation', height=10)[0]['inflation'], '0.10')

    def test_unconfirmed_macro_does_not_become_zero_or_current_spot(self):
        with patch.object(monitor, 'get_any', side_effect=RuntimeError('pruned')):
            result = monitor.macro(10, '2026-11-05T00:00:00Z', 'start')
        self.assertEqual(result['supply_start_atom'], '')
        self.assertEqual(result['inflation_start'], '')
        self.assertIn('pruned', result['macro_start_error'])

    def test_partial_block_or_tx_failure_never_advances_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'windows.csv'
            status = {'result': {'node_info': {'network': data.CHAIN}, 'sync_info': {'latest_block_height': '100', 'catching_up': False}}}
            boundary = {'header': {'time': '2026-11-05T00:00:00Z'}, 'data': {'txs': []}}
            incomplete = {'header': {'time': '2026-11-05T00:00:10Z'}, 'data': {'txs': ['dHg=']}}
            with patch.object(monitor, 'WINDOWS', path), patch.object(monitor, 'get_any', side_effect=[(status, 'rpc'), RuntimeError('tx unavailable')]), patch.object(monitor, 'block', side_effect=[(boundary, 'rpc'), (incomplete, 'rpc')]):
                with self.assertRaises(RuntimeError):
                    monitor.main()
            self.assertFalse(path.exists())

    def test_exact_batch_size_and_no_new_blocks_no_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'windows.csv'
            status = {'result': {'node_info': {'network': data.CHAIN}, 'sync_info': {'latest_block_height': '100', 'catching_up': False}}}
            with patch.object(monitor, 'WINDOWS', path), patch.object(monitor, 'BLOCK_BATCH', 60), patch.object(monitor, 'get_any', return_value=(status, 'rpc')), patch.object(monitor, 'collect', side_effect=RuntimeError('stop')) as collect:
                with self.assertRaises(RuntimeError):
                    monitor.main()
                collect.assert_called_once_with(41, 100)
            data.append_window(fixture(), path)
            status['result']['sync_info']['latest_block_height'] = '12'
            before = path.read_bytes()
            with patch.object(monitor, 'WINDOWS', path), patch.object(monitor, 'get_any', return_value=(status, 'rpc')), patch.object(monitor, 'collect') as collect:
                monitor.main()
                collect.assert_not_called()
            self.assertEqual(path.read_bytes(), before)

    def test_pipeline_propagates_failure(self):
        import run_criterion1_economy_pipeline as pipeline
        with patch.object(pipeline.subprocess, 'run', side_effect=subprocess.CalledProcessError(1, 'collector')):
            with self.assertRaises(subprocess.CalledProcessError):
                pipeline.main()


if __name__ == '__main__':
    unittest.main()
