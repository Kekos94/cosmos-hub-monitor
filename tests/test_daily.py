import copy
import csv
import io
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import criterion1_daily as daily
from criterion1_data import CHAIN, day_coverage, enrich, timestamp, validate

DAY = date(2026, 11, 4)
NOW = datetime(2026, 11, 5, 12, tzinfo=timezone.utc)
TIMES = {10: '2026-11-03T23:59:59Z', 11: '2026-11-04T00:00:00Z',
         12: '2026-11-04T06:00:00Z', 13: '2026-11-04T12:00:00Z',
         14: '2026-11-04T23:59:59Z', 15: '2026-11-05T00:00:00Z'}


def block(height):
    return {'header': {'chain_id': CHAIN, 'height': str(height), 'time': TIMES[height]}, 'data': {'txs': []}}, 'rpc'


def status():
    return {'result': {'node_info': {'network': CHAIN}, 'sync_info': {
        'earliest_block_height': '10', 'latest_block_height': '15', 'catching_up': False}}}, 'rpc'


def plan():
    with patch.object(daily.monitor, 'get_any', return_value=status()), patch.object(daily.monitor, 'block', side_effect=block):
        return daily.plan_day(DAY, now=NOW)


def chunk(start, end, **kwargs):
    n = end - start + 1
    begin, finish = TIMES[start-1], TIMES[end]
    coverage = day_coverage(timestamp(begin), timestamp(finish))
    r = dict(chain_id=CHAIN, start_height=str(start), end_height=str(end), block_count=str(n),
             start_boundary_time=begin, first_block_time=TIMES[start], end_time=finish,
             duration_seconds=str((timestamp(finish)-timestamp(begin)).total_seconds()),
             utc_day_coverage_seconds=json.dumps(coverage),
             utc_day_coverage_pct=json.dumps({k: v / 86400 * 100 for k, v in coverage.items()}),
             coverage_kind='contiguous_window_not_daily_total', collected_at=NOW.isoformat(),
             tx_total=str(n*2), tx_ibc=str(n), declared_fee_atom=str(n*3), declared_ibc_fee_atom=str(n),
             collected_fee_atom=str(n*3), collected_ibc_fee_atom=str(n), collected_tip_atom=str(n), collected_ibc_tip_atom='0',
             fee_collection_status='complete', tx_without_fee_event='0',
             supply_start_atom=str(500000000+start-1), supply_end_atom=str(500000000+end),
             inflation_start='0.10', inflation_end='0.10',
             macro_start_height=str(start-1), macro_end_height=str(end),
             macro_start_block_time=begin, macro_end_block_time=finish,
             macro_start_observed_at=NOW.isoformat(), macro_end_observed_at=NOW.isoformat())
    return enrich(r)


class PlanningTests(unittest.TestCase):
    def test_includes_midnight_start_excludes_next_midnight(self):
        state = plan()
        self.assertEqual(state['start_height'], 11)
        self.assertEqual(state['end_height'], 14)
        self.assertEqual(state['following_block']['height'], 15)
        self.assertEqual(daily.progress(state)['expected_blocks'], 4)
        self.assertEqual(daily.progress(state)['status'], 'incomplete')

    def test_rejects_current_day_and_pruned_boundary(self):
        with self.assertRaises(ValueError):
            daily.plan_day(date(2026, 11, 5), now=NOW)
        pruned = status()[0]
        pruned['result']['sync_info']['earliest_block_height'] = '11'
        with patch.object(daily.monitor, 'get_any', return_value=(pruned, 'rpc')), patch.object(daily.monitor, 'block', side_effect=block):
            with self.assertRaisesRegex(ValueError, 'Archive'):
                daily.plan_day(DAY, now=NOW)

    def test_status_without_earliest_height_searches_recent_blocks(self):
        s = status()[0]
        del s['result']['sync_info']['earliest_block_height']
        calls = []
        def recent(height):
            calls.append(height)
            if height < 10:
                return {'header': {'chain_id': CHAIN, 'height': str(height), 'time': '2026-11-03T00:00:00Z'}}, 'rpc'
            return block(height)
        with patch.object(daily.monitor, 'get_any', return_value=(s, 'rpc')), patch.object(daily.monitor, 'block', side_effect=recent):
            state = daily.plan_day(DAY, now=NOW)
        self.assertEqual(state['start_height'], 11)
        self.assertNotIn(1, calls)

    def test_requires_finalized_block_after_day_end(self):
        s = status()[0]
        s['result']['sync_info']['latest_block_height'] = '14'
        with patch.object(daily.monitor, 'get_any', return_value=(s, 'rpc')), patch.object(daily.monitor, 'block', side_effect=block):
            with self.assertRaisesRegex(ValueError, 'finalized'):
                daily.plan_day(DAY, now=NOW)

    def test_zero_block_day_is_proven_not_invented(self):
        # A halted chain has no block timestamped inside the day.
        times = dict(TIMES)
        times[11] = TIMES[15]
        def halted(height):
            return {'header': {'chain_id': CHAIN, 'height': str(height), 'time': times[height]}}, 'rpc'
        s = status()[0]
        s['result']['sync_info']['latest_block_height'] = '11'
        with patch.object(daily.monitor, 'get_any', return_value=(s, 'rpc')), patch.object(daily.monitor, 'block', side_effect=halted):
            state = daily.plan_day(DAY, now=NOW)
        row = daily.daily_summary(state)
        self.assertEqual(row['block_count'], 0)
        self.assertEqual(row['tx_total'], '0')
        self.assertEqual(row['estimated_emission_atom'], '')
        self.assertEqual(row['fee_to_estimated_emission_pct'], '')


class DailyAggregationTests(unittest.TestCase):
    def test_parallel_collection_preserves_height_and_timestamp_order(self):
        with patch.object(daily.monitor, 'block', side_effect=block), patch.object(daily.monitor, 'macro', return_value={}), patch.object(daily.monitor, 'price_snapshot', return_value={}):
            row = daily.monitor.collect(11, 14, workers=4)
        validate([row])
        self.assertEqual(row['block_count'], 4)
        self.assertEqual(row['first_block_time'], TIMES[11])
        self.assertEqual(row['end_time'], TIMES[14])
        self.assertEqual(row['tx_total'], 0)

    def test_partial_is_never_exported_as_daily_total(self):
        state = plan()
        state['chunks'] = [chunk(11, 12)]
        self.assertEqual(daily.progress(state)['block_coverage_pct'], '50.0')
        with self.assertRaisesRegex(ValueError, 'Partial'):
            daily.daily_summary(state)
        with tempfile.TemporaryDirectory() as tmp:
            directory, output = Path(tmp)/'days', Path(tmp)/'daily.csv'
            daily.save_day(state, directory)
            daily.export_daily(directory, output)
            self.assertEqual(list(csv.DictReader(io.StringIO(output.read_text()))), [])

    def test_complete_day_totals_all_blocks_once_same_period_ratios(self):
        state = plan()
        state['chunks'] = [chunk(11, 12), chunk(13, 14)]
        row = daily.daily_summary(state)
        self.assertEqual(row['coverage_status'], 'complete_utc_day_blocks')
        self.assertEqual(row['block_count'], 4)
        self.assertEqual(row['block_coverage_pct'], '100')
        self.assertEqual(row['tx_total'], '8')
        self.assertEqual(row['collected_fee_atom'], '12')
        self.assertEqual(row['collected_tip_atom'], '4')
        self.assertEqual(row['net_supply_change_atom'], '4')
        self.assertEqual(row['fee_to_net_supply_increase_pct'], '300')
        self.assertEqual(row['measurement_start_time'], TIMES[10])
        self.assertEqual(row['measurement_end_time'], TIMES[14])
        self.assertEqual(row['effective_burn_atom'], '')
        self.assertEqual(row['daily_fee_usd'], '')
        # Sum estimates from the measured chunks, never multiply sampled fees by 24h.
        from criterion1_data import number
        expected = sum(number(c['estimated_window_emission_atom']) for c in state['chunks'])
        self.assertEqual(number(row['estimated_emission_atom']), expected)
        self.assertEqual(number(row['fee_to_estimated_emission_pct']), 12 / expected * 100)

    def test_missing_fee_events_or_macro_keep_daily_ratios_unavailable(self):
        state = plan()
        first = chunk(11, 12)
        first.update(fee_collection_status='incomplete', tx_without_fee_event='1',
                     collected_fee_atom='', collected_ibc_fee_atom='', collected_tip_atom='', collected_ibc_tip_atom='',
                     supply_start_atom='', inflation_start='')
        state['chunks'] = [enrich(first), chunk(13, 14)]
        row = daily.daily_summary(state)
        self.assertEqual(row['coverage_status'], 'complete_utc_day_blocks')
        self.assertEqual(row['fee_collection_status'], 'incomplete')
        self.assertEqual(row['collected_fee_atom'], '')
        self.assertEqual(row['estimated_emission_atom'], '')
        self.assertEqual(row['fee_to_estimated_emission_pct'], '')
        self.assertEqual(row['net_supply_change_atom'], '')

    def test_rejects_skipped_overlapping_or_outside_day_chunks(self):
        for chunks in [[chunk(12, 14)], [chunk(11, 12), chunk(12, 14)], [chunk(11, 12), chunk(14, 14)]]:
            state = plan()
            state['chunks'] = chunks
            with self.assertRaises(ValueError):
                daily.validate_day(state)
        state = plan()
        state['following_block']['time'] = '2026-11-04T23:59:59Z'
        with self.assertRaises(ValueError):
            daily.validate_day(state)

    def test_saved_snapshots_cannot_be_repriced_or_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            state = plan()
            state['chunks'] = [chunk(11, 12)]
            daily.save_day(state, directory)
            repriced = copy.deepcopy(state)
            repriced['chunks'][0].update(atom_price_usd='9', price_observed_at=NOW.isoformat(), price_source_updated_at=NOW.isoformat())
            repriced['chunks'][0] = enrich(repriced['chunks'][0])
            with self.assertRaisesRegex(ValueError, 'historical'):
                daily.save_day(repriced, directory)
            with self.assertRaisesRegex(ValueError, 'historical'):
                daily.save_day(dict(state, chunks=[]), directory)

    def test_checkpoint_atomicity_and_tamper_detection(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            state = plan()
            state['chunks'] = [chunk(11, 12)]
            daily.save_day(state, directory)
            path = directory / '2026-11-04.json'
            before = path.read_bytes()
            state['chunks'].append(chunk(13, 14))
            with patch.object(daily.os, 'replace', side_effect=OSError('disk full')):
                with self.assertRaises(OSError):
                    daily.save_day(state, directory)
            self.assertEqual(path.read_bytes(), before)
            corrupt = json.loads(before)
            corrupt['chunks'][0]['collected_fee_atom'] = '999'
            path.write_text(json.dumps(corrupt))
            with self.assertRaises(ValueError):
                daily.load_days(directory)


class ResumeTests(unittest.TestCase):
    def test_catches_up_all_missing_closed_days_in_order(self):
        now = datetime(2026, 11, 6, 12, tzinfo=timezone.utc)
        s = status()[0]
        s['result']['sync_info']['latest_block_height'] = '19'
        extra = {16: '2026-11-05T06:00:00Z', 17: '2026-11-05T12:00:00Z',
                 18: '2026-11-05T23:59:59Z', 19: '2026-11-06T00:00:00Z'}
        with tempfile.TemporaryDirectory() as tmp, patch.dict(TIMES, extra):
            directory = Path(tmp)/'days'
            with patch.object(daily.monitor, 'get_any', return_value=(s, 'rpc')), patch.object(daily.monitor, 'block', side_effect=block), patch.object(daily.monitor, 'collect', side_effect=chunk) as collect:
                daily.collect_days(directory=directory, now=now, chunk_blocks=2, start_date=DAY)
                self.assertEqual([c.args for c in collect.call_args_list], [(11, 12), (13, 14), (15, 16), (17, 18)])
            states = daily.load_days(directory)
            self.assertEqual([s['date'] for s in states], ['2026-11-04', '2026-11-05'])
            self.assertTrue(all(daily.progress(s)['status'] == 'complete' for s in states))
            self.assertEqual(states[1]['previous_block'], states[0]['last_block'])
            output = Path(tmp)/'daily.csv'
            daily.export_daily(directory, output)
            with output.open() as f:
                self.assertEqual(len(list(csv.DictReader(f))), 2)

    def test_failure_then_resume_only_missing_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)/'days'
            state = plan()
            def first_call(start, end, **kwargs):
                if start == 13:
                    raise RuntimeError('endpoint failure')
                return chunk(start, end)
            with patch.object(daily, 'plan_day', return_value=state), patch.object(daily.monitor, 'collect', side_effect=first_call):
                with self.assertRaises(RuntimeError):
                    daily.collect_days(directory=directory, now=NOW, chunk_blocks=2)
            saved = daily.load_days(directory)[0]
            self.assertEqual(daily.progress(saved)['scanned_blocks'], 2)
            original = copy.deepcopy(saved['chunks'][0])
            with patch.object(daily.monitor, 'collect', side_effect=chunk) as collect, patch.object(daily, 'plan_day') as planner:
                daily.collect_days(directory=directory, now=NOW, chunk_blocks=2)
                self.assertEqual(collect.call_count, 1)
                self.assertEqual(collect.call_args.args, (13, 14))
                planner.assert_not_called()
            saved = daily.load_days(directory)[0]
            self.assertEqual(saved['chunks'][0], original)
            self.assertEqual(daily.progress(saved)['status'], 'complete')
            output = Path(tmp)/'daily.csv'
            daily.export_daily(directory, output)
            before = output.read_bytes()
            with patch.object(daily.monitor, 'collect') as collect:
                daily.collect_days(directory=directory, now=NOW)
                collect.assert_not_called()
            daily.export_daily(directory, output)
            self.assertEqual(output.read_bytes(), before)

    def test_budget_expiry_keeps_previous_chunks(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)/'days'
            state = plan()
            daily.save_day(state, directory)
            with patch.object(daily.monitor, 'collect', side_effect=TimeoutError):
                daily.collect_days(directory=directory, now=NOW)
            self.assertEqual(daily.progress(daily.load_days(directory)[0])['scanned_blocks'], 0)

    def test_start_date_cannot_silently_replace_existing_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)/'days'
            daily.save_day(plan(), directory)
            with self.assertRaisesRegex(ValueError, 'START_DATE'):
                daily.collect_days(directory=directory, now=NOW, start_date=date(2026, 11, 3))

    def test_no_day_is_silently_skipped_in_checkpoint_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            state = plan()
            state['chunks'] = [chunk(11, 14)]
            daily.save_day(state, directory)
            next_day = copy.deepcopy(state)
            next_day['date'] = '2026-11-06'
            # Filename mismatch alone also fails closed, rather than ignoring corruption.
            (directory/'2026-11-06.json').write_text(json.dumps(next_day))
            with self.assertRaises(ValueError):
                daily.load_days(directory)


if __name__ == '__main__':
    unittest.main()
