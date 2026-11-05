"""Criterion 1 window ledger. Legacy daily CSVs are read-only archives."""
import csv
import json
import math
import os
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WINDOWS = ROOT / 'hub_criterion1_windows.csv'
CHAIN = 'cosmoshub-4'
YEAR_SECONDS = Decimal(365 * 86400)


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('Timestamp must include timezone')
    return dt.astimezone(timezone.utc)


def number(value):
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError('Invalid number') from exc
    if not result.is_finite():
        raise ValueError('Non-finite number')
    return result


def day_coverage(start, end):
    """Seconds covered in each UTC day, for the interval (start, end]."""
    result = {}
    while start < end:
        midnight = datetime.combine(start.date() + timedelta(days=1), datetime.min.time(), timezone.utc)
        boundary = min(end, midnight)
        result[str(start.date())] = (boundary - start).total_seconds()
        start = boundary
    return result


def read_rows(path=WINDOWS):
    if not Path(path).exists():
        return []
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def enrich(row):
    """Only use snapshots stored with this window; never fetch spot here."""
    r = dict(row)
    r.update(estimated_window_emission_atom='', fee_to_estimated_emission_pct='',
             ibc_fee_to_estimated_emission_pct='', net_supply_change_atom='',
             fee_to_net_supply_increase_pct='', effective_burn_atom='',
             burn_status='not_measured', dilution_after_burn_atom='',
             emission_method='unavailable', fee_valuation_usd_at_collection='')
    if r.get('supply_start_atom', '') != '' and r.get('inflation_start', '') != '':
        estimate = number(r['supply_start_atom']) * number(r['inflation_start']) * number(r['duration_seconds']) / YEAR_SECONDS
        r['estimated_window_emission_atom'] = str(estimate)
        r['emission_method'] = 'start_supply_times_start_annual_rate_times_elapsed_365d_estimate'
        if estimate > 0 and r['fee_collection_status'] == 'complete':
            r['fee_to_estimated_emission_pct'] = str(number(r['collected_fee_atom']) / estimate * 100)
            r['ibc_fee_to_estimated_emission_pct'] = str(number(r['collected_ibc_fee_atom']) / estimate * 100)
    if r.get('supply_start_atom', '') != '' and r.get('supply_end_atom', '') != '':
        delta = number(r['supply_end_atom']) - number(r['supply_start_atom'])
        r['net_supply_change_atom'] = str(delta)
        if delta > 0 and r['fee_collection_status'] == 'complete':
            r['fee_to_net_supply_increase_pct'] = str(number(r['collected_fee_atom']) / delta * 100)
    if r.get('atom_price_usd', '') != '' and r['fee_collection_status'] == 'complete':
        # Explicit collection-time valuation, never a historical window price.
        r['fee_valuation_usd_at_collection'] = str(number(r['collected_fee_atom']) * number(r['atom_price_usd']))
    return r


def validate(rows):
    previous = None
    previous_time = None
    for r in rows:
        if r['chain_id'] != CHAIN:
            raise ValueError('Unexpected chain')
        start, end = int(r['start_height']), int(r['end_height'])
        if start < 2 or end < start or int(r['block_count']) != end - start + 1:
            raise ValueError('Invalid block coverage')
        if previous is not None and start != previous + 1:
            raise ValueError('Overlapping or discontinuous windows')
        previous = end
        if previous_time is not None and timestamp(r['start_boundary_time']) != previous_time:
            raise ValueError('Discontinuous window timestamps')
        previous_time = timestamp(r['end_time'])
        duration = (timestamp(r['end_time']) - timestamp(r['start_boundary_time'])).total_seconds()
        if duration <= 0 or not math.isclose(duration, float(r['duration_seconds']), abs_tol=1e-6):
            raise ValueError('Invalid duration')
        if not timestamp(r['start_boundary_time']) < timestamp(r['first_block_time']) <= timestamp(r['end_time']):
            raise ValueError('Invalid first block time')
        timestamp(r['collected_at'])
        coverage = json.loads(r['utc_day_coverage_seconds'])
        if coverage != day_coverage(timestamp(r['start_boundary_time']), timestamp(r['end_time'])):
            raise ValueError('Invalid UTC day coverage')
        if json.loads(r['utc_day_coverage_pct']) != {k: v / 86400 * 100 for k, v in coverage.items()}:
            raise ValueError('Invalid UTC day percentage')
        if r['coverage_kind'] != 'contiguous_window_not_daily_total':
            raise ValueError('Window must not be presented as daily total')
        for key in ('declared_fee_atom', 'declared_ibc_fee_atom', 'tx_total', 'tx_ibc'):
            if number(r[key]) < 0:
                raise ValueError('Negative fee/count')
        for key in ('tx_total', 'tx_ibc', 'tx_without_fee_event'):
            if int(r[key]) < 0:
                raise ValueError('Invalid transaction count')
        if number(r['tx_ibc']) > number(r['tx_total']):
            raise ValueError('IBC transactions exceed total')
        if number(r['declared_ibc_fee_atom']) > number(r['declared_fee_atom']):
            raise ValueError('IBC fees exceed total')
        if r['fee_collection_status'] == 'complete':
            if int(r['tx_without_fee_event']) != 0:
                raise ValueError('Missing fee events')
            if not 0 <= number(r['collected_ibc_fee_atom']) <= number(r['collected_fee_atom']):
                raise ValueError('Invalid collected fees')
            if r.get('collected_tip_atom', '') != '':
                if not 0 <= number(r['collected_tip_atom']) <= number(r['collected_fee_atom']):
                    raise ValueError('Invalid tips')
                if not 0 <= number(r['collected_ibc_tip_atom']) <= min(number(r['collected_tip_atom']), number(r['collected_ibc_fee_atom'])):
                    raise ValueError('Invalid IBC tips')
        elif r['fee_collection_status'] != 'incomplete' or r['collected_fee_atom'] != '' or r['collected_ibc_fee_atom'] != '':
            raise ValueError('Unknown fees must remain unavailable')
        elif not 0 < int(r['tx_without_fee_event']) <= int(r['tx_total']):
            raise ValueError('Invalid missing fee event count')
        for side, height, time_key in [('start', start - 1, 'start_boundary_time'), ('end', end, 'end_time')]:
            if r.get(f'supply_{side}_atom', '') != '':
                if int(r[f'macro_{side}_height']) != height or r[f'macro_{side}_block_time'] != r[time_key]:
                    raise ValueError('Macro snapshot is not aligned to window')
                timestamp(r[f'macro_{side}_observed_at'])
                if number(r[f'supply_{side}_atom']) <= 0 or number(r[f'inflation_{side}']) < 0:
                    raise ValueError('Invalid macro values')
        if r.get('atom_price_usd', '') != '':
            if number(r['atom_price_usd']) <= 0:
                raise ValueError('Invalid price')
            timestamp(r['price_observed_at'])
            timestamp(r['price_source_updated_at'])
        expected = enrich(r)
        for key in ('estimated_window_emission_atom', 'fee_to_estimated_emission_pct',
                    'ibc_fee_to_estimated_emission_pct', 'net_supply_change_atom',
                    'fee_to_net_supply_increase_pct', 'effective_burn_atom', 'burn_status',
                    'dilution_after_burn_atom', 'emission_method', 'fee_valuation_usd_at_collection'):
            if r.get(key) != expected[key]:
                raise ValueError(f'Invalid derived metric: {key}')


def append_window(row, path=WINDOWS):
    """Atomic commit of a fully collected window. Ledger is also the cursor."""
    rows = read_rows(path)
    validate(rows)
    for existing in rows:
        if existing['start_height'] == str(row['start_height']) and existing['end_height'] == str(row['end_height']):
            if existing == {k: str(v) for k, v in row.items()}:
                return False
            raise ValueError('Conflicting retry of existing window')
    rows.append({k: str(v) for k, v in row.items()})
    validate(rows)
    fields = list(rows[0])
    if any(set(r) != set(fields) for r in rows):
        raise ValueError('Ledger schema mismatch')
    fd, tmp = tempfile.mkstemp(dir=Path(path).parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return True
