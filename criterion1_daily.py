"""Full UTC-day collection with durable, validated chunk checkpoints.

Membership: block timestamps in [00:00 UTC, next 00:00 UTC).
Supply/estimated emission denominators use exactly those blocks (state at
first_height-1 through end_height), not an assumed 86400-second block span.
"""
import csv
import io
import json
import os
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import hub_fee_monitor_v41 as monitor
from criterion1_data import ROOT, CHAIN, number, timestamp, utcnow, validate

DAY_DIR = ROOT / 'data' / 'criterion1_days'
DAILY_CSV = ROOT / 'hub_criterion1_daily_complete.csv'


def midnight(day):
    return datetime.combine(day, datetime.min.time(), timezone.utc)


def atomic_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def header(block):
    h = block['header']
    return dict(height=int(h['height']), time=h['time'], chain_id=h['chain_id'])


def plan_day(day, *, now=None):
    now = now or datetime.now(timezone.utc)
    begin, finish = midnight(day), midnight(day + timedelta(days=1))
    if finish > now:
        raise ValueError('Cannot collect an unfinished UTC day')
    status, _ = monitor.get_any(monitor.RPCS, '/status')
    info = status['result']
    if info['node_info']['network'] != CHAIN or info['sync_info']['catching_up']:
        raise ValueError('Endpoint is on wrong chain or catching up')
    sync = info['sync_info']
    latest = int(sync['latest_block_height'])
    earliest = max(1, int(sync.get('earliest_block_height', 1)))
    cache = {}
    def get(height):
        if height not in cache:
            b, _ = monitor.block(height)
            cache[height] = header(b)
        return cache[height]
    if timestamp(get(latest)['time']) < finish:
        raise ValueError('A finalized block at/after next midnight is required')
    # Older Tendermint status responses omit earliest_block_height. Do not query
    # genesis on a pruned public node: bracket the recent day from the head.
    distance = 1
    lower = latest
    while timestamp(get(lower)['time']) >= begin:
        if lower == earliest:
            raise ValueError('Archive lacks a block preceding the start of the day')
        lower = max(earliest, latest-distance)
        distance *= 2
    def lower_bound(target):
        lo, hi = lower, latest
        while lo < hi:
            mid = (lo + hi) // 2
            if timestamp(get(mid)['time']) < target:
                lo = mid + 1
            else:
                hi = mid
        return lo
    first, following = lower_bound(begin), lower_bound(finish)
    state = dict(schema_version=1, date=str(day), chain_id=CHAIN,
                 day_start_utc=begin.isoformat(), day_end_utc=finish.isoformat(),
                 start_height=first, end_height=following-1,
                 previous_block=get(first-1), first_block=get(first),
                 last_block=get(following-1), following_block=get(following),
                 planned_at=utcnow(), chunks=[])
    validate_day(state)
    return state


def validate_day(state):
    day = date.fromisoformat(state['date'])
    if state['schema_version'] != 1 or state['chain_id'] != CHAIN:
        raise ValueError('Unexpected daily schema or chain')
    begin, finish = midnight(day), midnight(day + timedelta(days=1))
    if timestamp(state['day_start_utc']) != begin or timestamp(state['day_end_utc']) != finish:
        raise ValueError('Invalid UTC day bounds')
    first, end = int(state['start_height']), int(state['end_height'])
    if first < 2 or end < first-1:
        raise ValueError('Invalid planned heights')
    for key, height in [('previous_block', first-1), ('first_block', first),
                        ('last_block', end), ('following_block', end+1)]:
        h = state[key]
        if h['chain_id'] != CHAIN or int(h['height']) != height:
            raise ValueError('Invalid day boundary block')
        timestamp(h['time'])
    if not timestamp(state['previous_block']['time']) < begin <= timestamp(state['first_block']['time']):
        raise ValueError('First block does not prove midnight lower bound')
    if not timestamp(state['last_block']['time']) < finish <= timestamp(state['following_block']['time']):
        raise ValueError('Last block does not prove midnight upper bound')
    if first <= end and not begin <= timestamp(state['first_block']['time']) <= timestamp(state['last_block']['time']) < finish:
        raise ValueError('Planned blocks are outside the UTC day')
    if first > end and state['previous_block'] != state['last_block']:
        raise ValueError('Invalid zero-block day')
    if first > end and state['first_block'] != state['following_block']:
        raise ValueError('Invalid zero-block day successor')
    chunks = state['chunks']
    validate(chunks)
    for index, chunk in enumerate(chunks):
        if int(chunk['end_height']) > end:
            raise ValueError('Chunk exceeds planned day')
        if not begin <= timestamp(chunk['first_block_time']) <= timestamp(chunk['end_time']) < finish:
            raise ValueError('Chunk contains blocks outside the UTC day')
        if index == 0:
            if int(chunk['start_height']) != first or timestamp(chunk['start_boundary_time']) != timestamp(state['previous_block']['time']):
                raise ValueError('Daily checkpoint skips initial blocks')
            if timestamp(chunk['first_block_time']) != timestamp(state['first_block']['time']):
                raise ValueError('Wrong first block timestamp')
    if chunks and int(chunks[-1]['end_height']) == end:
        if timestamp(chunks[-1]['end_time']) != timestamp(state['last_block']['time']):
            raise ValueError('Wrong final block timestamp')


def progress(state):
    validate_day(state)
    expected = state['end_height'] - state['start_height'] + 1
    scanned = sum(int(c['block_count']) for c in state['chunks'])
    return dict(date=state['date'], status='complete' if scanned == expected else 'incomplete',
                expected_blocks=expected, scanned_blocks=scanned,
                block_coverage_pct=str(Decimal(scanned) / expected * 100) if expected else '100',
                next_height=state['start_height']+scanned)


def save_day(state, directory=DAY_DIR):
    validate_day(state)
    path = Path(directory) / (state['date'] + '.json')
    if path.exists():
        previous = json.loads(path.read_text(encoding='utf-8'))
        validate_day(previous)
        if {k: v for k, v in previous.items() if k != 'chunks'} != {k: v for k, v in state.items() if k != 'chunks'}:
            raise ValueError('Cannot replace the recorded day plan')
        if state['chunks'][:len(previous['chunks'])] != previous['chunks']:
            raise ValueError('Cannot rewrite or remove historical chunk snapshots')
        if state == previous:
            return
    atomic_text(path, json.dumps(state, indent=2, sort_keys=True) + '\n')


def load_days(directory=DAY_DIR):
    states = []
    for path in sorted(Path(directory).glob('*.json')):
        state = json.loads(path.read_text(encoding='utf-8'))
        validate_day(state)
        if path.stem != state['date']:
            raise ValueError('Checkpoint filename/date mismatch')
        if states and date.fromisoformat(state['date']) != date.fromisoformat(states[-1]['date']) + timedelta(days=1):
            raise ValueError('Missing daily checkpoint between recorded dates')
        if states and progress(states[-1])['status'] != 'complete':
            raise ValueError('Cannot skip an unfinished day')
        if states and (state['start_height'] != states[-1]['end_height'] + 1 or
                       state['previous_block'] != states[-1]['last_block']):
            raise ValueError('Non-contiguous block coverage between days')
        states.append(state)
    return states


def daily_summary(state):
    p = progress(state)
    if p['status'] != 'complete':
        raise ValueError('Partial collection cannot be exported as a daily total')
    chunks = state['chunks']
    def total(key):
        if any(c.get(key, '') == '' for c in chunks):
            return ''
        return str(sum((number(c[key]) for c in chunks), Decimal(0)))
    r = dict(date=state['date'], chain_id=CHAIN, coverage_status='complete_utc_day_blocks',
             day_start_utc=state['day_start_utc'], day_end_utc=state['day_end_utc'], utc_day_seconds=86400,
             start_height=state['start_height'], end_height=state['end_height'],
             block_count=p['scanned_blocks'], block_coverage_pct='100',
             measurement_start_time=state['previous_block']['time'], measurement_end_time=state['last_block']['time'],
             measured_block_interval_seconds=(timestamp(state['last_block']['time'])-timestamp(state['previous_block']['time'])).total_seconds(),
             first_block_time=state['first_block']['time'] if chunks else '',
             tx_total=total('tx_total'), tx_ibc=total('tx_ibc'),
             declared_fee_atom=total('declared_fee_atom'), declared_ibc_fee_atom=total('declared_ibc_fee_atom'),
             tx_without_fee_event=total('tx_without_fee_event'),
             fee_collection_status='complete' if all(c['fee_collection_status']=='complete' for c in chunks) else 'incomplete',
             collected_fee_atom=total('collected_fee_atom'), collected_ibc_fee_atom=total('collected_ibc_fee_atom'),
             collected_tip_atom=total('collected_tip_atom'), collected_ibc_tip_atom=total('collected_ibc_tip_atom'),
             estimated_emission_atom=total('estimated_window_emission_atom') if chunks else '',
             emission_method='sum_of_chunk_rate_duration_estimates' if chunks else 'unavailable',
             net_supply_change_atom='', fee_to_estimated_emission_pct='', ibc_fee_to_estimated_emission_pct='',
             fee_to_net_supply_increase_pct='', effective_burn_atom='', burn_status='not_measured',
             dilution_after_burn_atom='', historical_daily_price_usd='', daily_fee_usd='',
             price_status='no_historical_daily_price', completed_at=chunks[-1]['collected_at'] if chunks else state['planned_at'])
    if r['fee_collection_status'] != 'complete':
        for key in ('collected_fee_atom', 'collected_ibc_fee_atom', 'collected_tip_atom', 'collected_ibc_tip_atom'):
            r[key] = ''
    if r['estimated_emission_atom'] == '':
        r['emission_method'] = 'unavailable'
    if chunks and chunks[0].get('supply_start_atom', '') != '' and chunks[-1].get('supply_end_atom', '') != '':
        r['net_supply_change_atom'] = str(number(chunks[-1]['supply_end_atom']) - number(chunks[0]['supply_start_atom']))
    if r['fee_collection_status'] == 'complete':
        if r['estimated_emission_atom'] != '' and number(r['estimated_emission_atom']) > 0:
            r['fee_to_estimated_emission_pct'] = str(number(r['collected_fee_atom']) / number(r['estimated_emission_atom']) * 100)
            r['ibc_fee_to_estimated_emission_pct'] = str(number(r['collected_ibc_fee_atom']) / number(r['estimated_emission_atom']) * 100)
        if r['net_supply_change_atom'] != '' and number(r['net_supply_change_atom']) > 0:
            r['fee_to_net_supply_increase_pct'] = str(number(r['collected_fee_atom']) / number(r['net_supply_change_atom']) * 100)
    return r


# Stable empty-export schema without inventing a day or measurements.
DAILY_FIELDS = ('date chain_id coverage_status day_start_utc day_end_utc utc_day_seconds start_height end_height '
                'block_count block_coverage_pct measurement_start_time measurement_end_time measured_block_interval_seconds '
                'first_block_time tx_total tx_ibc declared_fee_atom declared_ibc_fee_atom tx_without_fee_event '
                'fee_collection_status collected_fee_atom collected_ibc_fee_atom collected_tip_atom collected_ibc_tip_atom '
                'estimated_emission_atom emission_method net_supply_change_atom fee_to_estimated_emission_pct '
                'ibc_fee_to_estimated_emission_pct fee_to_net_supply_increase_pct effective_burn_atom burn_status '
                'dilution_after_burn_atom historical_daily_price_usd daily_fee_usd price_status completed_at').split()


def export_daily(directory=DAY_DIR, path=DAILY_CSV):
    states = load_days(directory)
    output = io.StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=DAILY_FIELDS, lineterminator='\n')
    writer.writeheader()
    for state in states:
        if progress(state)['status'] == 'complete':
            writer.writerow(daily_summary(state))
    atomic_text(path, output.getvalue())


def collect_days(*, directory=DAY_DIR, now=None, budget_seconds=1200, chunk_blocks=120, workers=4, start_date=None):
    if not 1 <= chunk_blocks <= 1000 or not 1 <= workers <= 8 or budget_seconds <= 0:
        raise ValueError('Invalid collection limits')
    now = now or datetime.now(timezone.utc)
    yesterday = now.astimezone(timezone.utc).date() - timedelta(days=1)
    if start_date and start_date > yesterday:
        raise ValueError('START_DATE must be a closed UTC day')
    deadline = time.monotonic() + budget_seconds
    states = load_days(directory)
    if states and start_date and start_date != date.fromisoformat(states[0]['date']):
        raise ValueError('START_DATE cannot change an existing collection history')
    while time.monotonic() < deadline:
        if states and progress(states[-1])['status'] != 'complete':
            state = states[-1]
        else:
            day = date.fromisoformat(states[-1]['date']) + timedelta(days=1) if states else start_date or yesterday
            if day > yesterday:
                return
            state = plan_day(day, now=now)
            save_day(state, directory)
            states.append(state)
        p = progress(state)
        if p['status'] == 'complete':
            continue
        start = p['next_height']
        end = min(state['end_height'], start + chunk_blocks - 1)
        try:
            row = monitor.collect(start, end, deadline=deadline, workers=workers)
        except TimeoutError:
            print('Time budget reached; completed chunks retained for next execution.', flush=True)
            return
        # Convert to CSV-compatible strings before validation/persistence.
        candidate = dict(state, chunks=state['chunks'] + [{k: str(v) for k, v in row.items()}])
        save_day(candidate, directory)
        states[-1] = candidate
        p = progress(candidate)
        print(f'{p["date"]}: {p["scanned_blocks"]}/{p["expected_blocks"]} blocks ({p["status"]})', flush=True)


def main():
    try:
        collect_days(budget_seconds=int(os.environ.get('COLLECT_BUDGET_SECONDS', '1200')),
                     chunk_blocks=int(os.environ.get('CHUNK_BLOCKS', '120')),
                     workers=int(os.environ.get('COLLECT_WORKERS', '4')),
                     start_date=date.fromisoformat(os.environ['START_DATE']) if os.environ.get('START_DATE') else None)
    finally:
        # Even an endpoint failure preserves previously finished chunks for commit.
        export_daily()


if __name__ == '__main__':
    main()
