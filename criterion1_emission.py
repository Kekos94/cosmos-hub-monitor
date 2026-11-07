"""Audit native ATOM supply movements against historical bank supply."""
import argparse
import csv
import gzip
import io
import json
import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import hub_fee_monitor_v41 as rpc
from criterion1_daily import DAY_DIR, atomic_text, load_days, progress
from criterion1_data import ROOT, timestamp, utcnow
from criterion1_rpc_bulk import normalized_events

AUDITS = ROOT / 'data/criterion1_emission'
EXPORT = ROOT / 'hub_criterion1_emission_daily.csv'
PROVIDERS = [rpc.RPCS[0], 'https://cosmos-rpc.publicnode.com', 'https://cosmoshub.rpc.kjnodes.com']
# Gaia mint module account, independently verified at the audit boundary.
MINTER = 'cosmos1m3h30wlvsf8llruxtpukdvsy0km2kum8g38c8q'
CHUNK = 120
YEAR = Decimal(365 * 86400)
FIELDS = ('date status start_height end_height blocks_audited block_count interval_seconds '
          'opening_supply_atom closing_supply_atom monetary_emission_atom gross_mint_atom '
          'effective_burn_atom net_supply_change_atom reconciliation_gap_atom '
          'annualized_monetary_inflation_pct annualized_net_supply_growth_pct '
          'monetary_inflation_365d_pct criterion3_status').split()


def attribute(event, key):
    values = [a['value'] for a in event['attributes'] if a['key'] == key]
    if len(values) != 1:
        raise ValueError(f'Expected one {key} on {event["type"]}')
    return values[0]


def block_amounts(result, height):
    if int(result['height']) != height:
        raise ValueError('Wrong historical block result height')
    events = normalized_events(result['finalize_block_events'])
    # Governance and other end-block actions may mint or burn outside transactions.
    tx_results = result['txs_results'] or []
    if not isinstance(result['finalize_block_events'], list) or not isinstance(tx_results, list):
        raise ValueError('Missing block execution events')
    committed = events + [
        event for tx in tx_results if int(tx.get('code', 0)) == 0
        for event in normalized_events(tx.get('events'))
    ]
    provisions = []
    for event in events:
        if event['type'] == 'mint':
            amount = attribute(event, 'amount')
            if not re.fullmatch(r'[0-9]+', amount):
                raise ValueError('Invalid monetary issuance event')
            provisions.append(int(amount))
    monetary = gross = burned = 0
    for event in committed:
        if event['type'] == 'coinbase':
            amount = rpc.event_coins(attribute(event, 'amount'))
            gross += amount
            if attribute(event, 'minter') == MINTER:
                monetary += amount
        elif event['type'] == 'burn':
            burned += rpc.event_coins(attribute(event, 'amount'))
    if monetary != sum(provisions):
        raise ValueError(f'Monetary mint and bank credit disagree at {height}')
    return [height, monetary, gross, burned]


def bounds(state):
    return {k: state[k] for k in ('chain_id', 'date', 'start_height', 'end_height', 'previous_block', 'last_block')}


def supplies(state):
    def snapshot(block):
        h = block['height']
        result, source = rpc.get_any(rpc.LCDS, '/cosmos/bank/v1beta1/supply/by_denom',
                                     height=h, params={'denom': 'uatom'})
        coin = result['amount']
        value = rpc.coin_uatom([coin])
        if value <= 0:
            raise ValueError('Invalid native supply at audit boundary')
        return dict(height=h, uatom=value, source=source)
    for boundary in ('previous_block', 'last_block'):
        account, _ = rpc.get_any(rpc.LCDS, '/cosmos/auth/v1beta1/module_accounts/mint',
                                 height=state[boundary]['height'])
        module = account['account']
        if module['name'] != 'mint' or module['base_account']['address'] != MINTER or 'minter' not in module['permissions']:
            raise ValueError('Unexpected historical monetary module account')
    return dict(bounds=bounds(state), opening=snapshot(state['previous_block']),
                closing=snapshot(state['last_block']), observed_at=utcnow())


def get_batch(heights, provider, deadline, state, verified):
    token = rpc.REQUEST_DEADLINE.set(deadline)
    try:
        if provider not in verified:
            for key in ('previous_block', 'last_block'):
                expected = state[key]
                answer, _ = rpc.get_any([provider], '/block', params={'height': expected['height']})
                header = answer['result']['block']['header']
                if (header['chain_id'], int(header['height']), header['time']) != (
                        expected['chain_id'], expected['height'], expected['time']):
                    raise ValueError('RPC does not match the saved Cosmos Hub day bounds')
            verified.add(provider)
        if len(heights) == 1:
            height = heights[0]
            answer, source = rpc.get_any([provider], '/block_results', params={'height': height})
            return source, [block_amounts(answer['result'], height)]
        calls = [dict(jsonrpc='2.0', id=str(h), method='block_results', params={'height': str(h)}) for h in heights]
        # Use only the validated provider for this batch.
        answer, source = rpc.get_any([provider], '/', rpc_calls=calls)
        if any(item.get('error') or not isinstance(item.get('result'), dict) for item in answer):
            raise ValueError('Historical block batch contains an RPC error')
        by_id = {item['id']: item['result'] for item in answer}
        if len(answer) != len(heights) or set(by_id) != {str(h) for h in heights}:
            raise ValueError('Invalid historical block batch IDs')
        return source, [block_amounts(by_id[str(h)], h) for h in heights]
    finally:
        rpc.REQUEST_DEADLINE.reset(token)


def checkpoint(path, value):
    atomic_text(path, json.dumps(value, separators=(',', ':')) + '\n')


def chunk_paths(state, path):
    return [path / f'{first}.json' for first in range(state['start_height'], state['end_height']+1, CHUNK)]


def audit_chunks(state, path):
    archive = path / 'blocks.json.gz'
    if archive.is_file():
        with gzip.open(archive, 'rt', encoding='utf-8') as stream:
            yield from json.load(stream)
        return
    for file in chunk_paths(state, path):
        if not file.is_file():
            return
        yield json.loads(file.read_text())


def compact(state, path):
    files = chunk_paths(state, path)
    archive = path / 'blocks.json.gz'
    if not archive.exists():
        chunks = list(audit_chunks(state, path))
        if len(chunks) != len(files):
            raise ValueError('Cannot compact incomplete issuance audit')
        with tempfile.NamedTemporaryFile(dir=path, delete=False) as stream:
            temporary = Path(stream.name)
            try:
                stream.write(gzip.compress(json.dumps(chunks, separators=(',', ':')).encode(), mtime=0))
                stream.flush()
                os.fsync(stream.fileno())
                os.replace(temporary, archive)
            finally:
                temporary.unlink(missing_ok=True)
    for file in files:
        file.unlink(missing_ok=True)


def interval_seconds(start, end):
    def fraction(value):
        digits = re.search(r'\.(\d+)', value)
        return Decimal('0.' + digits[1]) if digits else Decimal(0)

    whole = timestamp(end).replace(microsecond=0) - timestamp(start).replace(microsecond=0)
    return Decimal(whole.days * 86400 + whole.seconds) + fraction(end) - fraction(start)


def summarize(state, root=AUDITS):
    path = Path(root) / state['date']
    start, end = state['start_height'], state['end_height']
    count = end - start + 1
    row = dict.fromkeys(FIELDS, '')
    row.update(date=state['date'], status='pending', start_height=start, end_height=end,
               block_count=count, blocks_audited=0, criterion3_status='insufficient_data')
    if not (path / 'supply.json').is_file():
        return row
    supply = json.loads((path / 'supply.json').read_text())
    if supply['bounds'] != bounds(state):
        raise ValueError('Audit supply belongs to different day bounds')
    opening, closing = supply['opening'], supply['closing']
    if opening['height'] != start - 1 or closing['height'] != end or min(opening['uatom'], closing['uatom']) <= 0:
        raise ValueError('Invalid audit supply heights or values')
    monetary = gross = burned = 0
    for index, chunk in enumerate(audit_chunks(state, path)):
        first = start + index * CHUNK
        blocks = chunk['blocks']
        if first > end or chunk['bounds'] != bounds(state) or [b[0] for b in blocks] != list(range(first, min(first+CHUNK, end+1))):
            raise ValueError('Audit checkpoint has wrong bounds or missing blocks')
        for _, m, g, b in blocks:
            if any(type(v) is not int for v in (m, g, b)) or not 0 <= m <= g or b < 0:
                raise ValueError('Invalid block supply movements')
            monetary += m
            gross += g
            burned += b
        row['blocks_audited'] += len(blocks)
    if row['blocks_audited'] != count:
        return row
    seconds = interval_seconds(state['previous_block']['time'], state['last_block']['time'])
    if seconds <= 0:
        raise ValueError('Audit boundary duration must be positive')
    change = closing['uatom'] - opening['uatom']
    gap = gross - burned - change
    atom = lambda n: str(Decimal(n) / 1_000_000)
    row.update(status='unreconciled' if gap else 'reconciled', interval_seconds=str(seconds),
               opening_supply_atom=atom(opening['uatom']), closing_supply_atom=atom(closing['uatom']),
               net_supply_change_atom=atom(change), reconciliation_gap_atom=atom(gap))
    if gap:
        return row
    factor = 100 * YEAR / (opening['uatom'] * seconds)
    row.update(monetary_emission_atom=atom(monetary), gross_mint_atom=atom(gross), effective_burn_atom=atom(burned),
               annualized_monetary_inflation_pct=str(monetary * factor),
               annualized_net_supply_growth_pct=str(change * factor))
    return row


def annual_rates(rows):
    by_date = {date.fromisoformat(row['date']): row for row in rows if row['status'] == 'reconciled'}
    for day, row in by_date.items():
        window = [by_date.get(day - timedelta(days=i)) for i in range(364, -1, -1)]
        if None in window or any(a['end_height'] + 1 != b['start_height'] or
                                 a['closing_supply_atom'] != b['opening_supply_atom']
                                 for a, b in zip(window, window[1:])):
            continue
        total = sum(Decimal(r['monetary_emission_atom']) for r in window)
        opening = Decimal(window[0]['opening_supply_atom'])
        row['monetary_inflation_365d_pct'] = str(total * 100 / opening)
        row['criterion3_status'] = 'below_5_pct' if total * 100 < opening * 5 else 'at_or_above_5_pct'


def export(states, path=EXPORT, root=AUDITS):
    rows = [summarize(state, root) for state in states if progress(state)['status'] == 'complete']
    annual_rates(rows)
    output = io.StringIO()
    writer = csv.DictWriter(output, FIELDS, lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    atomic_text(path, output.getvalue())


def collect(states, budget, root=AUDITS):
    if budget <= 0:
        raise ValueError('Audit budget must be positive')
    deadline = time.monotonic() + budget
    token = rpc.REQUEST_DEADLINE.set(deadline)
    try:
        providers = list(PROVIDERS)
        with ThreadPoolExecutor(max_workers=len(providers)) as pool:
            for state in reversed(states):
                if progress(state)['status'] != 'complete':
                    continue
                path = Path(root) / state['date']
                if not (path / 'supply.json').exists():
                    checkpoint(path / 'supply.json', supplies(state))
                if (path / 'blocks.json.gz').is_file():
                    summarize(state, root)
                    continue
                verified = set()
                for first in range(state['start_height'], state['end_height'] + 1, CHUNK):
                    file = path / f'{first}.json'
                    if file.exists():
                        continue
                    heights = list(range(first, min(first + CHUNK, state['end_height'] + 1)))
                    batches = [heights[i:i+10] for i in range(0, len(heights), 10)]
                    blocks, sources = [], set()
                    while batches:
                        wave, batches = batches[:len(providers)], batches[len(providers):]
                        futures = [(provider, batch, pool.submit(get_batch, batch, provider, deadline, state, verified))
                                   for batch, provider in zip(wave, providers)]
                        for provider, batch, future in futures:
                            try:
                                source, rows = future.result()
                            except (RuntimeError, ValueError, KeyError) as exc:
                                providers.remove(provider)
                                if not providers:
                                    raise RuntimeError('No usable emission RPC remains') from exc
                                print(f'Emission RPC {provider} unavailable: {exc}', flush=True)
                                batches.append(batch)
                            else:
                                sources.add(source)
                                blocks.extend(rows)
                    blocks.sort()
                    checkpoint(file, dict(bounds=bounds(state), blocks=blocks, sources=sorted(sources), observed_at=utcnow()))
                    if (first - state['start_height']) % (CHUNK * 10) == 0:
                        print(f'{state["date"]}: emission audit through block {blocks[-1][0]}', flush=True)
                result = summarize(state, root)
                compact(state, path)
                print(f'{state["date"]}: {result["status"]} (gap {result["reconciliation_gap_atom"]} ATOM)', flush=True)
    except TimeoutError:
        print('Emission audit time budget reached; validated batches retained.', flush=True)
    finally:
        rpc.REQUEST_DEADLINE.reset(token)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--collect', type=int, metavar='SECONDS')
    parser.add_argument('--day', type=date.fromisoformat)
    args = parser.parse_args()
    states = load_days(DAY_DIR)
    try:
        if args.collect is not None:
            collect([s for s in states if not args.day or s['date'] == str(args.day)], args.collect)
    finally:
        export(states)
