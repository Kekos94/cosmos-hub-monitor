"""Collect an exact, contiguous block window; never rewrite legacy daily data."""
import base64
import hashlib
import json
import os
import re
import time
import math
import threading
from contextvars import ContextVar
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from datetime import datetime, timezone
from decimal import Decimal
import requests
from criterion1_data import CHAIN, WINDOWS, append_window, day_coverage, enrich, read_rows, timestamp, utcnow, validate, number

RPCS = ['https://cosmos-rpc.polkachu.com', 'https://rpc.cosmos.directory/cosmoshub']
LCDS = ['https://cosmos-api.polkachu.com', 'https://rest.cosmos.directory/cosmoshub']
BLOCK_BATCH = int(os.environ.get('BLOCK_BATCH', '60'))


# Shared by all workers: parallelism must not multiply the provider request rate.
HTTP_MIN_INTERVAL = float(os.environ.get('HTTP_MIN_INTERVAL_SECONDS', '1.0'))
HTTP_MAX_ATTEMPTS = 4
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
REQUEST_DEADLINE = ContextVar('request_deadline', default=None)
_rate_lock = threading.Lock()
_next_request = {}


def wait_for_provider(base):
    host = urlsplit(base).netloc
    if not math.isfinite(HTTP_MIN_INTERVAL) or HTTP_MIN_INTERVAL < 0:
        raise ValueError('Invalid HTTP_MIN_INTERVAL_SECONDS')
    while True:
        with _rate_lock:
            now = time.monotonic()
            deadline = REQUEST_DEADLINE.get()
            wait = max(0, _next_request.get(host, 0) - now)
            if deadline is not None and (now >= deadline or now + wait >= deadline):
                raise TimeoutError('Provider cooldown exceeds remaining collection budget')
            if wait == 0:
                _next_request[host] = now + HTTP_MIN_INTERVAL
                return
        # Recheck after waiting: another worker may have extended the 429 cooldown.
        time.sleep(wait)


def retry_delay(response, attempt, *, now=None):
    fallback = float(2 ** attempt)
    if response is None:
        return fallback
    value = response.headers.get('Retry-After')
    if not isinstance(value, str):
        return fallback
    try:
        seconds = float(value)
        if not math.isfinite(seconds) or seconds < 0:
            return fallback
    except ValueError:
        try:
            updated = parsedate_to_datetime(value)
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            seconds = max(0, (updated - (now or datetime.now(timezone.utc))).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return fallback
    return max(fallback, seconds)


def defer_provider(base, delay):
    host = urlsplit(base).netloc
    with _rate_lock:
        _next_request[host] = max(_next_request.get(host, 0), time.monotonic() + delay)


def get_any(bases, path, *, height=None, params=None, rpc_calls=None):
    errors, terminal = [], set()
    for attempt in range(HTTP_MAX_ATTEMPTS):
        transient_failure = False
        for base in bases:
            if base in terminal:
                continue
            wait_for_provider(base)
            deadline = REQUEST_DEADLINE.get()
            timeout = min(25, deadline-time.monotonic()) if deadline is not None else 25
            if timeout <= 0:
                raise TimeoutError('Collection time budget reached')
            response = None
            try:
                if rpc_calls is None:
                    response = requests.get(base + path, params=params, timeout=timeout,
                                            headers={'x-cosmos-block-height': str(height)} if height is not None else {})
                else:
                    response = requests.post(base + path, json=rpc_calls, timeout=timeout)
                response.raise_for_status()
                if height is not None:
                    actual = response.headers.get('x-cosmos-block-height') or response.headers.get('grpc-metadata-x-cosmos-block-height')
                    if actual != str(height):
                        raise ValueError('Historical height not confirmed by response header')
                data = response.json()
                if rpc_calls is not None:
                    if not isinstance(data, list):
                        raise ValueError('JSON-RPC batching is not supported by this endpoint')
                elif data.get('error'):
                    raise ValueError(str(data['error']))
                return data, base
            except (requests.RequestException, ValueError) as exc:
                errors.append(f'{base} (attempt {attempt+1}): {exc}')
                retryable = (response is not None and response.status_code in RETRYABLE_STATUS) or (
                    response is None and isinstance(exc, (requests.ConnectionError, requests.Timeout)))
                if retryable:
                    transient_failure = True
                    delay = retry_delay(response, attempt)
                    defer_provider(base, delay)
                    print(f'Provider temporarily unavailable: {base}; retry after >= {delay:g}s', flush=True)
                else:
                    # A 404 or unconfirmed historical height is never converted to zero.
                    terminal.add(base)
        if not transient_failure:
            break
    raise RuntimeError('; '.join(errors))


def block(height):
    data, source = get_any(RPCS, '/block', params={'height': height})
    b = data['result']['block']
    if b['header']['chain_id'] != CHAIN or int(b['header']['height']) != height:
        raise ValueError('Wrong chain or block height')
    timestamp(b['header']['time'])
    return b, source


def coin_uatom(coins):
    total = 0
    for coin in coins or []:
        if not re.fullmatch(r'\d+', str(coin['amount'])):
            raise ValueError('Invalid coin amount')
        if coin['denom'] == 'uatom':
            total += int(coin['amount'])
    return total


def event_coins(value):
    if value == '':
        return 0
    coins = []
    for part in value.split(','):
        match = re.fullmatch(r'(\d+)([a-zA-Z][a-zA-Z0-9/:._-]*)', part)
        if not match:
            raise ValueError('Invalid fee event coin')
        coins.append({'amount': match[1], 'denom': match[2]})
    return coin_uatom(coins)


def collected_fee_components(tx, *, collector=None):
    """Read SDK deduction or fee-market fee+tip events without double counting.

    A failed message can still pay fees. Missing/ambiguous events are unknown.
    Neither event attests a burn or the final beneficiary of all fees.
    """
    events = tx['tx_response'].get('events', [])
    def values(event_type, key):
        return [a['value'] for e in events if e['type'] == event_type
                for a in e.get('attributes', []) if a['key'] == key]
    sdk = values('tx', 'fee')
    market = values('fee_pay', 'fee')
    tips = values('tip_pay', 'tip')
    if len(sdk) > 1 or len(market) > 1 or len(tips) > 1:
        return None, None
    tip = event_coins(tips[0]) if tips else 0
    if market:
        total = event_coins(market[0]) + tip
        if sdk and event_coins(sdk[0]) != total:
            return None, None
        return total, tip
    if sdk:
        total = event_coins(sdk[0])
        if tip > total:
            return None, None
        return total, tip
    from fee_evidence import failed_ante_fee
    return failed_ante_fee(tx, collector)


_collector_proofs = {}


def collector_proof(height):
    if height not in _collector_proofs:
        name = 'feemarket-fee-collector'
        result, source = get_any(LCDS, '/cosmos/auth/v1beta1/module_accounts/' + name, height=height)
        account = result['account']
        if account.get('name') != name or account.get('@type') != '/cosmos.auth.v1beta1.ModuleAccount':
            raise ValueError('Wrong fee collector module account')
        _collector_proofs[height] = dict(height=height, account=account, source=source, observed_at=utcnow())
    return _collector_proofs[height]


def collected_fee(tx):
    return collected_fee_components(tx)[0]


def macro(height, block_time, side):
    result = {f'supply_{side}_atom': '', f'inflation_{side}': '',
              f'macro_{side}_height': '', f'macro_{side}_block_time': '',
              f'macro_{side}_observed_at': '', f'macro_{side}_sources': '', f'macro_{side}_error': ''}
    try:
        supply, src = get_any(LCDS, '/cosmos/bank/v1beta1/supply/by_denom', height=height, params={'denom': 'uatom'})
        inflation, inf_src = get_any(LCDS, '/cosmos/mint/v1beta1/inflation', height=height)
        if supply['amount']['denom'] != 'uatom':
            raise ValueError('Invalid supply denom')
        supply_atom = number(supply['amount']['amount']) / 1000000
        rate = number(inflation['inflation'])
        if supply_atom <= 0 or rate < 0:
            raise ValueError('Invalid macro snapshot')
        result.update({f'supply_{side}_atom': str(supply_atom), f'inflation_{side}': str(rate),
                       f'macro_{side}_height': height, f'macro_{side}_block_time': block_time,
                       f'macro_{side}_observed_at': utcnow(), f'macro_{side}_sources': f'{src};{inf_src}'})
    except (RuntimeError, ValueError, KeyError) as exc:
        result[f'macro_{side}_error'] = str(exc)
    return result


def price_snapshot():
    result = dict(atom_price_usd='', price_observed_at='', price_source_updated_at='',
                  price_source='https://api.coingecko.com/api/v3/simple/price', price_error='')
    try:
        response = requests.get(result['price_source'], params={'ids': 'cosmos', 'vs_currencies': 'usd',
                                                               'include_last_updated_at': 'true'}, timeout=20)
        response.raise_for_status()
        price = response.json()['cosmos']
        observed = datetime.now(timezone.utc)
        updated = datetime.fromtimestamp(price['last_updated_at'], timezone.utc)
        if not 0 <= (observed - updated).total_seconds() <= 3600 or number(price['usd']) <= 0:
            raise ValueError('Invalid or stale price')
        result.update(atom_price_usd=str(price['usd']), price_observed_at=observed.isoformat(),
                      price_source_updated_at=updated.isoformat())
    except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
        result['price_error'] = str(exc)
    return result


def block_payload(height, *, deadline=None):
    token = REQUEST_DEADLINE.set(deadline)
    try:
        return _block_payload(height, deadline=deadline)
    finally:
        REQUEST_DEADLINE.reset(token)


def _block_payload(height, *, deadline=None):
    def check_budget():
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError('Collection time budget reached')
    check_budget()
    b, source = block(height)
    transactions = []
    for encoded in b['data'].get('txs') or []:
        check_budget()
        tx_hash = hashlib.sha256(base64.b64decode(encoded, validate=True)).hexdigest().upper()
        tx, lcd_source = get_any(LCDS, '/cosmos/tx/v1beta1/txs/' + tx_hash)
        response = tx['tx_response']
        if int(response['height']) != height or response['txhash'].upper() != tx_hash:
            raise ValueError('Transaction does not match requested block/hash')
        transactions.append((tx, lcd_source))
    return height, b, source, transactions


def collect(start, end, *, deadline=None, workers=1, bulk=False):
    token = REQUEST_DEADLINE.set(deadline)
    try:
        return _collect(start, end, deadline=deadline, workers=workers, bulk=bulk)
    finally:
        REQUEST_DEADLINE.reset(token)


def _collect(start, end, *, deadline=None, workers=1, bulk=False):
    boundary, source = block(start - 1)
    start_time = boundary['header']['time']
    r = dict(chain_id=CHAIN, start_height=start, end_height=end, block_count=end-start+1,
             start_boundary_time=start_time, first_block_time='', end_time='', duration_seconds='',
             utc_day_coverage_seconds='', utc_day_coverage_pct='',
             coverage_kind='contiguous_window_not_daily_total', collected_at='',
             tx_total=0, tx_ibc=0, declared_fee_atom='', declared_ibc_fee_atom='',
             collected_fee_atom='', collected_ibc_fee_atom='', fee_collection_status='complete',
             tx_without_fee_event=0, fee_ante_transfer_count=0, collected_tip_atom='', collected_ibc_tip_atom='', rpc_sources='', lcd_sources='')
    declared = declared_ibc = collected = collected_ibc = tips = ibc_tips = 0
    rpc_sources, lcd_sources = {source}, set()
    fee_proofs = {}
    previous_time = timestamp(start_time)
    fetch = partial(block_payload, deadline=deadline)
    if bulk:
        import sys
        from criterion1_rpc_bulk import payloads as bulk_payloads
        payloads = bulk_payloads(sys.modules[__name__], start, end, deadline=deadline)
    elif workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            payloads = list(pool.map(fetch, range(start, end + 1)))
    else:
        payloads = map(fetch, range(start, end + 1))
    for height, b, source, transactions in payloads:
        rpc_sources.update(source.split(';'))
        current_time = timestamp(b['header']['time'])
        if current_time <= previous_time:
            raise ValueError('Non-increasing block times')
        previous_time = current_time
        if height == start:
            r['first_block_time'] = b['header']['time']
        r['end_time'] = b['header']['time']
        for tx, source in transactions:
            if source:
                lcd_sources.add(source)
            is_ibc = any(m.get('@type', '').startswith('/ibc.') for m in tx['tx']['body']['messages'])
            fee = coin_uatom(tx['tx']['auth_info']['fee']['amount'])
            if fee < 0:
                raise ValueError('Negative fee')
            actual, tip = collected_fee_components(tx)
            if actual is None and int(tx['tx_response'].get('code', 0)) != 0:
                try:
                    proof = collector_proof(height)
                    actual, tip = collected_fee_components(tx, collector=proof)
                except (RuntimeError, ValueError, KeyError):
                    actual, tip = None, None
                if actual is not None:
                    r['fee_ante_transfer_count'] += 1
                    fee_proofs[str(height)] = proof
            r['tx_total'] += 1
            r['tx_ibc'] += int(is_ibc)
            declared += fee
            declared_ibc += fee if is_ibc else 0
            if actual is None:
                r['tx_without_fee_event'] += 1
            else:
                collected += actual
                collected_ibc += actual if is_ibc else 0
                tips += tip
                ibc_tips += tip if is_ibc else 0
    r.update(declared_fee_atom=str(Decimal(declared) / 1000000),
             declared_ibc_fee_atom=str(Decimal(declared_ibc) / 1000000))
    if r['tx_without_fee_event']:
        r['fee_collection_status'] = 'incomplete'
    else:
        r.update(collected_fee_atom=str(Decimal(collected) / 1000000),
                 collected_ibc_fee_atom=str(Decimal(collected_ibc) / 1000000),
                 collected_tip_atom=str(Decimal(tips) / 1000000), collected_ibc_tip_atom=str(Decimal(ibc_tips) / 1000000))
    r['duration_seconds'] = (timestamp(r['end_time']) - timestamp(start_time)).total_seconds()
    coverage = day_coverage(timestamp(start_time), timestamp(r['end_time']))
    r['utc_day_coverage_seconds'] = json.dumps(coverage, sort_keys=True)
    r['utc_day_coverage_pct'] = json.dumps({k: v / 86400 * 100 for k, v in coverage.items()}, sort_keys=True)
    r['fee_ante_transfer_proofs'] = json.dumps(fee_proofs, sort_keys=True)
    r.update(macro(start - 1, start_time, 'start'))
    r.update(macro(end, r['end_time'], 'end'))
    r.update(price_snapshot())
    r.update(collected_at=utcnow(), rpc_sources=';'.join(sorted(rpc_sources)), lcd_sources=';'.join(sorted(lcd_sources)))
    return enrich(r)


def main():
    if not 1 <= BLOCK_BATCH <= 10000:
        raise ValueError('BLOCK_BATCH must be between 1 and 10000')
    rows = read_rows(WINDOWS)
    validate(rows)
    status, _ = get_any(RPCS, '/status')
    info = status['result']
    if info['node_info']['network'] != CHAIN or info['sync_info']['catching_up']:
        raise ValueError('Endpoint is on wrong chain or catching up')
    latest = int(info['sync_info']['latest_block_height'])
    start = int(rows[-1]['end_height']) + 1 if rows else max(2, latest - BLOCK_BATCH + 1)
    if start > latest:
        print('No new blocks; ledger unchanged.')
        return
    end = min(latest, start + BLOCK_BATCH - 1)
    # Any block/tx failure aborts before any write. Cursor and measurements are one file.
    row = collect(start, end)
    row.update(latest_height_seen=latest, unprocessed_blocks_seen=latest-end)
    append_window(row, WINDOWS)
    print(f'Committed window {start}..{end}: {row["duration_seconds"]} s, {row["block_count"]} blocks; latest={latest}')
    print(f'Fee collection: {row["fee_collection_status"]}; burn: not measured')


if __name__ == '__main__':
    main()
