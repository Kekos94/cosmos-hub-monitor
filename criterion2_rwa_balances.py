"""Historical bank supply and ICS20 escrow for reviewed RWA representations."""
import argparse
import base64
import re
import time
from pathlib import Path

import hub_fee_monitor_v41 as http
from cosmos_tx_decode import single
from criterion1_data import utcnow
from criterion2_core import decimal_amount, digest
from criterion2_export import write_csv
from criterion2_lifecycle_evidence import bank_denom
from criterion2_pipeline import DATA, ROOT, load_config, read, validate_plan, write_once
from criterion2_rwa_registry import identify, load_registry
from criterion2_transit_collect import batch, channel_requests
from criterion2_transit_evidence import encode_field, topology

BALANCE_ROOT = ROOT / 'data/criterion2_rwa_balances'
SUPPLY = '/cosmos.bank.v1beta1.Query/SupplyOf'
ESCROW = '/ibc.applications.transfer.v1.Query/TotalEscrowForDenom'
FIELDS = ('registry_id date chain_id product_id symbol denom height block_time_utc status '
          'supply_raw escrow_raw local_raw supply_amount escrow_amount local_amount source '
          'observed_at evidence_sha256').split()


def coin_amount(result, height, denom):
    response = result['response']
    if int(response['code']) != 0 or str(response['height']) != str(height):
        raise ValueError('Historical amount query failed or returned the wrong height')
    raw = base64.b64decode(response['value'], validate=True)
    coin = single(raw, 1, required=True)
    if single(coin, 1, required=True).decode() != denom:
        raise ValueError('Historical coin denomination differs from registry')
    amount = single(coin, 2, required=True).decode()
    if not re.fullmatch(r'[0-9]+', amount):
        raise ValueError('Invalid historical coin amount')
    return int(amount)


def queries(height, denom, representation):
    params = dict(data=encode_field(1, denom.encode()).hex(), height=str(height), prove=False)
    calls = [
        dict(jsonrpc='2.0', id=name, method='abci_query', params=dict(params, path=path))
        for name, path in [('supply', SUPPLY), ('escrow', ESCROW)]
    ]
    if representation['kind'] == 'ics20_single_hop':
        calls.extend(channel_requests(height, representation['channel']))
    return calls


def amounts(results, plan, representation, product, registry):
    height = plan['end_height']
    denom = bank_denom(representation['denom'])
    supply = coin_amount(results['supply'], height, denom)
    escrow = coin_amount(results['escrow'], height, denom)
    if escrow > supply:
        raise ValueError('Historical ICS20 escrow exceeds bank supply')
    if representation['kind'] == 'ics20_single_hop':
        route = topology(results['Channel'], results['ChannelClientState'], height)
        if (route['chain_id'], route['counterparty_port'], route['counterparty_channel']) != (
                product['origin_chain_id'], 'transfer', representation['counterparty_channel']):
            raise ValueError('Historical channel does not match the reviewed representation')
    else:
        native = dict(chain_id=plan['chain_id'], denom=denom, height=height)
        if identify(native, plan, registry, ROOT)['included_in_strict_rwa'] != 'true':
            raise ValueError('Native units are unavailable')
    return supply, escrow, supply - escrow


def closing_plans():
    _, universe = load_config()
    plans = {}
    for path in sorted((DATA / universe).glob('*/*/plan.json')):
        plan = read(path)
        validate_plan(plan)
        if plan['universe_id'] != universe:
            raise ValueError('RWA balance plan belongs to another universe')
        key = plan['date'], plan['chain_id']
        if key in plans:
            raise ValueError('Duplicate closing plan')
        plans[key] = plan
    return plans


def tasks(registry, registry_id):
    products = {p['id']: p for p in registry['products'] if p['include_in_strict_rwa']}
    plans = closing_plans()
    for day in sorted({day for day, _ in plans}, reverse=True):
        for rep in registry['representations']:
            product = products.get(rep['product_id'])
            if product is None:
                continue
            chain = rep['chain_id']
            denom = bank_denom(rep['denom'])
            path = BALANCE_ROOT / registry_id / day / chain / (digest(denom) + '.json')
            yield day, rep, product, denom, plans.get((day, chain)), path


def identity(registry_id, day, chain, denom, plan):
    return dict(registry_id=registry_id, date=day, chain_id=chain, denom=denom,
                height=plan['end_height'], block_time_utc=plan['last_block']['time'],
                source=plan['source'], plan_sha256=digest(plan))


def collect(budget_seconds):
    if budget_seconds <= 0:
        raise ValueError('RWA balance budget must be positive')
    registry, registry_id = load_registry()
    deadline = time.monotonic() + budget_seconds
    token = http.REQUEST_DEADLINE.set(deadline)
    try:
        for day, rep, product, denom, plan, path in tasks(registry, registry_id):
            if plan is None or path.exists():
                continue
            if time.monotonic() >= deadline:
                return
            try:
                results = batch(plan['source'], queries(plan['end_height'], denom, rep))
                amounts(results, plan, rep, product, registry)
                proof = identity(registry_id, day, rep['chain_id'], denom, plan)
                write_once(path, dict(proof, observed_at=utcnow(), results=results))
                print(f"RWA balances {day} {rep['chain_id']} {product['symbol']}: verified", flush=True)
            except TimeoutError:
                return
            except (RuntimeError, ValueError, KeyError, TypeError) as exc:
                print(f"RWA balances {day} {rep['chain_id']} unavailable: {exc}", flush=True)
    finally:
        http.REQUEST_DEADLINE.reset(token)


def export():
    registry, registry_id = load_registry()
    rows = []
    for day, rep, product, denom, plan, path in tasks(registry, registry_id):
        row = dict.fromkeys(FIELDS, '')
        row.update(registry_id=registry_id, date=day, chain_id=rep['chain_id'],
                   product_id=product['id'], symbol=product['symbol'], denom=denom,
                   status='missing_closing_height' if plan is None else 'pending_historical_proof')
        if plan is not None:
            expected = identity(registry_id, day, rep['chain_id'], denom, plan)
            row.update(height=expected['height'], block_time_utc=expected['block_time_utc'])
            if path.exists():
                proof = read(path)
                if any(proof.get(k) != v for k, v in expected.items()):
                    raise ValueError('RWA balance proof does not match its closing plan')
                values = amounts(proof['results'], plan, rep, product, registry)
                row.update(status='verified', source=proof['source'],
                           observed_at=proof['observed_at'], evidence_sha256=digest(proof))
                for field, value in zip(('supply', 'escrow', 'local'), values):
                    row[field + '_raw'] = str(value)
                    row[field + '_amount'] = str(decimal_amount(value, product['decimals']))
        rows.append(row)
    write_csv(ROOT / 'ibc_rwa_balances_daily.csv', rows, FIELDS)
    return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--collect', type=int, metavar='SECONDS')
    args = parser.parse_args()
    if args.collect is not None:
        collect(args.collect)
    export()
