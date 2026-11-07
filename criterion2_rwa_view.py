"""Daily IBC activity for exact identities reviewed in the RWA registry."""
import argparse
import csv
import gzip
import time
from collections import defaultdict
from pathlib import Path

import hub_fee_monitor_v41 as http
from criterion2_core import decimal_amount, digest
from criterion1_data import utcnow
from criterion2_export import all_days, write_csv
from criterion2_lifecycle_collect import journals, config as lifecycle_config
from criterion2_lifecycle_evidence import bank_denom
from criterion2_lifecycle_export import key, reconcile
from criterion2_net import config as net_config, FAILURES
from criterion2_pipeline import DATA, IN_DATA, ROOT, write_once
from criterion2_rwa_registry import load_registry, identify
from criterion2_transit_collect import batch, channel_requests
from criterion2_transit_evidence import MODEL, topology

FIELDS = ('date chain_id product_id symbol verified_in verified_out confirmed_in confirmed_out '
          'net_amount pending_identity unsettled transit_packets transit_amount').split()


def cohorts():
    _, outgoing, incoming, _ = net_config()
    for direction, root, universe in [('out', DATA, outgoing), ('in', IN_DATA, incoming)]:
        for day in all_days(root):
            if day['complete'] and day['plan']['universe_id'] == universe:
                yield direction, day


def collect_routes(budget):
    registry, _ = load_registry()
    folder = ROOT / Path(registry['sources_manifest']).parent
    strict = {p['id']: p for p in registry['products'] if p['include_in_strict_rwa']}
    representations = {
        (r['chain_id'], bank_denom(r['denom'])): r for r in registry['representations']
        if r['kind'] == 'ics20_single_hop' and r['product_id'] in strict
    }
    deadline = time.monotonic() + budget
    token = http.REQUEST_DEADLINE.set(deadline)
    attempted = set()
    try:
        for _, day in cohorts():
            plan = day['plan']
            for packet in day['packets']:
                route = representations.get((packet['chain_id'], bank_denom(packet['denom'])))
                if route is None:
                    continue
                channel = route['channel']
                height = str(packet['height'])
                path = folder / f"{packet['chain_id']}_{height}_{channel}.json"
                if path.exists() or path in attempted:
                    continue
                if time.monotonic() >= deadline:
                    return
                attempted.add(path)
                try:
                    results = batch(plan['source'], channel_requests(height, channel))
                    proof = topology(results['Channel'], results['ChannelClientState'], height)
                    origin = strict[route['product_id']]['origin_chain_id']
                    if (proof['chain_id'], proof['counterparty_port'], proof['counterparty_channel']) != (
                            origin, 'transfer', route['counterparty_channel']):
                        raise ValueError('Historical route does not match the registry')
                    write_once(path, dict(chain_id=packet['chain_id'], height=height, channel=channel,
                                          source=plan['source'], observed_at=utcnow(), results=results))
                except TimeoutError:
                    return
                except (RuntimeError, ValueError, KeyError) as exc:
                    print(f'RWA route unavailable for {path.stem}: {exc}', flush=True)
    finally:
        http.REQUEST_DEADLINE.reset(token)


def read_rows(path, compressed=False):
    opener = gzip.open if compressed else open
    with opener(path, 'rt', newline='', encoding='utf-8') as stream:
        return list(csv.DictReader(stream))


def export(root=ROOT):
    registry, _ = load_registry(root=root)
    strict = {p['id']: p for p in registry['products'] if p['include_in_strict_rwa']}
    folder = root / Path(registry['sources_manifest']).parent
    _, outgoing, incoming, net_uid = net_config()
    transit_uid = digest(dict(model=MODEL, incoming=incoming, outgoing=outgoing))[:16]
    lifecycle = {
        (r['date'], r['chain_id'], r['direction'], r['packet_id'], r['height']): r['outcome']
        for r in read_rows(root / 'ibc_packet_lifecycle.csv.gz', compressed=True)
        if r['universe_id'] == {'out': outgoing, 'in': incoming}.get(r['direction'])
    }
    net_days = {(r['date'], r['chain_id']): r for r in read_rows(root / 'ibc_net_chains_daily.csv')
                if r['universe_id'] == net_uid and r['coverage_status'] == 'common_indexed_day'}
    evidence = defaultdict(list)
    _, life_uid = lifecycle_config()
    for journal in journals():
        plan = journal['plan']
        if journal['complete'] and plan['universe_id'] == life_uid:
            for event in journal['records']:
                evidence[key(event)].append((plan['date'], event))

    identities = {
        (r['chain_id'], bank_denom(r['denom'])): r['product_id']
        for r in registry['representations'] if r['product_id'] in strict
    }
    groups = defaultdict(list)
    hub_receipts = {}
    for direction, day in cohorts():
        plan = day['plan']
        for packet in day['packets']:
            product = identities.get((packet['chain_id'], bank_denom(packet['denom'])))
            if product is None:
                continue
            try:
                identity = identify(packet, plan, registry, folder)
                verified = identity['included_in_strict_rwa'] == 'true'
            except ValueError as exc:
                print(f"RWA identity unverified for {packet['packet_id']}: {exc}", flush=True)
                verified = False
            groups[(plan['date'], plan['chain_id'], product)].append((direction, packet, verified))
            if direction == 'in' and plan['chain_id'] == 'cosmoshub-4' and verified:
                hub_receipts[(plan['date'], packet['packet_id'], str(packet['height']))] = product

    transits = defaultdict(list)
    for row in read_rows(root / 'ibc_transit_packets.csv'):
        product = hub_receipts.get((row['date'], row['parent_packet_id'], row['height']))
        if product and row['universe_id'] == transit_uid and row['status'] == 'confirmed_transit':
            transits[(row['date'], 'cosmoshub-4', product)].append(int(row['amount_raw']))

    rows = []
    for (day, chain, product), packets in sorted(groups.items()):
        decimals = strict[product]['decimals']
        counts = {'in': 0, 'out': 0}
        totals = {'in': 0, 'out': 0}
        successes = {'in': 0, 'out': 0}
        common_net = net_days.get((day, chain))
        cutoff = common_net['evidence_through_utc'][:10] if common_net else ''
        net_raw = 0
        pending = unsettled = 0
        net_complete = bool(cutoff and not int(common_net['unknown_payload_packets']))
        for direction, packet, verified in packets:
            if not verified:
                pending += 1
                net_complete = False
                continue
            counts[direction] += 1
            outcome = lifecycle.get((day, chain, direction, packet['packet_id'], str(packet['height'])))
            amount = int(packet['amount_raw'])
            if outcome == 'success_confirmed':
                totals[direction] += amount
                successes[direction] += 1
            elif outcome not in FAILURES:
                unsettled += 1
            if cutoff:
                relevant = [event for event_day, event in evidence[key(packet)]
                            if day <= event_day < cutoff and
                            (event['event_type'] == 'write_acknowledgement') == (direction == 'in')]
                outcome_at_cutoff = reconcile(packet, relevant, direction)['outcome']
                if outcome_at_cutoff == 'success_confirmed':
                    net_raw += amount if direction == 'in' else -amount
                elif outcome_at_cutoff not in FAILURES:
                    net_complete = False
        transit_amounts = transits[(day, chain, product)]
        rows.append(dict(date=day, chain_id=chain, product_id=product, symbol=strict[product]['symbol'],
                         verified_in=counts['in'], verified_out=counts['out'],
                         confirmed_in=str(decimal_amount(totals['in'], decimals)) if successes['in'] else '',
                         confirmed_out=str(decimal_amount(totals['out'], decimals)) if successes['out'] else '',
                         net_amount=str(decimal_amount(net_raw, decimals)) if net_complete else '',
                         pending_identity=pending, unsettled=unsettled,
                         transit_packets=len(transit_amounts),
                         transit_amount=str(decimal_amount(sum(transit_amounts), decimals)) if transit_amounts else ''))
    write_csv(root / 'ibc_rwa_daily.csv', rows, FIELDS)
    return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--collect-routes', type=int, metavar='SECONDS')
    args = parser.parse_args()
    if args.collect_routes is None:
        export()
    else:
        collect_routes(args.collect_routes)
