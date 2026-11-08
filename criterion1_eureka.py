"""Attribute Hub transaction fees to the reviewed Ethereum Eureka route."""
import argparse
import base64
import csv
import hashlib
import io
import json
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import hub_fee_monitor_v41 as rpc
from criterion1_daily import DAILY_CSV, atomic_text, load_days, progress
from criterion1_data import CHAIN, ROOT, read_rows, utcnow
from criterion1_rpc_bulk import normalized_events
from criterion2_pipeline import Client

PROOFS = ROOT / 'data/criterion1_eureka_ethereum'
EXPORT = ROOT / 'hub_criterion1_eureka_daily.csv'
HUB_CLIENT = '08-wasm-1369'
ETHEREUM_CLIENT = 'cosmoshub-0'
PACKETS = {
    'recv_packet': ('packet_dest_client', 'packet_source_client'),
    'send_packet': ('packet_source_client', 'packet_dest_client'),
    'acknowledge_packet': ('packet_source_client', 'packet_dest_client'),
    'timeout_packet': ('packet_source_client', 'packet_dest_client'),
}
EVIDENCE_ATTRIBUTES = {
    **{kind: set(keys) for kind, keys in PACKETS.items()},
    'tx': {'fee'}, 'fee_pay': {'fee'}, 'tip_pay': {'tip'},
}
FIELDS = ('date start_height end_height status indexed_transactions transactions_without_fee_proof '
          'collected_fee_atom collected_tip_atom share_of_hub_fees_pct source observed_at').split()


def bounds(state):
    return {key: state[key] for key in ('date', 'chain_id', 'start_height', 'end_height',
                                        'previous_block', 'last_block')}


def matching_events(events, kind):
    local, _ = PACKETS[kind]
    matches = []
    for event in events:
        if event['type'] != kind:
            continue
        attrs = [(attr['key'], attr['value']) for attr in event['attributes']]
        if (local, HUB_CLIENT) in attrs:
            matches.append(event)
    return matches


def is_route(events, kind):
    _, remote = PACKETS[kind]
    return any([(a['key'], a['value']) for a in event['attributes'] if a['key'] == remote]
               == [(remote, ETHEREUM_CLIENT)] for event in matching_events(events, kind))


def evidence_events(events):
    return [dict(type=event['type'], attributes=[
        attr for attr in event['attributes'] if attr['key'] in EVIDENCE_ATTRIBUTES[event['type']]
    ]) for event in events if event['type'] in EVIDENCE_ATTRIBUTES]


def indexed_transactions(client, state, kind):
    local, _ = PACKETS[kind]
    first, last = state['start_height'], state['end_height']
    query = f"{kind}.{local}='{HUB_CLIENT}' AND tx.height>={first} AND tx.height<={last}"
    page, size, total, seen = 1, 50, None, set()
    while True:
        result = client.search(query, page, size)
        reported = int(result['total_count'])
        if total is None:
            total = reported
        txs = result['txs']
        if reported != total or total < 0 or len(txs) != min(size, total - len(seen)):
            raise ValueError('Incomplete or inconsistent Eureka index page')
        for tx in txs:
            height = int(tx['height'])
            tx_hash = hashlib.sha256(base64.b64decode(tx['tx'], validate=True)).hexdigest().upper()
            if not first <= height <= last or tx_hash != tx['hash'].upper() or tx_hash in seen:
                raise ValueError('Invalid or duplicate Eureka transaction')
            seen.add(tx_hash)
            execution = tx['tx_result']
            events = normalized_events(execution.get('events'))
            if not matching_events(events, kind):
                raise ValueError('Eureka index result lacks the requested event')
            if int(execution.get('code', 0)) == 0 and is_route(events, kind):
                yield dict(hash=tx_hash, height=height,
                           events=evidence_events(events))
        if len(seen) == total:
            return
        page += 1


def collect_day(state, client):
    for key in ('previous_block', 'last_block'):
        block = state[key]
        if client.block(block['height']) != block:
            raise ValueError('Eureka index provider differs from the audited Hub boundaries')
    rows = {}
    for kind in PACKETS:
        for tx in indexed_transactions(client, state, kind):
            existing = rows.setdefault(tx['hash'], tx)
            if existing != tx:
                raise ValueError('Conflicting evidence for the same Eureka transaction')
    return dict(bounds=bounds(state), transactions=list(rows.values()), source=client.source, observed_at=utcnow())


def summarize(state, proof, daily):
    row = dict.fromkeys(FIELDS, '')
    row.update(date=state['date'], start_height=state['start_height'], end_height=state['end_height'], status='pending')
    if proof is None:
        return row
    if proof['bounds'] != bounds(state):
        raise ValueError('Eureka proof does not match audited day')
    hashes = set()
    collected = tips = missing = 0
    for tx in proof['transactions']:
        if (tx['hash'] in hashes or not state['start_height'] <= tx['height'] <= state['end_height']
                or not any(is_route(tx['events'], kind) for kind in PACKETS)):
            raise ValueError('Invalid Eureka evidence')
        hashes.add(tx['hash'])
        fee, tip = rpc.collected_fee_components({'tx_response': {'code': 0, 'events': tx['events']}})
        if fee is None:
            missing += 1
        else:
            collected += fee
            tips += tip
    row.update(indexed_transactions=len(hashes), transactions_without_fee_proof=missing,
               status='missing_fee_evidence' if missing else 'indexed_fee_verified',
               source=proof['source'], observed_at=proof['observed_at'])
    if not missing:
        fee_atom = Decimal(collected) / 1_000_000
        row.update(collected_fee_atom=str(fee_atom), collected_tip_atom=str(Decimal(tips) / 1_000_000))
        if (daily and daily['fee_collection_status'] == 'complete' and
                (int(daily['start_height']), int(daily['end_height'])) == (state['start_height'], state['end_height'])):
            total = Decimal(daily['collected_fee_atom'])
            if fee_atom > total:
                raise ValueError('Eureka fees exceed audited Hub fees')
            if total > 0:
                row['share_of_hub_fees_pct'] = str(100 * fee_atom / total)
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--collect', type=int, metavar='SECONDS')
    parser.add_argument('--day', type=date.fromisoformat)
    args = parser.parse_args()
    states = [s for s in load_days() if progress(s)['status'] == 'complete']
    deadline = time.monotonic() + args.collect if args.collect is not None else None
    token = rpc.REQUEST_DEADLINE.set(deadline)
    try:
        if deadline is not None:
            for state in reversed(states):
                path = PROOFS / (state['date'] + '.json')
                if path.exists() or (args.day and str(args.day) != state['date']):
                    continue
                for source in rpc.RPCS:
                    try:
                        proof = collect_day(state, Client({'chain_id': CHAIN}, source))
                        atomic_text(path, json.dumps(proof, separators=(',', ':')) + '\n')
                        print(f'{state["date"]}: {len(proof["transactions"])} indexed Eureka transactions', flush=True)
                        break
                    except TimeoutError:
                        raise
                    except (RuntimeError, ValueError, KeyError) as exc:
                        print(f'{state["date"]}: Eureka index unavailable at {source}: {exc}', flush=True)
    except TimeoutError:
        print('Eureka collection budget reached; verified days retained.', flush=True)
    finally:
        rpc.REQUEST_DEADLINE.reset(token)
        daily = {row['date']: row for row in read_rows(DAILY_CSV)}
        output = io.StringIO()
        writer = csv.DictWriter(output, FIELDS, lineterminator='\n')
        writer.writeheader()
        for state in states:
            path = PROOFS / (state['date'] + '.json')
            proof = json.loads(path.read_text()) if path.exists() else None
            writer.writerow(summarize(state, proof, daily.get(state['date'])))
        atomic_text(EXPORT, output.getvalue())


if __name__ == '__main__':
    main()
