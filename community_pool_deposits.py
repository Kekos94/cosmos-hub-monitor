"""Index explicit bank transfers to the Cosmos Hub community pool."""
import argparse
import base64
import csv
import hashlib
import io
import json
import re
import time

import hub_fee_monitor_v41 as rpc
from criterion1_daily import atomic_text
from criterion1_data import CHAIN, ROOT, utcnow
from criterion1_rpc_bulk import normalized_events
from criterion2_pipeline import Client

ADDRESS = 'cosmos1jv65s3grqf6v6jl3dp4t6c9t9rk99cd88lyufl'
FIRST_HEIGHT = 32785062
PROOF = ROOT / 'data/criterion1_community_pool.json'
CSV = 'hub_community_pool_deposits.csv'
MIGRATION = 'https://forum.cosmos.network/t/hub-weekly-update-23-november-5-2026/17540'
CLASSIFIED = {
    'C535CBC0D664697A89A85245906E1B19C15AA319AA0131FF9FC79856415487B2': 'migration_test',
    '5936893FC5133584F815A123CCA233CA51AD54BEE9448D721809765280F06016': 'migration_return',
}
INJECTIVE_USDC = 'ibc/E107DB3E8AABB49C87CB737A1ABBFDBA9803ECAE05E295D8D7A0B1A7FE83706C'
FIELDS = ('time_utc height tx_hash event_index sender denom amount_raw amount_display asset category evidence_url').split()


def transfers(tx):
    execution = tx['tx_result']
    if int(execution.get('code', 0)) != 0:
        return
    for index, event in enumerate(normalized_events(execution.get('events'))):
        if event['type'] != 'transfer':
            continue
        attrs = event['attributes']
        if not any(a == {'key': 'recipient', 'value': ADDRESS} for a in attrs):
            continue
        fields = {}
        for key in ('recipient', 'sender', 'amount'):
            values = [a['value'] for a in attrs if a['key'] == key]
            if len(values) != 1:
                raise ValueError('Ambiguous community-pool transfer')
            fields[key] = values[0]
        for coin in fields['amount'].split(','):
            match = re.fullmatch(r'([0-9]+)([a-zA-Z][a-zA-Z0-9/:._-]*)', coin)
            if not match:
                raise ValueError('Invalid community-pool transfer coin')
            yield index, fields['sender'], match[2], match[1]


def collect(client):
    status = client.rpc('status')
    if status['node_info']['network'] != CHAIN or status['sync_info']['catching_up']:
        raise ValueError('Community-pool index is on the wrong chain or catching up')
    end = int(status['sync_info']['latest_block_height'])
    query = f"transfer.recipient='{ADDRESS}' AND tx.height>={FIRST_HEIGHT} AND tx.height<={end}"
    size, page, total, seen, records, times = 100, 1, None, set(), [], {}
    while True:
        result = client.search(query, page, size)
        reported = int(result['total_count'])
        if total is None:
            total = reported
        txs = result['txs'] or []
        if reported != total or len(txs) != min(size, total - len(seen)):
            raise ValueError('Incomplete community-pool index page')
        for tx in txs:
            height = int(tx['height'])
            tx_hash = hashlib.sha256(base64.b64decode(tx['tx'], validate=True)).hexdigest().upper()
            if tx_hash != tx['hash'].upper() or tx_hash in seen or not FIRST_HEIGHT <= height <= end:
                raise ValueError('Wrong or duplicate community-pool index transaction')
            seen.add(tx_hash)
            for index, sender, denom, raw in transfers(tx):
                if height not in times:
                    times[height] = client.block(height)['time']
                category = CLASSIFIED.get(tx_hash, 'unclassified') if denom == INJECTIVE_USDC else 'unclassified'
                asset = {INJECTIVE_USDC: 'USDC.inj', 'uatom': 'ATOM'}.get(denom, denom)
                amount = f'{int(raw)//1000000}.{int(raw)%1000000:06d}' if asset != denom else raw
                records.append(dict(time_utc=times[height], height=height, tx_hash=tx_hash, event_index=index,
                                    sender=sender, denom=denom, amount_raw=raw, amount_display=amount,
                                    asset=asset, category=category, evidence_url=MIGRATION if category != 'unclassified' else ''))
        if len(seen) == total:
            return dict(source=client.source, from_height=FIRST_HEIGHT, through_height=end, observed_at=utcnow(),
                        records=records)
        page += 1


def save(new):
    if PROOF.exists():
        old = json.loads(PROOF.read_text())
        def key(row):
            return row['tx_hash'], row['event_index'], row['denom']
        indexed = {key(row): row for row in new['records']}
        if new['through_height'] < old['through_height'] or any(
                indexed.get(key(row)) != row for row in old['records']):
            raise ValueError('Community-pool index no longer reproduces earlier evidence')
    atomic_text(PROOF, json.dumps(new, ensure_ascii=False, indent=2) + '\n')


def export(root=ROOT):
    proof = root / PROOF.relative_to(ROOT)
    snapshot = json.loads(proof.read_text()) if proof.exists() else None
    output = io.StringIO()
    writer = csv.DictWriter(output, FIELDS, lineterminator='\n')
    writer.writeheader()
    if snapshot:
        writer.writerows(snapshot['records'])
    atomic_text(root / CSV, output.getvalue())
    return snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--collect', type=int, metavar='SECONDS')
    args = parser.parse_args()
    if args.collect is not None:
        if args.collect <= 0:
            parser.error('--collect must be positive')
        token = rpc.REQUEST_DEADLINE.set(time.monotonic() + args.collect)
        try:
            for source in rpc.RPCS:
                try:
                    save(collect(Client({'chain_id': CHAIN}, source)))
                    break
                except TimeoutError:
                    print('Community-pool collection reached its time budget.', flush=True)
                    break
                except (RuntimeError, ValueError, KeyError) as exc:
                    print(f'Community-pool index unavailable at {source}: {exc}', flush=True)
        finally:
            rpc.REQUEST_DEADLINE.reset(token)
    export()


if __name__ == '__main__':
    main()
