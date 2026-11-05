"""Add fee evidence without rewriting immutable block/snapshot checkpoints."""
import argparse
import gzip
import os
import tempfile
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from criterion1_data import ROOT, enrich, number, timestamp, utcnow
import hub_fee_monitor_v41 as monitor
from criterion1_rpc_bulk import payloads

AUDIT_DIR = ROOT / 'data' / 'criterion1_fee_audits'
RELEVANT = {'tx', 'fee_pay', 'tip_pay', 'coin_spent', 'coin_received', 'transfer', 'message'}


def read_audit(path):
    path = Path(path)
    data = gzip.decompress(path.read_bytes()) if path.suffix == '.gz' else path.read_bytes()
    return json.loads(data)


def compress_audit(path):
    path = Path(path)
    data = path.read_bytes()
    if not json.loads(data)['complete']:
        raise ValueError('Cannot finalize a partial fee audit')
    compressed = gzip.compress(data, mtime=0)
    if gzip.decompress(compressed) != data:
        raise ValueError('Audit compression verification failed')
    target = path.with_suffix('.json.gz')
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(compressed)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)
        if gzip.decompress(target.read_bytes()) != data:
            raise ValueError('Stored audit compression verification failed')
        path.unlink()  # The generated JSON is preserved byte-for-byte inside gzip.
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return target


def state_hash(state):
    return hashlib.sha256(json.dumps(state, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def audited_chunk(original, record):
    first, end = int(original['start_height']), int(original['end_height'])
    if (record['start_height'], record['end_height']) != (first, end):
        raise ValueError('Audit chunk bounds mismatch')
    blocks = record['blocks']
    if [b['height'] for b in blocks] != list(range(first, end+1)):
        raise ValueError('Audit must cover every block in affected chunk')
    if timestamp(blocks[0]['time']) != timestamp(original['first_block_time']) or timestamp(blocks[-1]['time']) != timestamp(original['end_time']):
        raise ValueError('Audit timestamps mismatch')
    ordered = [(b['height'], index, tx_hash) for b in blocks for index, tx_hash in enumerate(b['tx_hashes'])]
    receipts = record['transactions']
    if [(r['height'], r['index'], r['hash']) for r in receipts] != ordered:
        raise ValueError('Audit transaction order/count mismatch')
    if len(ordered) != int(original['tx_total']) or len(set(ordered)) != len(ordered):
        raise ValueError('Audit transaction coverage mismatch')
    declared = declared_ibc = actual = actual_ibc = tips = tips_ibc = ibc_count = missing = recovered = initially_unknown = 0
    for receipt in receipts:
        tx = receipt['tx']
        response = tx['tx_response']
        if str(response['height']) != str(receipt['height']) or response['txhash'] != receipt['hash']:
            raise ValueError('Audit transaction identity mismatch')
        ibc = any(m['@type'].startswith('/ibc.') for m in tx['tx']['body']['messages'])
        fee = monitor.coin_uatom(tx['tx']['auth_info']['fee']['amount'])
        declared += fee
        declared_ibc += fee if ibc else 0
        ibc_count += int(ibc)
        paid, tip = monitor.collected_fee_components(tx)
        if paid is None:
            initially_unknown += 1
            proof = receipt.get('collector')
            if proof:
                paid, tip = monitor.collected_fee_components(tx, collector=proof)
                recovered += int(paid is not None)
        if paid is None:
            missing += 1
        else:
            actual += paid
            actual_ibc += paid if ibc else 0
            tips += tip
            tips_ibc += tip if ibc else 0
    if (number(original['declared_fee_atom']) != Decimal(declared)/1000000 or
        number(original['declared_ibc_fee_atom']) != Decimal(declared_ibc)/1000000 or
        int(original['tx_ibc']) != ibc_count or initially_unknown != int(original['tx_without_fee_event']) + int(original.get('fee_ante_transfer_count', 0))):
        raise ValueError('Audit disagrees with original transaction measurements')
    row = dict(original, tx_without_fee_event=str(missing), fee_ante_transfer_count=str(recovered),
               fee_collection_status='incomplete' if missing else 'complete')
    if not missing:
        row.update(collected_fee_atom=str(Decimal(actual)/1000000), collected_ibc_fee_atom=str(Decimal(actual_ibc)/1000000),
                   collected_tip_atom=str(Decimal(tips)/1000000), collected_ibc_tip_atom=str(Decimal(tips_ibc)/1000000))
    return enrich(row), recovered - int(original.get("fee_ante_transfer_count", 0))


def apply_fee_audit(state, audit, *, require_complete=True):
    if audit['schema_version'] != 1 or audit['date'] != state['date'] or audit['state_sha256'] != state_hash(state):
        raise ValueError('Fee audit does not match immutable source checkpoint')
    affected = [c for c in state['chunks'] if int(c['tx_without_fee_event'])]
    if require_complete and (not audit['complete'] or len(audit['chunks']) != len(affected)):
        raise ValueError('Fee audit is not complete')
    if len(audit['chunks']) > len(affected):
        raise ValueError('Unexpected fee audit chunks')
    replacements, recovered = {}, 0
    for original, record in zip(affected, audit['chunks']):
        updated, n = audited_chunk(original, record)
        replacements[original['start_height']] = updated
        recovered += n
    result = dict(state, chunks=[replacements.get(c['start_height'], c) for c in state['chunks']])
    return result, recovered


def load_fee_audit(state, directory=AUDIT_DIR):
    path = Path(directory) / (state['date'] + '.json')
    if path.with_suffix('.json.gz').exists():
        path = path.with_suffix('.json.gz')
    if not path.exists():
        return None
    audit = read_audit(path)
    apply_fee_audit(state, audit, require_complete=False)
    return audit if audit['complete'] else None


def audit_day(day):
    from criterion1_daily import DAY_DIR, atomic_text, progress, validate_day
    state = json.loads((DAY_DIR / (day + '.json')).read_text())
    validate_day(state)
    if progress(state)['status'] != 'complete':
        raise ValueError('Finish UTC-day block coverage before auditing historical fees')
    path = AUDIT_DIR / (day + '.json')
    existing = path.with_suffix('.json.gz') if path.with_suffix('.json.gz').exists() else path
    audit = read_audit(existing) if existing.exists() else dict(
        schema_version=1, date=day, state_sha256=state_hash(state), observed_at=utcnow(), complete=False, chunks=[])
    apply_fee_audit(state, audit, require_complete=False)
    if audit['complete']:
        apply_fee_audit(state, audit)
        if existing.suffix != '.gz':
            compress_audit(existing)
        print('Existing complete fee audit verified; no network requests needed.', flush=True)
        return
    affected = [c for c in state['chunks'] if int(c['tx_without_fee_event'])]
    for chunk in affected[len(audit['chunks']):]:
        first, end = int(chunk['start_height']), int(chunk['end_height'])
        record = dict(start_height=first, end_height=end, blocks=[], transactions=[])
        for height, block, source, txs in payloads(monitor, first, end):
            hashes = []
            for index, (tx, _) in enumerate(txs):
                response = tx['tx_response']
                # Keep all fee/message-scope evidence, omit unrelated packet payloads/signatures.
                response = dict(response, events=[dict(e, attributes=[a for a in e.get('attributes', []) if a['key'] != 'signature'])
                               for e in response['events'] if e['type'] in RELEVANT])
                tx = dict(tx, tx_response=response)
                proof = None
                paid, _ = monitor.collected_fee_components(tx)
                if paid is None and int(response.get('code', 0)):
                    proof = monitor.collector_proof(height)
                hashes.append(response['txhash'])
                record['transactions'].append(dict(height=height, index=index, hash=response['txhash'], tx=tx, collector=proof, source=source))
            record['blocks'].append(dict(height=height, time=block['header']['time'], tx_hashes=hashes))
        updated, n = audited_chunk(chunk, record)
        audit['chunks'].append(record)
        atomic_text(path, json.dumps(audit, sort_keys=True, separators=(',', ':')) + '\n')
        print(f'Fee audit {len(audit["chunks"])}/{len(affected)} chunks; {first}..{end}: recovered {n}, still unknown {updated["tx_without_fee_event"]}', flush=True)
    audit['complete'] = True
    audited, recovered = apply_fee_audit(state, audit)
    atomic_text(path, json.dumps(audit, sort_keys=True, separators=(',', ':')) + '\n')
    compress_audit(path)
    print(f'Fee audit complete: recovered {recovered}; remaining unknown {sum(int(c["tx_without_fee_event"]) for c in audited["chunks"])}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('day')
    audit_day(parser.parse_args().day)
