"""Bounded RPC batches; never infer activity from a sampled block range."""
import base64
import hashlib

from criterion1_data import CHAIN, timestamp
from cosmos_tx_decode import decode_tx

# CometBFT /blockchain returns at most 20 metadata entries.
METADATA_BATCH = 20
# Small JSON-RPC request batches, even on nodes that impose a batch-size limit.
RPC_BATCH_CALLS = 10


def metadata(api, start, end):
    data, source = api.get_any(api.RPCS, '/blockchain', params={'minHeight': start, 'maxHeight': end})
    entries = data['result']['block_metas']
    by_height = {}
    for entry in entries:
        h = entry['header']
        height = int(h['height'])
        if h['chain_id'] != CHAIN or height in by_height or not start <= height <= end:
            raise ValueError('Wrong chain, duplicate or unexpected block metadata')
        timestamp(h['time'])
        if int(entry['num_txs']) < 0:
            raise ValueError('Negative metadata transaction count')
        by_height[height] = entry
    if set(by_height) != set(range(start, end+1)):
        raise ValueError('Truncated block metadata range')
    return by_height, source


def normalized_events(events):
    """Older Tendermint ABCI attributes are base64; REST uses plain strings."""
    normalized = []
    for event in events or []:
        attrs = []
        for attr in event.get('attributes', []):
            key, value = attr['key'], attr['value']
            if key not in ('fee', 'tip'):
                try:
                    decoded = base64.b64decode(key, validate=True).decode('utf-8')
                except (ValueError, UnicodeError):
                    decoded = None
                if decoded in ('fee', 'tip'):
                    key = decoded
                    value = base64.b64decode(value, validate=True).decode('utf-8')
            attrs.append({'key': key, 'value': value})
        normalized.append({'type': event['type'], 'attributes': attrs})
    return normalized


def rpc_results(api, calls, preferred):
    bases = [preferred] + [base for base in api.RPCS if base != preferred]
    values, source = api.get_any(bases, '/', rpc_calls=calls)
    expected = {call['id'] for call in calls}
    by_id = {}
    for item in values:
        ident = item.get('id')
        if ident not in expected or ident in by_id:
            raise ValueError('Unexpected or duplicate JSON-RPC batch result')
        if item.get('error'):
            raise RuntimeError(f'RPC batch unavailable: {item["error"]}')
        if not isinstance(item.get('result'), dict):
            raise ValueError('Missing JSON-RPC batch result')
        by_id[ident] = item['result']
    if set(by_id) != expected:
        raise ValueError('Truncated JSON-RPC batch response')
    return by_id, source


def transactions(height, entry, block_result, execution_result):
    b = block_result['block']
    if b['header'] != entry['header'] or block_result['block_id']['hash'] != entry['block_id']['hash']:
        raise ValueError('Block data does not match requested metadata')
    if int(execution_result['height']) != height:
        raise ValueError('Execution results have wrong height')
    encoded = b['data'].get('txs') or []
    results = execution_result.get('txs_results') or []
    if len(encoded) != int(entry['num_txs']) or len(results) != len(encoded):
        raise ValueError('Missing transactions or execution results')
    decoded = []
    for raw_b64, result in zip(encoded, results):
        raw = base64.b64decode(raw_b64, validate=True)
        tx_hash = hashlib.sha256(raw).hexdigest().upper()
        tx = {'tx': decode_tx(raw), 'tx_response': {'height': str(height), 'txhash': tx_hash,
              'code': result.get('code', 0), 'events': normalized_events(result.get('events', []))}}
        # No LCD call: the result corresponds to this transaction's index in the block.
        decoded.append((tx, ''))
    return b, decoded


def payloads(api, start, end, *, deadline=None):
    for first in range(start, end+1, METADATA_BATCH):
        last = min(end, first+METADATA_BATCH-1)
        try:
            entries, meta_source = metadata(api, first, last)
        except RuntimeError as exc:
            print(f'RPC metadata unavailable; using checked individual reads: {exc}', flush=True)
            for height in range(first, last+1):
                yield api.block_payload(height, deadline=deadline)
            continue
        occupied = [h for h in sorted(entries) if int(entries[h]['num_txs']) > 0]
        calls = [{'jsonrpc': '2.0', 'id': f'{method}:{h}', 'method': method, 'params': {'height': str(h)}}
                 for h in occupied for method in ('block', 'block_results')]
        values, sources = {}, {meta_source}
        try:
            for offset in range(0, len(calls), RPC_BATCH_CALLS):
                results, source = rpc_results(api, calls[offset:offset+RPC_BATCH_CALLS], meta_source)
                values.update(results)
                sources.add(source)
        except RuntimeError as exc:
            print(f'RPC batch unavailable; using checked individual reads: {exc}', flush=True)
            for height in range(first, last+1):
                yield api.block_payload(height, deadline=deadline)
            continue
        for height in range(first, last+1):
            entry = entries[height]
            if int(entry['num_txs']) == 0:
                # Require an explicitly empty commitment, not just a missing tx array.
                if entry['header']['data_hash'] not in ('', hashlib.sha256(b'').hexdigest().upper()):
                    raise ValueError('Empty block metadata has nonempty transaction commitment')
                b, txs = {'header': entry['header'], 'data': {'txs': []}}, []
            else:
                b, txs = transactions(height, entry, values[f'block:{height}'], values[f'block_results:{height}'])
            yield height, b, ';'.join(sorted(sources)), txs
