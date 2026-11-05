import base64
import hashlib
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

import criterion1_rpc_bulk as bulk
from cosmos_tx_decode import decode_tx
from hub_fee_monitor_v41 import collected_fee_components
from criterion1_data import CHAIN


def vi(n):
    result = bytearray()
    while n > 127:
        result.append((n & 127) | 128)
        n >>= 7
    result.append(n)
    return bytes(result)


def field(n, data):
    return vi(n*8+2) + vi(len(data)) + data


def tx_raw(amount='1890', kind='/ibc.core.client.v1.MsgUpdateClient'):
    message = field(1, kind.encode()) + field(2, b'payload')
    body = field(1, message) + field(2, b'memo')
    coin = field(1, b'uatom') + field(2, amount.encode())
    fee = field(1, coin) + vi(2*8) + vi(12345)
    auth = field(2, fee)
    return field(1, body) + field(2, auth) + field(3, b'signature')


def entry(height, count=0):
    return {'header': {'chain_id': CHAIN, 'height': str(height),
                       'time': f'2026-11-04T00:00:{height:02d}Z',
                       'data_hash': '' if count == 0 else 'COMMITMENT'},
            'num_txs': str(count), 'block_id': {'hash': f'HASH{height}'}}


def results_for(meta):
    n = int(meta['num_txs'])
    encoded = base64.b64encode(tx_raw()).decode()
    block = {'block_id': meta['block_id'], 'block': {'header': meta['header'], 'data': {'txs': [encoded]*n}}}
    executions = {'height': meta['header']['height'], 'txs_results': [
        {'code': 5, 'events': [{'type': 'fee_pay', 'attributes': [{'key': 'ZmVl', 'value': base64.b64encode(b'1422uatom').decode()}]},
                               {'type': 'tip_pay', 'attributes': [{'key': 'dGlw', 'value': base64.b64encode(b'468uatom').decode()}]}]}
        for _ in range(n)]}
    return block, executions


class DecodeTests(unittest.TestCase):
    def test_local_decode_preserves_message_urls_and_large_coin_amounts(self):
        tx = decode_tx(tx_raw(amount='9007199254740993'))
        self.assertEqual(tx['body']['messages'][0]['@type'], '/ibc.core.client.v1.MsgUpdateClient')
        self.assertEqual(tx['auth_info']['fee']['amount'], [{'denom': 'uatom', 'amount': '9007199254740993'}])

    def test_malformed_truncated_and_duplicate_fields_fail(self):
        for raw in [b'\x0a\xff', b'\x80'*11, b'\x00', tx_raw()[:-1], tx_raw()+field(1, b'other'), tx_raw(amount='-1')]:
            with self.subTest(raw=raw[:8]), self.assertRaises(ValueError):
                decode_tx(raw)

    def test_fee_events_plain_and_base64_are_equivalent(self):
        meta = entry(11, 1)
        block, executions = results_for(meta)
        _, txs = bulk.transactions(11, meta, block, executions)
        self.assertEqual(collected_fee_components(txs[0][0]), (1890, 468))
        self.assertEqual(txs[0][0]['tx_response']['code'], 5)
        expected = hashlib.sha256(tx_raw()).hexdigest().upper()
        self.assertEqual(txs[0][0]['tx_response']['txhash'], expected)
        plain = [{'type': 'fee_pay', 'attributes': [{'key': 'fee', 'value': '1422uatom'}]}]
        self.assertEqual(bulk.normalized_events(plain), plain)

    def test_missing_execution_events_remain_unknown(self):
        meta = entry(11, 1)
        block, executions = results_for(meta)
        executions['txs_results'][0]['events'] = []
        _, txs = bulk.transactions(11, meta, block, executions)
        self.assertEqual(collected_fee_components(txs[0][0]), (None, None))

    def test_truncated_or_wrong_block_results_never_become_zero(self):
        for change in ('missing_result', 'wrong_height', 'wrong_header', 'wrong_hash', 'wrong_count'):
            meta = entry(11, 1)
            block, executions = results_for(meta)
            if change == 'missing_result': executions['txs_results'] = []
            if change == 'wrong_height': executions['height'] = '12'
            if change == 'wrong_header': block['block']['header'] = dict(meta['header'], chain_id='wrong')
            if change == 'wrong_hash': block['block_id'] = {'hash': 'WRONG'}
            if change == 'wrong_count': block['block']['data']['txs'] = []
            with self.subTest(change=change), self.assertRaises(ValueError):
                bulk.transactions(11, meta, block, executions)


class BatchTests(unittest.TestCase):
    def test_metadata_requires_every_height_exactly_once(self):
        good = [entry(11), entry(12)]
        for entries in [good[:1], good+[entry(11)], [entry(10), entry(12)]]:
            api = SimpleNamespace(RPCS=['rpc'], get_any=Mock(return_value=({'result': {'block_metas': entries}}, 'rpc')))
            with self.assertRaises(ValueError):
                bulk.metadata(api, 11, 12)

    def test_batch_results_are_matched_by_id_not_response_order(self):
        calls = [{'id': 'b:11'}, {'id': 'r:11'}]
        answer = [{'id': 'r:11', 'result': {'kind': 'result'}}, {'id': 'b:11', 'result': {'kind': 'block'}}]
        api = SimpleNamespace(RPCS=['rpc'], get_any=Mock(return_value=(answer, 'rpc')))
        values, _ = bulk.rpc_results(api, calls, 'rpc')
        self.assertEqual(values['b:11']['kind'], 'block')
        for bad in [answer[:1], answer+[answer[0]], answer+[{'id': 'unexpected', 'result': {}}]]:
            api.get_any.return_value = bad, 'rpc'
            with self.assertRaises(ValueError):
                bulk.rpc_results(api, calls, 'rpc')

    def test_empty_blocks_need_one_metadata_request_and_no_tx_or_result_reads(self):
        api = SimpleNamespace(RPCS=['rpc'], get_any=Mock(return_value=({'result': {'block_metas': [entry(11), entry(12)]}}, 'rpc')))
        payloads = list(bulk.payloads(api, 11, 12))
        self.assertEqual(len(payloads), 2)
        self.assertTrue(all(p[3] == [] for p in payloads))
        self.assertEqual(api.get_any.call_count, 1)

    def test_empty_block_with_nonempty_commitment_is_rejected(self):
        meta = entry(11)
        meta['header']['data_hash'] = 'NOT_EMPTY'
        api = SimpleNamespace(RPCS=['rpc'], get_any=Mock(return_value=({'result': {'block_metas': [meta]}}, 'rpc')))
        with self.assertRaises(ValueError):
            list(bulk.payloads(api, 11, 11))

    def test_busy_blocks_use_bounded_batches_no_per_transaction_lcd(self):
        entries = {h: entry(h, 3) for h in range(11, 18)}
        calls_seen = []
        def get(bases, path, **kwargs):
            if path == '/blockchain':
                return {'result': {'block_metas': list(entries.values())}}, 'rpc'
            calls = kwargs['rpc_calls']
            calls_seen.append(calls)
            answer = []
            for c in reversed(calls):
                method, h = c['id'].split(':')
                block, executions = results_for(entries[int(h)])
                answer.append({'id': c['id'], 'result': block if method == 'block' else executions})
            return answer, 'rpc'
        api = SimpleNamespace(RPCS=['rpc'], get_any=Mock(side_effect=get))
        payloads = list(bulk.payloads(api, 11, 17))
        self.assertEqual([p[0] for p in payloads], list(range(11, 18)))
        self.assertEqual(sum(len(p[3]) for p in payloads), 21)
        self.assertEqual([len(c) for c in calls_seen], [10, 4])
        self.assertEqual(api.get_any.call_count, 3)

    def test_unavailable_bulk_uses_existing_checked_fallback(self):
        api = SimpleNamespace(RPCS=['rpc'], get_any=Mock(side_effect=RuntimeError('unsupported')),
                              block_payload=Mock(side_effect=lambda h, **k: (h, 'checked')))
        self.assertEqual(list(bulk.payloads(api, 11, 12)), [(11, 'checked'), (12, 'checked')])
        self.assertEqual(api.block_payload.call_count, 2)


if __name__ == '__main__':
    unittest.main()
