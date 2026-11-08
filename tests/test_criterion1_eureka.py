import base64
import hashlib
import unittest
from unittest.mock import Mock

from criterion1_eureka import ETHEREUM_CLIENT, HUB_CLIENT, bounds, collect_day, summarize


def event(kind, **attrs):
    return dict(type=kind, attributes=[dict(key=k, value=v) for k, v in attrs.items()])


class EurekaFeesTest(unittest.TestCase):
    def test_route_deduplication_and_verified_fees(self):
        state = dict(chain_id='cosmoshub-4', date='2026-11-07', start_height=101, end_height=102,
                     previous_block={'height': 100}, last_block={'height': 102})
        encoded = base64.b64encode(b'signed tx').decode()
        tx_hash = hashlib.sha256(b'signed tx').hexdigest().upper()
        events = [
            event('send_packet', packet_source_client=HUB_CLIENT, packet_dest_client=ETHEREUM_CLIENT),
            event('recv_packet', packet_source_client=ETHEREUM_CLIENT, packet_dest_client=HUB_CLIENT),
            event('tx', fee='100uatom'),
        ]
        tx = dict(height='101', hash=tx_hash, tx=encoded, tx_result=dict(code=0, events=events))
        client = Mock(source='rpc')
        client.block.side_effect = [state['previous_block'], state['last_block']]
        hit = dict(total_count='1', txs=[tx])
        empty = dict(total_count='0', txs=[])
        client.search.side_effect = [hit, hit, empty, empty]
        proof = collect_day(state, client)
        self.assertEqual(proof['bounds'], bounds(state))
        self.assertEqual(len(proof['transactions']), 1)
        daily = dict(fee_collection_status='complete', start_height='101', end_height='102',
                     collected_fee_atom='1')
        row = summarize(state, proof, daily)
        self.assertEqual(row['collected_fee_atom'], '0.0001')
        self.assertEqual(row['share_of_hub_fees_pct'], '0.0100')
        proof['transactions'][0]['events'].pop()
        row = summarize(state, proof, daily)
        self.assertEqual(row['status'], 'missing_fee_evidence')
        self.assertEqual(row['collected_fee_atom'], '')
        self.assertEqual(row['share_of_hub_fees_pct'], '')
