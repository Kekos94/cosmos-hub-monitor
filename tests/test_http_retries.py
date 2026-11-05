import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import requests
import hub_fee_monitor_v41 as monitor


class Clock:
    def __init__(self):
        self.now = 100.0
        self.waits = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds


def response(status=200, headers=None):
    r = Mock(status_code=status, headers=headers or {})
    r.json.return_value = {'tx_response': {'height': '123'}}
    if status >= 400:
        r.raise_for_status.side_effect = requests.HTTPError(f'HTTP {status}')
    return r


class HttpRetryTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        with monitor._rate_lock:
            monitor._next_request.clear()
        self.addCleanup(monitor._next_request.clear)
        for target, name, value in [(monitor.time, 'monotonic', self.clock.monotonic),
                                    (monitor.time, 'sleep', self.clock.sleep),
                                    (monitor, 'HTTP_MIN_INTERVAL', 1.0)]:
            p = patch.object(target, name, value)
            p.start()
            self.addCleanup(p.stop)

    def test_429_then_fallback_404_retries_original_provider_without_skipping_tx(self):
        seen = []
        answers = [response(429, {'Retry-After': '8'}), response(404), response()]
        def get(url, **kwargs):
            seen.append((url, self.clock.now))
            return answers.pop(0)
        with patch.object(monitor.requests, 'get', side_effect=get):
            data, source = monitor.get_any(['https://primary.test', 'https://fallback.test'], '/tx/hash')
        self.assertEqual(source, 'https://primary.test')
        self.assertEqual(data['tx_response']['height'], '123')
        self.assertEqual([url for url, _ in seen], ['https://primary.test/tx/hash', 'https://fallback.test/tx/hash', 'https://primary.test/tx/hash'])
        self.assertGreaterEqual(seen[-1][1] - seen[0][1], 8)

    def test_successful_fallback_does_not_wait_for_rate_limited_provider(self):
        with patch.object(monitor.requests, 'get', side_effect=[response(429, {'Retry-After': '30'}), response()]):
            _, source = monitor.get_any(['https://primary.test', 'https://fallback.test'], '/tx/hash')
        self.assertEqual(source, 'https://fallback.test')
        self.assertEqual(self.clock.now, 100)
        self.assertGreaterEqual(monitor._next_request['primary.test'], 130)

    def test_shared_cooldown_applies_to_another_request_and_worker(self):
        monitor.defer_provider('https://primary.test', 10)
        seen = []
        with patch.object(monitor.requests, 'get', side_effect=lambda *a, **k: seen.append(self.clock.now) or response()):
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(monitor.get_any, ['https://primary.test'], '/tx/other').result()
        self.assertGreaterEqual(seen[0], 110)

    def test_provider_spacing_is_shared_across_paths(self):
        seen = []
        with patch.object(monitor.requests, 'get', side_effect=lambda *a, **k: seen.append(self.clock.now) or response()):
            monitor.get_any(['https://primary.test'], '/one')
            monitor.get_any(['https://primary.test'], '/two')
        self.assertGreaterEqual(seen[1] - seen[0], 1)

    def test_retry_after_http_date_and_invalid_values(self):
        now = datetime(2026, 11, 5, 12, tzinfo=timezone.utc)
        r = response(429, {'Retry-After': 'Thu, 05 Nov 2026 12:00:12 GMT'})
        self.assertEqual(monitor.retry_delay(r, 0, now=now), 12)
        for invalid in ('NaN', 'inf', '-1', 'bad date'):
            self.assertEqual(monitor.retry_delay(response(429, {'Retry-After': invalid}), 2, now=now), 4)

    def test_permanent_404_does_not_retry_or_return_zero(self):
        with patch.object(monitor.requests, 'get', return_value=response(404)) as get:
            with self.assertRaisesRegex(RuntimeError, 'HTTP 404'):
                monitor.get_any(['https://primary.test'], '/tx/missing')
        self.assertEqual(get.call_count, 1)

    def test_transient_errors_have_bounded_retries(self):
        with patch.object(monitor.requests, 'get', return_value=response(503)) as get:
            with self.assertRaisesRegex(RuntimeError, 'attempt 4'):
                monitor.get_any(['https://primary.test'], '/tx/hash')
        self.assertEqual(get.call_count, monitor.HTTP_MAX_ATTEMPTS)
        self.assertEqual(self.clock.waits, [1.0, 2.0, 4.0])

    def test_connection_timeout_can_recover(self):
        with patch.object(monitor.requests, 'get', side_effect=[requests.Timeout('timeout'), response()]):
            data, _ = monitor.get_any(['https://primary.test'], '/tx/hash')
        self.assertEqual(data['tx_response']['height'], '123')
        self.assertEqual(self.clock.waits, [1.0])

    def test_retry_after_cannot_exceed_collection_budget(self):
        token = monitor.REQUEST_DEADLINE.set(105)
        self.addCleanup(monitor.REQUEST_DEADLINE.reset, token)
        with patch.object(monitor.requests, 'get', return_value=response(429, {'Retry-After': '30'})) as get:
            with self.assertRaises(TimeoutError):
                monitor.get_any(['https://primary.test'], '/tx/hash')
        self.assertEqual(get.call_count, 1)
        self.assertEqual(self.clock.waits, [])

    def test_rpc_post_batch_uses_the_same_retry_after_protection(self):
        ok = response()
        ok.json.return_value = [{'id': 'block:123', 'result': {}}]
        calls = [{'jsonrpc': '2.0', 'id': 'block:123', 'method': 'block', 'params': {'height': '123'}}]
        with patch.object(monitor.requests, 'post', side_effect=[response(429, {'Retry-After': '8'}), ok]) as post:
            data, _ = monitor.get_any(['https://primary.test'], '/', rpc_calls=calls)
        self.assertEqual(data[0]['id'], 'block:123')
        self.assertEqual(self.clock.waits, [8.0])
        for call in post.call_args_list:
            self.assertEqual(call.kwargs['json'], calls)

    def test_retry_preserves_requested_historical_height(self):
        ok = response(headers={'x-cosmos-block-height': '123'})
        with patch.object(monitor.requests, 'get', side_effect=[response(429), ok]) as get:
            monitor.get_any(['https://primary.test'], '/inflation', height=123)
        for call in get.call_args_list:
            self.assertEqual(call.kwargs['headers'], {'x-cosmos-block-height': '123'})


if __name__ == '__main__':
    unittest.main()
