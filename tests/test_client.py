import asyncio
import copy
import json
import io
import heapq
import re
import socket
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from mailman_tasks import *


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.handle_api()

    def do_GET(self):
        self.handle_api()

    def handle_api(self):
        state = self.server.state
        path = urlsplit(self.path).path
        kind = 'create' if path.endswith('/create') else 'poll' if path.endswith('/completed') else 'verify'
        body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))) or b'{}')
        with state['lock']:
            state['calls'].append((kind, self.path, body, dict(self.headers), time.perf_counter()))
            response = state[kind].pop(0) if state[kind] else None
            if response is None:
                if kind == 'create':
                    task_id = str(len(state['calls']))
                    if state['auto']:
                        state['results'].setdefault(body['group'], []).append({'id': task_id, 'status': state['status'], 'results': state['details']})
                    response = (200, {'id': task_id}, {})
                elif kind == 'poll':
                    group = parse_qs(urlsplit(self.path).query, keep_blank_values=True)['group'][0]
                    results = state['results'].setdefault(group, [])
                    page = results[:100]
                    del results[:100]
                    response = (200, {'tasks': page}, {})
                else:
                    response = (200, {}, {})
        if isinstance(response, tuple) and response[0] == 'hold':
            _, entered, release, payload = response
            entered.set()
            release.wait(2)
            response = (200, payload, {})
        if isinstance(response, tuple) and response[0] == 'trickle':
            phase = response[1]
            try:
                if phase == 'headers':
                    self.wfile.write(b'HTTP/1.0 200 OK\r\nX-Trickle: ')
                    for _ in range(20):
                        self.wfile.write(b'x')
                        self.wfile.flush()
                        time.sleep(.02)
                    self.wfile.write(b'\r\nContent-Length: 2\r\n\r\n{}')
                else:
                    self.send_response(200)
                    self.send_header('Content-Length', '20')
                    self.end_headers()
                    for _ in range(20):
                        self.wfile.write(b' ')
                        self.wfile.flush()
                        time.sleep(.02)
            except OSError:
                pass
            return
        if response == 'drop':
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        if response == 'truncated503':
            self.send_response(503)
            self.send_header('Content-Length', '100')
            self.send_header('Retry-After', '.01')
            self.end_headers()
            self.wfile.write(b'{')
            self.close_connection = True
            return
        code, data, headers = response
        encoded = json.dumps(data).encode()
        try:
            self.send_response(code)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        except OSError:
            # close() and the total HTTP timeout deliberately shut the socket.
            pass


def task():
    return code_task(email='test@example.com', from_='sender@example.com', subject_includes='code')


class Tests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.state = dict(lock=threading.Lock(), calls=[], create=[], poll=[], verify=[], results={}, auto=True, status='success', details={'code': '123456'})
        self.server.state = self.state
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        self.thread.start()
        self.client = MailmanTasks('secret-key', group='bot & / +', base_url='http://127.0.0.1:' + str(self.server.server_port))
        self.client._poll_interval = .02
        self.client._full_page_interval = .005
        self.client._auth_interval = .01
        self.client._backoff = lambda attempt: .005

    def tearDown(self):
        self.client.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def count(self, kind):
        return sum(c[0] == kind for c in self.state['calls'])

    def test_defaults_headers_encoding_and_destructive_poll(self):
        self.assertTrue(self.client.verify())
        task_id = self.client.create_task(task())
        sent = self.state['calls'][-1]
        self.assertEqual(sent[2]['timeout'], 300)
        self.assertLess(abs(sent[2]['receivedAt'] - time.time()), 2)
        self.assertEqual(sent[3]['Authorization'], 'Token secret-key')
        self.assertEqual(sent[3]['User-Agent'], 'mailman-tasks-python/0.1.0')
        self.assertEqual(self.client.poll()[0]['id'], task_id)
        self.assertEqual(self.client.poll(), [])
        self.assertIn('group=bot+%26+%2F+%2B', self.state['calls'][-1][1])

    def test_validation_before_transport(self):
        mutations = [lambda t: t.update(email=' '), lambda t: t.update(email='x'*1025),
                     lambda t: t.update(receivedAt=0), lambda t: t.update(receivedAt=1.5),
                     lambda t: t.update(timeout=-1), lambda t: t.update(mailFilters=[]),
                     lambda t: t.update(mailElements=t['mailElements']*11),
                     lambda t: t['mailFilters'][0].update(orValues=['a']*11),
                     lambda t: t['mailFilters'][0].update(orValues=[None]),
                     lambda t: t['mailFilters'][0].update(optional=1),
                     lambda t: t['mailFilters'][0].update(comparator='contains'),
                     lambda t: t['mailElements'][0].update(regexSource='bad'),
                     lambda t: t['mailElements'][0].update(formatter='bad'),
                     lambda t: t['mailElements'][0].update(regex=''),
                     lambda t: t['mailElements'][0].update(name='')]
        for mutate in mutations:
            t = task()
            mutate(t)
            with self.subTest(t=t), self.assertRaises(InvalidRequestError):
                self.client.create_task(t)
        self.assertEqual(self.state['calls'], [])
        for timeout in (0, 4000):
            t = task()
            t['timeout'] = timeout
            self.client.create_task(t)
            self.assertEqual(self.state['calls'][-1][2]['timeout'], 3600)

    def test_error_mapping_and_redaction(self):
        for code, message, cls in [(400, 'bad secret-key', InvalidRequestError), (413, 'large', PayloadTooLargeError),
                                   (429, 'pending task limit exceeded', PendingLimitError), (401, 'bad key', AuthError),
                                   (408, '', RequestTimeoutError)]:
            self.state['create'] = [(code, {'error': message}, {})]
            before = self.count('create')
            with self.assertRaises(cls) as caught:
                self.client.create_task(task())
            self.assertEqual(caught.exception.status, code)
            self.assertNotIn('secret-key', str(caught.exception))
            self.assertEqual(self.count('create'), before + 1)


    def test_create_retries_http_but_not_dropped_connection(self):
        self.state['create'] = [(429, {'error': 'rate limit exceeded'}, {'Retry-After': '.01'}),
                                (503, {'error': 'server busy'}, {'Retry-After': '.04'})]
        self.client.create_task(task())
        self.assertEqual(self.count('create'), 3)
        times = [c[-1] for c in self.state['calls']]
        self.assertGreaterEqual(times[2]-times[1], .025)
        self.state['create'] = ['drop']
        with self.assertRaises(MailmanError):
            self.client.create_task(task())
        self.assertEqual(self.count('create'), 4)

    def test_retry_exhaustion(self):
        self.state['create'] = [(503, {'error': 'server busy'}, {'Retry-After': '0'})]*3
        with self.assertRaises(ServerBusyError) as caught:
            self.client.create_task(task())
        self.assertEqual(caught.exception.retry_after, 0)
        self.assertEqual(self.count('create'), 3)

    def test_poll_recovery(self):
        self.state['poll'] = ['drop', (401, {'error': 'invalid user API key'}, {}),
                              (429, {'error': 'rate limit exceeded'}, {'Retry-After': '.01'}),
                              (503, {'error': 'busy'}, {'Retry-After': '.01'}),
                              (408, {}, {})]
        self.assertEqual(self.client.run(task(), timeout=2)['results']['code'], '123456')
        self.assertEqual(self.count('poll'), 6)

    def test_pages_and_result_before_wait(self):
        group = self.client.group
        self.state['results'][group] = [{'id': str(i), 'status': 'success', 'results': {}} for i in range(101)]
        self.assertEqual(self.client.wait('100', timeout=1)['id'], '100')
        self.assertEqual(self.count('poll'), 2)
        self.assertEqual(self.client.wait('0', timeout=.1)['id'], '0')
        self.assertEqual(self.count('poll'), 2)
        self.assertEqual(self.client.poll(), [])

    def test_two_concurrent_waits_one_poller(self):
        ids = [self.client.create_task(task()) for _ in range(2)]
        barrier = threading.Barrier(3)
        def wait(i):
            barrier.wait()
            return self.client.wait(i, timeout=1)
        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(wait, i) for i in ids]
            barrier.wait()
            self.assertEqual({f.result()['id'] for f in futures}, set(ids))
        self.assertEqual(self.count('poll'), 1)

    def test_deadline_and_outcomes(self):
        with self.assertRaises(TaskTimeoutError):
            self.client.wait('lost', deadline=time.time()+.03)
        self.state['status'] = 'error'
        self.state['details'] = {'errorStatus': 'invalid mail task', 'errorMessage': 'bad regex'}
        with self.assertRaises(TaskFailedError) as caught:
            self.client.run(task(), timeout=1)
        self.assertEqual(caught.exception.errorStatus, 'invalid mail task')
        self.assertEqual(caught.exception.error_message, 'bad regex')
        self.state['status'] = 'timeout'
        with self.assertRaises(TaskTimeoutError):
            self.client.run(task(), timeout=1)

    def test_buffer_bound_ttl_and_group_override(self):
        self.client._buffer_limit = 2
        self.state['results'][self.client.group] = [{'id': str(i), 'status': 'success'} for i in range(4)]
        self.client.wait('3', timeout=1)
        self.assertLessEqual(len(self.client._buffer), 2)
        self.assertIn((self.client.group, "2"), self.client._buffer)
        self.client._buffer_ttl = 0
        with self.assertRaises(TaskTimeoutError):
            self.client.wait('2', timeout=.03)
        t = task()
        self.client._buffer_ttl = 3600
        t['group'] = 'other'
        self.assertEqual(self.client.run(t, timeout=1)['status'], 'success')

    def test_helpers_and_input_immutable(self):
        t = task()
        before = copy.deepcopy(t)
        self.client.create_task(t)
        self.assertEqual(t, before)
        self.assertIsNotNone(re.search(t['mailElements'][0]['regex'], 'code 123456'))
        link = link_task(email='a', from_='b', subject_includes='c', link_includes='/login?a=[x]')
        self.assertIsNotNone(re.search(link['mailElements'][0]['regex'], 'https://example.com/login?a=[x]&b=1'))

    def test_every_poll_attempt_obeys_pace(self):
        self.client._poll_interval = .06
        self.client._backoff = lambda attempt: .001
        self.client._auth_interval = .001
        self.state['poll'] = [(429, {'error': 'rate limit exceeded'}, {'Retry-After': '.001'}),
                              'drop', (503, {'error': 'busy'}, {'Retry-After': '.001'})]
        result = self.client.run(task(), timeout=2)
        self.assertEqual(result['status'], 'success')
        self.client.poll()  # direct calls share the same pacing gate
        attempts = [c[-1] for c in self.state['calls'] if c[0] == 'poll']
        self.assertEqual(len(attempts), 5)
        self.assertTrue(all(b - a >= .045 for a, b in zip(attempts, attempts[1:])), attempts)

    def test_buffer_survives_later_poll_failure(self):
        self.state['results'][self.client.group] = [{'id': 'early', 'status': 'success'}]
        self.assertEqual(self.client.poll()[0]['id'], 'early')
        self.state['poll'] = [(400, {'error': 'invalid group query'}, {})]
        with self.assertRaises(InvalidRequestError):
            self.client.wait('different', timeout=1)
        self.assertEqual(self.client.wait('early', timeout=.1)['id'], 'early')

    def test_request_timeouts_no_create_retry_and_default_deadline(self):
        class TimeoutOpener:
            def __init__(self):
                self.calls = []
            def open(self, request, timeout):
                self.calls.append(timeout)
                raise socket.timeout('ambiguous request timeout')
        opener = TimeoutOpener()
        client = MailmanTasks('secret-key', opener=opener, request_timeout=.123)
        with self.assertRaises(MailmanError):
            client.create_task(task())
        self.assertEqual(len(opener.calls), 1)
        self.assertTrue(0 < opener.calls[0] <= .123)
        client.close()
        self.state['auto'] = False
        t = task()
        t['timeout'] = 7
        t['group'] = 'explicit group'
        task_id = self.client.create_task(t)
        future, group, duration = self.client._register(task_id, None, None)
        try:
            self.assertEqual(group, 'explicit group')
            self.assertTrue(21 <= duration <= 22)
        finally:
            self.client._unregister(task_id, group, future)
        with self.assertRaises(TaskTimeoutError):
            self.client.wait(task_id, timeout=.03)
        self.assertTrue(any('explicit+group' in c[1] for c in self.state['calls'] if c[0] == 'poll'))

    def test_delivery_is_stored_before_waiter_notification(self):
        self.state['auto'] = False
        task_id = self.client.create_task(task())
        future, group, _ = self.client._register(task_id, None, 1)
        self.state['results'][group] = [{'id': task_id, 'status': 'success'}]
        self.client.poll(group)
        self.assertEqual(future.result(timeout=1)['id'], task_id)
        # Simulate the caller timing out/cancelling after delivery, before consumption.
        self.client._unregister(task_id, group, future)
        self.assertEqual(self.client.wait(task_id, timeout=.1)['id'], task_id)

    def test_worker_stops_if_last_waiter_expires_at_pacing_gate(self):
        self.client._poll_interval = .08
        self.client.poll()
        with self.assertRaises(TaskTimeoutError):
            self.client.wait('lost', timeout=.01)
        time.sleep(.1)
        self.assertEqual(self.count('poll'), 1)
        self.assertFalse(self.client._workers)

    def test_truncated_http_error_body_recovers(self):
        self.state['poll'] = ['truncated503']
        self.assertEqual(self.client.run(task(), timeout=1)['status'], 'success')
        self.assertEqual(self.count('poll'), 2)
        # Also preserve the status for direct callers (no leaked IncompleteRead).
        self.state['poll'] = ['truncated503']
        with self.assertRaises(ServerBusyError) as caught:
            self.client.poll()
        self.assertEqual(caught.exception.status, 503)

    def test_unexpected_worker_failure_wakes_waiters_and_restarts(self):
        original = self.client._opener
        class FailOnce:
            def open(inner, request, timeout):
                self.client._opener = original
                raise RuntimeError('private transport details secret-key')
        self.client._opener = FailOnce()
        with self.assertRaisesRegex(MailmanError, 'worker failed unexpectedly') as caught:
            self.client.wait('recover', timeout=1)
        self.assertNotIn('secret-key', str(caught.exception))
        with self.client._condition:
            self.assertFalse(self.client._workers)
        self.state['results'][self.client.group] = [{'id': 'recover', 'status': 'success'}]
        self.assertEqual(self.client.wait('recover', timeout=1)['id'], 'recover')

    def test_direct_poll_and_restarted_worker_share_long_error_delays(self):
        for status in (503, 401):
            with self.subTest(status=status):
                group = 'delay-' + str(status)
                self.client._backoff = lambda attempt: .18 * (attempt + 1)
                self.client._auth_interval = .18
                self.state['poll'] = [(status, {}, {})]
                self.client._created['lost-' + group] = (time.monotonic(), group, time.monotonic() + 1)
                future, selected, _ = self.client._register('lost-' + group, None, 1)
                with self.client._condition:
                    # Wait for the error handler to publish its gate atomically.
                    self.assertTrue(self.client._condition.wait_for(
                        lambda: group in self.client._groups and self.client._groups[group].attempt == 1,
                        timeout=1))
                self.client._unregister('lost-' + group, selected, future)
                # Direct caller must inherit the old worker's delay.
                before = self.count('poll')
                self.client.poll(group)
                attempts = [c[-1] for c in self.state['calls'] if c[0] == 'poll' and group in c[1]]
                self.assertEqual(self.count('poll'), before + 1)
                self.assertGreaterEqual(attempts[-1] - attempts[-2], .16)
                # Then a direct error must delay a newly started worker too.
                self.state['poll'] = [(status, {}, {})]
                with self.assertRaises(MailmanError):
                    self.client.poll(group)
                self.state['results'][group] = [{'id': 'restart-' + group, 'status': 'success'}]
                self.client._created['restart-' + group] = (time.monotonic(), group, time.monotonic() + 1)
                self.assertEqual(self.client.wait('restart-' + group, timeout=1)['status'], 'success')
                attempts = [c[-1] for c in self.state['calls'] if c[0] == 'poll' and group in c[1]]
                self.assertGreaterEqual(attempts[-1] - attempts[-2], .16)

    def test_live_metadata_capacity_reclaims_completed_and_expired(self):
        self.client._metadata_limit = 2
        self.state['auto'] = False
        old = dict(task(), group='old-explicit', timeout=3600)
        old_id = self.client.create_task(old)
        other_id = self.client.create_task(task())
        before = self.count('create')
        for _ in range(2):
            with self.assertRaisesRegex(TooManyTasksError, 'metadata capacity exceeded'):
                self.client.create_task(task())
        self.assertEqual(self.count('create'), before)
        metadata = self.client._created[old_id]
        self.assertEqual(metadata[1], 'old-explicit')
        self.assertGreater(metadata[2] - time.monotonic(), 3600)
        future, selected, duration = self.client._register(old_id, None, None)
        self.assertEqual(selected, 'old-explicit')
        self.assertGreater(duration, 3600)
        self.client._unregister(old_id, selected, future)
        # Completion reclaims capacity without losing buffered routing.
        self.state['results']['old-explicit'] = [{'id': old_id, 'status': 'success'}]
        self.client.poll('old-explicit')
        self.assertNotIn(old_id, self.client._created)
        new_id = self.client.create_task(dict(task(), timeout=3600))
        self.assertEqual(self.client.wait(old_id, timeout=.1)['id'], old_id)
        # Expiry also frees a slot, without needing an extra HTTP response.
        end = self.client._created[other_id][2]
        with patch('mailman_tasks.client.time.monotonic', return_value=end + .01):
            self.client.create_task(task())
        self.assertNotIn(other_id, self.client._created)
        self.assertIn(new_id, self.client._created)

    def test_metadata_capacity_reserves_concurrent_creates(self):
        entered, release = threading.Event(), threading.Event()
        class BlockingOpener:
            def open(inner, request, timeout):
                entered.set()
                release.wait(1)
                return io.BytesIO(b'{"id":"reserved"}')
        with MailmanTasks('key', opener=BlockingOpener(), metadata_limit=1) as client:
            with ThreadPoolExecutor(1) as pool:
                pending = pool.submit(client.create_task, task())
                self.assertTrue(entered.wait(1))
                try:
                    with self.assertRaisesRegex(TooManyTasksError, 'metadata capacity exceeded'):
                        client.create_task(task())
                finally:
                    release.set()
                self.assertEqual(pending.result(timeout=1), 'reserved')
                self.assertIn('reserved', client._created)

    def test_inactive_groups_retire_without_splitting_queued_callers(self):
        self.client._poll_interval = .04
        self.client.poll('queued')
        with ThreadPoolExecutor(3) as pool:
            calls = [pool.submit(self.client.poll, 'queued') for _ in range(3)]
            for call in calls:
                self.assertEqual(call.result(timeout=1), [])
        attempts = [c[-1] for c in self.state['calls'] if c[0] == 'poll']
        self.assertTrue(all(b-a >= .025 for a, b in zip(attempts, attempts[1:])), attempts)
        # Automatic retirement needs no subsequent API call to trigger pruning.
        with self.client._condition:
            self.assertTrue(self.client._condition.wait_for(lambda: not self.client._groups, timeout=1))
        for i in range(100):
            self.client.poll('inactive-' + str(i))
        with self.client._condition:
            self.assertTrue(self.client._condition.wait_for(lambda: not self.client._groups, timeout=1))

    def test_async_worker_failure_restarts_and_capacity_is_injectable(self):
        async def check():
            async with AsyncMailmanTasks('key', base_url=self.client.base_url, metadata_limit=1) as client:
                client._client._poll_interval = .01
                original = client._client._opener
                class FailOnce:
                    def open(inner, request, timeout):
                        client._client._opener = original
                        raise RuntimeError('unexpected transport failure')
                client._client._opener = FailOnce()
                with self.assertRaisesRegex(MailmanError, 'worker failed unexpectedly'):
                    await client.wait('async-restart', timeout=1)
                self.state['results']['default'] = [{'id': 'async-restart', 'status': 'success'}]
                self.assertEqual((await client.wait('async-restart', timeout=1))['status'], 'success')
                self.state['auto'] = False
                await client.create_task(task())
                with self.assertRaisesRegex(TooManyTasksError, 'metadata capacity exceeded'):
                    await client.create_task(task())
        asyncio.run(check(), debug=True)

    def test_expired_known_task_keeps_group_and_times_out_immediately(self):
        self.state['auto'] = False
        self.client._metadata_limit = 2
        task_id = self.client.create_task(dict(task(), group='specific', timeout=1))
        start, group, _ = self.client._created[task_id]
        expired = time.monotonic() - .01
        self.client._created[task_id] = (start, group, expired)
        heapq.heappush(self.client._expiry, (expired, task_id))
        # Pruning through another create must not turn the old id into unknown.
        self.client.create_task(task())
        self.assertNotIn(task_id, self.client._created)
        future, group, duration = self.client._register(task_id, None, None)
        self.assertEqual(group, 'specific')
        self.assertEqual(duration, 0)
        with self.assertRaises(TaskTimeoutError):
            future.result()
        with self.assertRaises(TaskTimeoutError):
            self.client.wait(task_id, timeout=315)
        self.assertEqual(self.count('poll'), 0)
        # Tombstones are bounded independently of live metadata.
        for i in range(5):
            end = time.monotonic() - .01
            self.client._created['expired-' + str(i)] = (end, 'specific', end)
            heapq.heappush(self.client._expiry, (end, 'expired-' + str(i)))
            self.client._prune()
        self.assertLessEqual(len(self.client._expired), 2)
        retained = set(self.client._expired)
        with patch('mailman_tasks.client.time.monotonic', return_value=time.monotonic() + 3601):
            self.client._prune()
            self.assertFalse(retained.intersection(self.client._expired))

    def test_wall_clock_jumps_do_not_change_known_task_deadline(self):
        self.state['auto'] = False
        task_id = self.client.create_task(dict(task(), group='specific', timeout=1))
        end = self.client._created[task_id][2]
        for jump in (-3600, 3600):
            with self.subTest(jump=jump), patch('mailman_tasks.client.time.time', return_value=time.time()+jump):
                # Hold the condition so the worker cannot send before removal.
                with self.client._condition:
                    future, group, duration = self.client._register(task_id, None, None)
                    self.assertEqual(group, 'specific')
                    self.assertTrue(15 <= duration <= 16)
                    self.assertEqual(self.client._created[task_id][2], end)
                    self.client._unregister(task_id, group, future)
        self.assertEqual(self.count('poll'), 0)

    def test_run_deadline_bounds_post_retry_sleep_and_request_timeout(self):
        original = self.client._opener
        timeouts = []
        class RecordingOpener:
            def open(inner, request, timeout):
                timeouts.append((request.method, timeout))
                return original.open(request, timeout=timeout)
        self.client._opener = RecordingOpener()
        self.client._backoff = lambda attempt: .15
        self.state['create'] = [(503, {'error': 'busy'}, {'Retry-After': '.15'})]
        started = time.perf_counter()
        with self.assertRaises(TaskTimeoutError):
            self.client.run(task(), deadline=time.time()+.03)
        self.assertLess(time.perf_counter()-started, .12)
        self.assertEqual(self.count('create'), 1)
        self.assertTrue(0 < timeouts[0][1] <= .03)
        with self.assertRaises(TaskTimeoutError):
            self.client.run(task(), deadline=time.time()-1)
        self.assertEqual(self.count('create'), 1)

    def test_wait_get_has_its_own_timeout_independent_of_waiter_deadline(self):
        requests = []
        class RecordingOpener:
            def open(inner, request, timeout):
                requests.append(timeout)
                return io.BytesIO(b'{"tasks":[]}')
        self.client._opener = RecordingOpener()
        with self.assertRaises(TaskTimeoutError):
            self.client.wait('lost', timeout=.02)
        self.assertEqual(len(requests), 1)
        self.assertTrue(29 < requests[0] <= 30, requests)
        time.sleep(.03)
        self.assertEqual(len(requests), 1)

    def test_create_retry_uses_longest_backoff_and_retry_after(self):
        self.client._backoff = lambda attempt: .08 * (attempt + 1)
        self.state['create'] = [(503, {}, {'Retry-After': '.01'}),
                                (503, {}, {'Retry-After': '0'})]
        self.client.create_task(task())
        attempts = [c[-1] for c in self.state['calls'] if c[0] == 'create']
        self.assertEqual(len(attempts), 3)
        self.assertGreaterEqual(attempts[1]-attempts[0], .065)
        self.assertGreaterEqual(attempts[2]-attempts[1], .145)

    def test_close_wakes_create_backoff(self):
        entered = threading.Event()
        def backoff(attempt):
            entered.set()
            return 5.0
        self.client._backoff = backoff
        self.state['create'] = [(503, {}, {'Retry-After': '0'})]
        before = self.count('create')
        errors = []
        def create():
            try:
                self.client.create_task(task())
            except MailmanError as exc:
                errors.append(exc)
        worker = threading.Thread(target=create)
        worker.start()
        self.assertTrue(entered.wait(1))
        started = time.monotonic()
        self.client.close()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertLess(time.monotonic() - started, 1)
        self.assertIn('closed', str(errors[0]))
        self.assertEqual(self.count('create'), before + 1)

    def test_async_close_wakes_create_backoff(self):
        async def check():
            client = AsyncMailmanTasks('key', base_url=self.client.base_url)
            entered = threading.Event()
            def backoff(attempt):
                entered.set()
                return 5.0
            client._client._backoff = backoff
            self.state['create'] = [(503, {}, {'Retry-After': '0'})]
            before = self.count('create')
            pending = asyncio.create_task(client.create_task(task()))
            self.assertTrue(await asyncio.get_running_loop().run_in_executor(None, entered.wait, 1))
            started = time.monotonic()
            await client.close()
            with self.assertRaises(MailmanError) as raised:
                await asyncio.wait_for(pending, 2)
            self.assertLess(time.monotonic() - started, 1)
            self.assertIn('closed', str(raised.exception))
            self.assertEqual(self.count('create'), before + 1)
        asyncio.run(check(), debug=True)

    def test_async_cancel_during_create_backoff_stops_retry(self):
        async def check():
            async with AsyncMailmanTasks('key', base_url=self.client.base_url) as client:
                entered = threading.Event()
                def backoff(attempt):
                    entered.set()
                    return .15
                client._client._backoff = backoff
                for method in ('create_task', 'run'):
                    self.state['create'] = [(503, {}, {'Retry-After': '0'})]
                    entered.clear()
                    before = self.count('create')
                    pending = asyncio.create_task(getattr(client, method)(task()))
                    self.assertTrue(await asyncio.get_running_loop().run_in_executor(None, entered.wait, 1))
                    pending.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await pending
                    await asyncio.sleep(.2)
                    self.assertEqual(self.count('create'), before + 1)
                    self.assertEqual(client._client._creating, 0)
                    self.assertFalse(client._client._created)
        asyncio.run(check(), debug=True)

    def test_async_run_deadline_bounds_create_retry(self):
        async def check():
            async with AsyncMailmanTasks('key', base_url=self.client.base_url) as client:
                client._client._backoff = lambda attempt: .15
                self.state['create'] = [(503, {}, {'Retry-After': '.15'})]
                with self.assertRaises(TaskTimeoutError):
                    await client.run(task(), timeout=.03)
                self.assertEqual(self.count('create'), 1)
        asyncio.run(check(), debug=True)

    def test_run_absolute_deadline_survives_clock_jump_during_retry(self):
        original = self.client._opener
        self.client._backoff = lambda attempt: .15
        for jump in (-3600, 3600):
            with self.subTest(jump=jump):
                wall = time.time()
                self.state['create'] = [(503, {}, {'Retry-After': '.15'})]
                before = self.count('create')
                with patch('mailman_tasks.client.time.time', return_value=wall) as clock:
                    class JumpClockOpener:
                        def open(inner, request, timeout):
                            clock.return_value = wall + jump
                            return original.open(request, timeout=timeout)
                    self.client._opener = JumpClockOpener()
                    started = time.perf_counter()
                    with self.assertRaises(TaskTimeoutError):
                        self.client.run(task(), deadline=wall + .03)
                    self.assertLess(time.perf_counter()-started, .12)
                self.assertEqual(self.count('create'), before + 1)
        self.client._opener = original

    def _held_poll(self, *ids):
        entered, release = threading.Event(), threading.Event()
        payload = {'tasks': [{'id': task_id, 'status': 'success', 'results': {'code': '123456'}} for task_id in ids]}
        self.state['poll'] = [('hold', entered, release, payload)]
        return entered, release

    def _wait_for_stopped_worker(self, client):
        with client._condition:
            self.assertTrue(client._condition.wait_for(lambda: not client._workers, timeout=1))

    def test_sync_late_poll_survives_earliest_waiter_deadline(self):
        entered, release = self._held_poll('long', 'late')
        with ThreadPoolExecutor(2) as pool:
            short = pool.submit(self.client.wait, 'short', timeout=.08)
            self.assertTrue(entered.wait(1))
            long = pool.submit(self.client.wait, 'long', timeout=1)
            try:
                started = time.perf_counter()
                with self.assertRaises(TaskTimeoutError):
                    short.result(timeout=.3)
                self.assertLess(time.perf_counter()-started, .2)
                self.assertTrue(self.client._workers[self.client.group].daemon)
                self.assertFalse(long.done())
            finally:
                release.set()
            self.assertEqual(long.result(timeout=1)['id'], 'long')
        self._wait_for_stopped_worker(self.client)
        self.assertEqual(self.client.wait('late', timeout=.1)['results']['code'], '123456')
        self.assertEqual(self.count('poll'), 1)

    def test_sync_late_poll_survives_last_waiter_leaving(self):
        entered, release = self._held_poll('late')
        future, group, _ = self.client._register('leaving', None, 1)
        self.assertTrue(entered.wait(1))
        try:
            self.client._unregister('leaving', group, future)
            future.cancel()
            self.assertFalse(self.client._waiters[group])
            self.assertTrue(self.client._workers[group].is_alive())
        finally:
            release.set()
        self._wait_for_stopped_worker(self.client)
        self.assertEqual(self.client.wait('late', timeout=.1)['id'], 'late')
        self.assertEqual(self.count('poll'), 1)

    def test_sync_late_poll_survives_only_waiter_timeout(self):
        entered, release = self._held_poll('late')
        with ThreadPoolExecutor(1) as pool:
            only = pool.submit(self.client.wait, 'only', timeout=.08)
            self.assertTrue(entered.wait(1))
            try:
                with self.assertRaises(TaskTimeoutError):
                    only.result(timeout=.3)
                self.assertTrue(self.client._workers[self.client.group].is_alive())
            finally:
                release.set()
        self._wait_for_stopped_worker(self.client)
        self.assertEqual(self.client.wait('late', timeout=.1)['id'], 'late')
        self.assertEqual(self.count('poll'), 1)

    def test_async_late_poll_survives_waiter_deadlines_and_cancellation(self):
        async def check():
            for scenario in ('earliest', 'last-leaves', 'only-times-out'):
                with self.subTest(scenario=scenario):
                    async with AsyncMailmanTasks('key', base_url=self.client.base_url) as client:
                        before = self.count('poll')
                        entered, release = self._held_poll('long', 'late')
                        only = asyncio.create_task(client.wait('only', timeout=.08 if scenario != 'last-leaves' else 1))
                        self.assertTrue(await asyncio.get_running_loop().run_in_executor(None, entered.wait, 1))
                        long = asyncio.create_task(client.wait('long', timeout=1)) if scenario == 'earliest' else None
                        try:
                            if scenario == 'last-leaves':
                                only.cancel()
                                with self.assertRaises(asyncio.CancelledError):
                                    await only
                            else:
                                started = time.perf_counter()
                                with self.assertRaises(TaskTimeoutError):
                                    await asyncio.wait_for(only, .3)
                                self.assertLess(time.perf_counter()-started, .2)
                            self.assertTrue(client._client._workers['default'].daemon)
                            self.assertTrue(client._client._workers['default'].is_alive())
                            if long is not None:
                                self.assertFalse(long.done())
                        finally:
                            release.set()
                        if long is not None:
                            self.assertEqual((await long)['id'], 'long')
                        await asyncio.get_running_loop().run_in_executor(None, self._wait_for_stopped_worker, client._client)
                        self.assertEqual((await client.wait('late', timeout=.1))['results']['code'], '123456')
                        self.assertEqual(self.count('poll'), before + 1)
        asyncio.run(check(), debug=True)

    def test_total_request_timeout_bounds_trickling_headers_and_body(self):
        self.client.request_timeout = .08
        for phase in ('headers', 'body'):
            with self.subTest(phase=phase):
                self.state['verify'] = [('trickle', phase)]
                started = time.perf_counter()
                with self.assertRaises(MailmanError):
                    self.client.verify()
                self.assertLess(time.perf_counter()-started, .25)
                self.assertFalse(self.client._requests)

    def test_close_discards_in_flight_poll_results(self):
        entered, release = self._held_poll('late')
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(self.client.wait, 'only', timeout=1)
            self.assertTrue(entered.wait(1))
            try:
                self.client.close()
                with self.assertRaises(MailmanError):
                    pending.result(timeout=.2)
                self._wait_for_stopped_worker(self.client)
                self.assertFalse(self.client._buffer)
            finally:
                release.set()

    def test_network_error_export_and_ambiguous_create_is_not_retried(self):
        self.assertTrue(issubclass(NetworkError, MailmanError))
        self.state['create'] = ['drop']
        with self.assertRaises(NetworkError) as caught:
            self.client.create_task(task())
        self.assertIsNone(caught.exception.status)
        self.assertNotIn('secret-key', str(caught.exception))
        self.assertEqual(self.count('create'), 1)
        # A known HTTP 408 remains a RequestTimeoutError, not NetworkError.
        self.state['create'] = [(408, {}, {})]
        with self.assertRaises(RequestTimeoutError) as caught:
            self.client.create_task(task())
        self.assertNotIsInstance(caught.exception, NetworkError)

    def test_transport_timeout_is_network_error(self):
        class TimeoutOpener:
            def open(inner, request, timeout):
                raise socket.timeout('transport timed out')
        with MailmanTasks('key', opener=TimeoutOpener()) as client:
            with self.assertRaises(NetworkError):
                client.create_task(task())
        self.client.request_timeout = .05
        self.state['verify'] = [('trickle', 'body')]
        with self.assertRaises(NetworkError):
            self.client.verify()

    def test_tracking_overflow_type_and_no_request_sent(self):
        self.assertTrue(issubclass(TooManyTasksError, MailmanError))
        self.client._metadata_limit = 1
        self.state['auto'] = False
        self.client.create_task(task())
        before = len(self.state['calls'])
        with self.assertRaises(TooManyTasksError) as caught:
            self.client.create_task(task())
        self.assertIsNone(caught.exception.status)
        self.assertEqual(len(self.state['calls']), before)
        async def check():
            async with AsyncMailmanTasks('key', base_url=self.client.base_url, metadata_limit=1) as client:
                await client.create_task(task())
                before = len(self.state['calls'])
                with self.assertRaises(TooManyTasksError):
                    await client.create_task(task())
                self.assertEqual(len(self.state['calls']), before)
                self.state['verify'] = ['drop']
                with self.assertRaises(NetworkError):
                    await client.verify()
        asyncio.run(check())

    def test_async(self):
        async def check():
            async with AsyncMailmanTasks('secret-key', base_url=self.client.base_url, group=self.client.group) as client:
                client._client._poll_interval = .01
                client._client._backoff = lambda attempt: .005
                self.assertTrue(await client.verify())
                self.state['poll'] = ['drop']
                results = await asyncio.gather(*(client.run(task(), timeout=2) for _ in range(40)))
                self.assertEqual(len(results), 40)
                self.assertTrue(all(r['results']['code'] == '123456' for r in results))
                self.assertEqual(await client.poll(), [])
                with self.assertRaises(TaskTimeoutError):
                    await client.wait('lost', timeout=.01)
                pending = asyncio.create_task(client.wait('cancel', timeout=2))
                await asyncio.sleep(.005)
                pending.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await pending
                self.assertFalse(client._client._waiters.get(self.client.group))
                self.state['verify'] = [(401, {}, {})]
                with self.assertRaises(AuthError):
                    await client.verify()
                self.state['status'] = 'error'
                with self.assertRaises(TaskFailedError):
                    await client.run(task(), timeout=1)
                self.state['status'] = 'timeout'
                with self.assertRaises(TaskTimeoutError):
                    await client.run(task(), timeout=1)
        asyncio.run(check())


if __name__ == '__main__':
    unittest.main()
