"""Thread-safe transport and a single polling worker per client/group."""
import asyncio
import functools
import heapq
import http.client
import json
import math
import random
import re
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from concurrent.futures import Future, TimeoutError as FutureTimeout
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple

from .errors import (MailmanError, AuthError, InvalidRequestError, PayloadTooLargeError,
                     RateLimitError, PendingLimitError, ServerBusyError,
                     RequestTimeoutError, NetworkError, TooManyTasksError, TaskFailedError, TaskTimeoutError)
from .validation import prepare

Task = Dict[str, Any]
_NETWORK_ERRORS = (OSError, http.client.HTTPException)


def _copy_result(result: Task) -> Task:
    copied = dict(result)
    if "results" in result:
        copied["results"] = dict(result["results"])
    return copied


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        return None


def _retry_after(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return None


def _deadline(deadline: Optional[float], timeout: Optional[float], default: float = 315) -> float:
    if deadline is not None and timeout is not None:
        raise ValueError("supply deadline or timeout, not both")
    remaining = deadline - time.time() if deadline is not None else timeout if timeout is not None else default
    if not math.isfinite(remaining):
        raise ValueError("deadline/timeout must be finite")
    return time.monotonic() + remaining


class _Exchange:
    """One total HTTP budget, independent of poll waiters."""
    def __init__(self, timeout: float) -> None:
        self.end = time.monotonic() + timeout
        self.future = Future()  # type: Future
        self.lock = threading.Lock()
        self.socket = None  # type: Optional[socket.socket]
        self.stopped = False

    def track_socket(self, connection: socket.socket) -> None:
        with self.lock:
            self.socket = connection
            if self.stopped:
                self._shutdown()

    def _shutdown(self) -> None:
        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def abort(self, error: MailmanError) -> None:
        with self.lock:
            self.stopped = True
            self._shutdown()
            if not self.future.done():
                self.future.set_exception(error)

    def finish(self, result: Optional[Task], error: Optional[BaseException]) -> None:
        with self.lock:
            if not self.future.done():
                if time.monotonic() >= self.end:
                    error = NetworkError("HTTP request exceeded its total timeout")
                    self._shutdown()
                if error is not None:
                    self.future.set_exception(error)
                else:
                    self.future.set_result(result)


class _HTTPConnection(http.client.HTTPConnection):
    def __init__(self, *args: Any, exchange: _Exchange, **kwargs: Any) -> None:
        self.exchange = exchange
        super().__init__(*args, **kwargs)

    def connect(self) -> None:
        super().connect()
        self.exchange.track_socket(self.sock)


class _HTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, *args: Any, exchange: _Exchange, **kwargs: Any) -> None:
        self.exchange = exchange
        super().__init__(*args, **kwargs)

    def connect(self) -> None:
        super().connect()
        self.exchange.track_socket(self.sock)


class _HTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, context: threading.local) -> None:
        super().__init__()
        self.context = context

    def http_open(self, req: Any) -> Any:
        factory = functools.partial(_HTTPConnection, exchange=self.context.exchange)
        return self.do_open(factory, req)


class _HTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, context: threading.local) -> None:
        self.tls_context = ssl.create_default_context()
        super().__init__(context=self.tls_context)
        self.context = context

    def https_open(self, req: Any) -> Any:
        factory = functools.partial(_HTTPSConnection, exchange=self.context.exchange)
        return self.do_open(factory, req, context=self.tls_context)


class _Group:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.users = 0  # Includes callers queued on lock.
        self.next_poll = 0.0
        self.attempt = 0


class MailmanTasks:
    """Synchronous client. An injected opener must implement urllib's open()."""
    _poll_interval = 3.0
    _full_page_interval = 1.0
    _auth_interval = 10.0
    _buffer_limit = 50000
    _buffer_ttl = 3600.0
    _expired_ttl = 3600.0

    def __init__(self, api_key: str, *, group: str = "default",
                 base_url: str = "https://tasks.getmailman.app",
                 request_timeout: float = 15, user_agent: Optional[str] = None,
                 opener: Any = None, metadata_limit: int = 50000) -> None:
        parts = urllib.parse.urlsplit(base_url)
        if parts.scheme not in ("http", "https") or not parts.netloc or parts.username or parts.password or parts.query or parts.fragment:
            raise ValueError("base_url must be an HTTP(S) origin or base path")
        if not isinstance(api_key, str) or not api_key or any(c in api_key for c in "\r\n"):
            raise ValueError("api_key must be a nonempty single-line string")
        if not math.isfinite(request_timeout) or request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        if type(metadata_limit) is not int or metadata_limit < 1:
            raise ValueError("metadata_limit must be a positive integer")
        self._metadata_limit = metadata_limit
        self._creating = 0
        self._expiry = []  # type: List[Tuple[float, str]]
        self._expired = OrderedDict()  # type: OrderedDict[str, Tuple[float, str, float]]
        self._wait_deadlines = {}  # type: Dict[Future, float]
        self._result_groups = {}  # type: Dict[str, str]
        self._key = api_key
        self.group = group
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.user_agent = user_agent or "mailman-tasks-python/0.2.0"
        self._request_context = threading.local()
        self._opener = opener or urllib.request.build_opener(
            _NoRedirect(), _HTTPHandler(self._request_context), _HTTPSHandler(self._request_context))
        self._requests = set()  # type: Set[_Exchange]
        self._condition = threading.Condition(threading.RLock())
        self._waiters = {}  # type: Dict[str, Dict[str, List[Future]]]
        self._workers = {}  # type: Dict[str, threading.Thread]
        self._buffer = OrderedDict()  # type: OrderedDict[Tuple[str, str], Tuple[float, Task]]
        self._created = OrderedDict()  # type: OrderedDict[str, Tuple[float, str, float]]
        self._groups = {}  # type: Dict[str, _Group]
        self._retire_timer = None  # type: Optional[threading.Timer]
        self._retire_at = float("inf")
        self._closed = False
        self._interrupts = set()  # type: Set[threading.Event]  # create backoffs that close() wakes

    def _safe(self, message: str) -> str:
        return message.replace(self._key, "[redacted]")

    def _backoff(self, attempt: int) -> float:
        return min(30.0, 2.0 ** min(attempt, 5) * random.uniform(1.0, 1.25))

    def _check_operation(self, end: Optional[float], cancel: Optional[threading.Event] = None) -> float:
        if self._closed:
            raise MailmanError("Client is closed")
        if cancel is not None and cancel.is_set():
            raise MailmanError("Create cancelled; a request already sent may have been accepted")
        remaining = self.request_timeout if end is None else end - time.monotonic()
        if remaining <= 0:
            raise TaskTimeoutError("Task lost: create it again")
        return min(self.request_timeout, remaining)

    def _request(self, method: str, path: str, body: Optional[Task] = None,
                 *, end: Optional[float] = None, cancel: Optional[threading.Event] = None) -> Task:
        exchange = _Exchange(self._check_operation(end, cancel))
        with self._condition:
            if self._closed:
                raise MailmanError("Client is closed")
            self._requests.add(exchange)
        def perform() -> None:
            self._request_context.exchange = exchange
            try:
                result = self._exchange(method, path, body, exchange=exchange, end=end, cancel=cancel)
            except BaseException as exc:
                exchange.finish(None, exc)
            else:
                exchange.finish(result, None)
        # A stalled DNS resolver or injected opener must never hold process exit.
        transport = threading.Thread(target=perform, daemon=True)
        try:
            transport.start()
            try:
                return exchange.future.result(timeout=max(0, exchange.end - time.monotonic()))
            except FutureTimeout:
                error = (TaskTimeoutError("Task lost: create it again; request may have been accepted")
                         if end is not None and time.monotonic() >= end else
                         NetworkError("HTTP request exceeded its total timeout; a create may have been accepted"))
                exchange.abort(error)
                raise error from None
            except NetworkError:
                if end is not None and time.monotonic() >= end:
                    raise TaskTimeoutError("Task lost: create it again; request may have been accepted") from None
                raise
        finally:
            with self._condition:
                self._requests.discard(exchange)

    def _exchange(self, method: str, path: str, body: Optional[Task], *, exchange: _Exchange,
                  end: Optional[float], cancel: Optional[threading.Event]) -> Task:
        with self._condition:
            if self._closed:
                raise MailmanError("Client is closed")
        data = json.dumps(body, ensure_ascii=True, allow_nan=False).encode("utf-8") if body is not None else None
        request = urllib.request.Request(self.base_url + path, data=data, method=method,
                                         headers={"Authorization": "Token " + self._key,
                                                  "Content-Type": "application/json",
                                                  "User-Agent": self.user_agent})
        try:
            timeout = min(self._check_operation(end, cancel), exchange.end - time.monotonic())
            if timeout <= 0:
                raise NetworkError("HTTP request exceeded its total timeout")
            response = self._opener.open(request, timeout=timeout)
            with response:
                payload = response.read()
            return json.loads(payload.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            status = exc.code
            delay = _retry_after(exc.headers.get("Retry-After"))
            try:
                with exc:
                    payload = exc.read()
            except _NETWORK_ERRORS:
                # The status/headers are known even if the error body was lost.
                # A truncated 429 might conceal a pending-limit response: do not
                # automatically retry that ambiguous create.
                if status == 429:
                    raise NetworkError("Incomplete HTTP 429 error response", status=429, retry_after=delay) from None
                payload = b""
            try:
                message = json.loads(payload.decode("utf-8")).get("error", "HTTP {}".format(status))
            except (ValueError, AttributeError):
                message = "HTTP {}".format(status)
            message = self._safe(str(message))
            cls = {400: InvalidRequestError, 401: AuthError, 408: RequestTimeoutError,
                   413: PayloadTooLargeError, 429: RateLimitError, 503: ServerBusyError}.get(status, MailmanError)
            if status == 401:
                message = "The key was rejected, or Mailman isn't open on your computer."
            if status == 429 and message == "pending task limit exceeded":
                cls = PendingLimitError
            raise cls(message, status=status, retry_after=delay) from None
        except _NETWORK_ERRORS:
            if end is not None and time.monotonic() >= end:
                raise TaskTimeoutError("Task lost: create it again; request may have been accepted") from None
            # Do not expose urllib exceptions, which may include request details.
            raise NetworkError("Network request failed; a create may have been accepted") from None
        except (ValueError, UnicodeError):
            raise NetworkError("Invalid JSON response; a create may have been accepted") from None

    def verify(self) -> bool:
        self._request("GET", "/api/v1/user/verify")
        return True

    def _prepare(self, task: Mapping[str, Any]) -> Task:
        try:
            body = prepare(task, self.group)
            # Catch invalid unknown JSON values before touching the transport too.
            json.dumps(body, allow_nan=False)
        except InvalidRequestError as exc:
            raise InvalidRequestError(self._safe(str(exc)), status=400) from None
        except (ValueError, TypeError):
            raise InvalidRequestError("task must contain JSON values", status=400) from None
        return body

    def create_task(self, task: Mapping[str, Any]) -> str:
        return self._create_task(task)

    def _create_task(self, task: Mapping[str, Any], *, end: Optional[float] = None,
                     cancel: Optional[threading.Event] = None) -> str:
        body = self._prepare(task)
        if end is None:
            end = time.monotonic() + body["timeout"] + 15
        self._check_operation(end, cancel)
        with self._condition:
            self._prune()
            if len(self._created) + self._creating >= self._metadata_limit:
                raise TooManyTasksError("Live task metadata capacity exceeded; collect results or wait for expiry")
            self._creating += 1  # Reserve before sending, including concurrent creates.
        try:
            for attempt in range(3):
                try:
                    created_at = time.monotonic()
                    result = self._request("POST", "/api/v1/tasks/mail/create", body, end=end, cancel=cancel)
                    task_id = result["id"]
                    with self._condition:
                        task_end = created_at + body["timeout"] + 15
                        if task_id not in self._result_groups:
                            self._created[task_id] = (created_at, body["group"], task_end)
                            heapq.heappush(self._expiry, (task_end, task_id))
                    return task_id
                except (RateLimitError, ServerBusyError) as exc:
                    if isinstance(exc, PendingLimitError) or attempt == 2:
                        raise
                    delay = max(self._backoff(attempt), exc.retry_after or 0)
                    wake = time.monotonic() + delay
                    # Event.wait makes async cancellation interrupt backoff. The
                    # absolute monotonic deadline also bounds every retry sleep.
                    interrupt = cancel if cancel is not None else threading.Event()
                    with self._condition:
                        self._interrupts.add(interrupt)  # close() sets it too
                    try:
                        while time.monotonic() < wake:
                            remaining = self._check_operation(end, cancel)
                            interrupt.wait(max(0.0, min(wake - time.monotonic(), remaining)))
                    finally:
                        with self._condition:
                            self._interrupts.discard(interrupt)
                    self._check_operation(end, cancel)
            raise AssertionError("unreachable")
        finally:
            with self._condition:
                self._creating -= 1

    def poll(self, group: Optional[str] = None) -> List[Task]:
        """One destructive GET, with no retry. Persist every returned result."""
        return self._poll(self.group if group is None else group, worker=False)

    def _retire_groups(self) -> None:
        # Caller holds _condition. References are acquired before queuing on a
        # group's lock, so retiring a state can never split its serialization.
        now = time.monotonic()
        pending = []
        for group, state in list(self._groups.items()):
            if state.users or group in self._workers or self._waiters.get(group):
                continue
            if self._closed or state.next_poll <= now:
                del self._groups[group]
                self._condition.notify_all()
            else:
                pending.append(state.next_poll)
        if pending and not self._closed and min(pending) < self._retire_at:
            if self._retire_timer is not None:
                self._retire_timer.cancel()
            self._retire_at = min(pending)
            timer = threading.Timer(max(.001, self._retire_at - now), self._retire_idle)
            timer.daemon = True
            self._retire_timer = timer
            timer.start()

    def _retire_idle(self) -> None:
        with self._condition:
            if self._retire_timer is not threading.current_thread():
                return
            self._retire_timer = None
            self._retire_at = float("inf")
            self._retire_groups()

    def _poll(self, selected: str, *, worker: bool) -> List[Task]:
        with self._condition:
            self._retire_groups()
            state = self._groups.setdefault(selected, _Group())
            state.users += 1
        try:
            with state.lock:
                with self._condition:
                    while state.next_poll > time.monotonic():
                        if worker:
                            self._expire_waiters(selected)
                        if worker and not self._waiters.get(selected):
                            return []
                        if self._closed:
                            raise MailmanError("Client is closed")
                        delay = state.next_poll - time.monotonic()
                        if worker:
                            delay = min(delay, self._group_end(selected) - time.monotonic())
                        self._condition.wait(max(0, delay))
                    if worker:
                        self._expire_waiters(selected)
                    if worker and not self._waiters.get(selected):
                        return []
                    state.next_poll = time.monotonic() + self._poll_interval
                query = urllib.parse.urlencode({"group": selected})
                try:
                    results = self._request("GET", "/api/v1/tasks/completed?" + query)["tasks"]
                except MailmanError as exc:
                    # Update the shared gate BEFORE releasing state.lock. Direct
                    # callers and restarted workers inherit the same error streak.
                    delay = max(self._poll_interval, exc.retry_after or 0)
                    if isinstance(exc, AuthError):
                        delay = max(delay, self._auth_interval)
                    elif exc.status in (None, 408, 429, 503):
                        delay = max(delay, self._backoff(state.attempt))
                    with self._condition:
                        state.attempt += 1
                        state.next_poll = max(state.next_poll, time.monotonic() + delay)
                        self._condition.notify_all()
                    raise
                with self._condition:
                    if self._closed:
                        raise MailmanError("Client is closed; in-flight poll results discarded")
                    state.attempt = 0
                    interval = self._full_page_interval if len(results) == 100 else self._poll_interval
                    # Pace from completion so transport dispatch delays cannot
                    # shorten the gap between actual HTTP attempts.
                    state.next_poll = time.monotonic() + interval
                    self._prune()
                    for result in results:
                        if result["status"] == "success":
                            result.setdefault("results", {})
                        task_id = result["id"]
                        key = (selected, task_id)
                        self._buffer[key] = (time.monotonic(), _copy_result(result))
                        self._buffer.move_to_end(key)
                        self._result_groups[task_id] = selected
                        self._created.pop(task_id, None)
                        self._expired.pop(task_id, None)
                        waiting = self._waiters.get(selected, {}).pop(task_id, [])
                        for future in waiting:
                            if not future.done():
                                future.set_result(result)
                    while len(self._buffer) > self._buffer_limit:
                        key, _ = self._buffer.popitem(last=False)
                        self._result_groups.pop(key[1], None)
                return results
        finally:
            with self._condition:
                state.users -= 1
                self._retire_groups()

    def _prune(self) -> None:
        cutoff = time.monotonic() - self._buffer_ttl
        while self._buffer and next(iter(self._buffer.values()))[0] <= cutoff:
            key, _ = self._buffer.popitem(last=False)
            self._result_groups.pop(key[1], None)
        now = time.monotonic()
        while self._expired and next(iter(self._expired.values()))[0] + self._expired_ttl <= now:
            self._expired.popitem(last=False)
        while self._expiry and self._expiry[0][0] <= now:
            end, task_id = heapq.heappop(self._expiry)
            metadata = self._created.get(task_id)
            if metadata and metadata[2] == end:
                del self._created[task_id]
                self._expired[task_id] = (now, metadata[1], end)
                while len(self._expired) > self._metadata_limit:
                    self._expired.popitem(last=False)
        # Compact stale heap records left by completed tasks.
        if len(self._expiry) > max(100, 2 * len(self._created)):
            self._expiry = [(meta[2], task_id) for task_id, meta in self._created.items()]
            heapq.heapify(self._expiry)

    def _register(self, task_id: str, deadline: Optional[float], timeout: Optional[float], *, end: Optional[float] = None) -> Tuple[Future, str, float]:
        if deadline is not None and timeout is not None:
            raise ValueError("supply deadline or timeout, not both")
        with self._condition:
            if self._closed:
                raise MailmanError("Client is closed")
            self._prune()
            metadata = self._created.get(task_id) or self._expired.get(task_id)
            group = metadata[1] if metadata else self._result_groups.get(task_id, self.group)
            if end is None:
                end = _deadline(deadline, timeout) if deadline is not None or timeout is not None else metadata[2] if metadata else _deadline(None, None)
            if task_id in self._expired:
                end = min(end, self._expired[task_id][2])  # Known expired ids never get a fresh wait.
            future = Future()  # type: Future
            cached = self._buffer.get((group, task_id))
            if cached:
                future.set_result(cached[1])
            elif end <= time.monotonic():
                future.set_exception(TaskTimeoutError("Task lost: create it again"))
            else:
                self._wait_deadlines[future] = end
                self._waiters.setdefault(group, {}).setdefault(task_id, []).append(future)
                if group not in self._workers:
                    worker = threading.Thread(target=self._poll_loop, args=(group,), daemon=True)
                    self._workers[group] = worker
                    worker.start()
            return future, group, max(0.0, end - time.monotonic())

    def _unregister(self, task_id: str, group: str, future: Future) -> None:
        with self._condition:
            self._wait_deadlines.pop(future, None)
            waiting = self._waiters.get(group, {}).get(task_id, [])
            if future in waiting:
                waiting.remove(future)
            if not waiting:
                self._waiters.get(group, {}).pop(task_id, None)
            self._condition.notify_all()

    def wait(self, task_id: str, *, deadline: Optional[float] = None,
             timeout: Optional[float] = None) -> Task:
        """Wait for any final status. deadline is Unix seconds; timeout is relative."""
        return self._wait(task_id, deadline=deadline, timeout=timeout)

    def _wait(self, task_id: str, *, deadline: Optional[float] = None,
              timeout: Optional[float] = None, end: Optional[float] = None) -> Task:
        if end is None and (deadline is not None or timeout is not None):
            end = _deadline(deadline, timeout)
        future, group, duration = self._register(task_id, deadline, timeout, end=end)
        try:
            result = future.result(timeout=duration)
            return _copy_result(result)
        except FutureTimeout:
            raise TaskTimeoutError("Task lost: create it again") from None
        finally:
            self._unregister(task_id, group, future)

    def _group_end(self, group: str) -> float:
        return min(self._wait_deadlines[f] for waiting in self._waiters[group].values() for f in waiting)

    def _expire_waiters(self, group: str) -> None:
        now = time.monotonic()
        for task_id, waiting in list(self._waiters.get(group, {}).items()):
            for future in list(waiting):
                if self._wait_deadlines[future] <= now:
                    waiting.remove(future)
                    self._wait_deadlines.pop(future, None)
                    if not future.done():
                        future.set_exception(TaskTimeoutError("Task lost: create it again"))
            if not waiting:
                self._waiters[group].pop(task_id, None)

    def _poll_loop(self, group: str) -> None:
        failure = None  # type: Optional[MailmanError]
        try:
            while True:
                with self._condition:
                    self._expire_waiters(group)
                    if not self._waiters.get(group) or self._closed:
                        return
                try:
                    self._poll(group, worker=True)
                except MailmanError as exc:
                    if exc.status not in (None, 401, 408, 429, 503) or isinstance(exc, PendingLimitError):
                        failure = exc
                        return
        except Exception:
            # Unexpected transport/dispatcher failures must wake existing waiters
            # and allow the next wait to start a fresh worker. Never leak details.
            failure = MailmanError("Polling worker failed unexpectedly; retry the wait")
        finally:
            with self._condition:
                self._workers.pop(group, None)
                if failure is None and self._waiters.get(group) and not self._closed:
                    # A waiter may register between the normal stop check and
                    # finally. Transfer it to a replacement under the same lock.
                    replacement = threading.Thread(target=self._poll_loop, args=(group,), daemon=True)
                    self._workers[group] = replacement
                    replacement.start()
                else:
                    waiting = self._waiters.pop(group, {})
                    for futures in waiting.values():
                        for future in futures:
                            if not future.done():
                                future.set_exception(failure or MailmanError("Polling worker stopped"))
                self._retire_groups()
                self._condition.notify_all()

    def run(self, task: Mapping[str, Any], *, deadline: Optional[float] = None,
            timeout: Optional[float] = None) -> Task:
        if deadline is not None and timeout is not None:
            raise ValueError("supply deadline or timeout, not both")
        started = time.monotonic()
        end = _deadline(deadline, timeout) if deadline is not None or timeout is not None else None
        body = self._prepare(task)
        if end is None:
            end = started + body["timeout"] + 15
        task_id = self._create_task(body, end=end)
        return _outcome(self._wait(task_id, end=end), self._safe)

    def close(self) -> None:
        with self._condition:
            self._closed = True
            for interrupt in self._interrupts:
                interrupt.set()
            for exchange in list(self._requests):
                exchange.abort(MailmanError("Client is closed; in-flight results discarded"))
            if self._retire_timer is not None:
                self._retire_timer.cancel()
                self._retire_timer = None
            self._retire_at = float("inf")
            self._retire_groups()
            for tasks in self._waiters.values():
                for waiting in tasks.values():
                    for future in waiting:
                        if not future.done():
                            future.set_exception(MailmanError("Client is closed"))
            self._condition.notify_all()

    def __enter__(self) -> "MailmanTasks":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


def _outcome(result: Task, redact: Callable[[str], str]) -> Task:
    if result["status"] == "timeout":
        raise TaskTimeoutError("Task timed out: no matching email arrived in time", reason="timeout")
    if result["status"] == "error":
        safe_result = dict(result)
        safe_result["results"] = {k: redact(str(v)) for k, v in result.get("results", {}).items()}
        raise TaskFailedError(safe_result)
    return result


class AsyncMailmanTasks:
    """Async facade; HTTP uses an executor, waits do not occupy executor threads."""
    def __init__(self, api_key: str, *, group: str = "default",
                 base_url: str = "https://tasks.getmailman.app", request_timeout: float = 15,
                 user_agent: Optional[str] = None, opener: Any = None,
                 metadata_limit: int = 50000) -> None:
        self._client = MailmanTasks(api_key, group=group, base_url=base_url,
                                   request_timeout=request_timeout, user_agent=user_agent, opener=opener,
                                   metadata_limit=metadata_limit)

    async def _call(self, function: Callable[..., Any], *args: Any) -> Any:
        return await asyncio.get_running_loop().run_in_executor(None, functools.partial(function, *args))

    async def verify(self) -> bool:
        return await self._call(self._client.verify)

    async def create_task(self, task: Mapping[str, Any]) -> str:
        return await self._create_task(task)

    async def _create_task(self, task: Mapping[str, Any], *, end: Optional[float] = None) -> str:
        cancel = threading.Event()
        call = functools.partial(self._client._create_task, task, end=end, cancel=cancel)
        try:
            return await self._call(call)
        except asyncio.CancelledError:
            cancel.set()
            raise

    async def poll(self, group: Optional[str] = None) -> List[Task]:
        return await self._call(self._client.poll, group)

    async def wait(self, task_id: str, *, deadline: Optional[float] = None,
                   timeout: Optional[float] = None) -> Task:
        return await self._wait(task_id, deadline=deadline, timeout=timeout)

    async def _wait(self, task_id: str, *, deadline: Optional[float] = None,
                    timeout: Optional[float] = None, end: Optional[float] = None) -> Task:
        if end is None and (deadline is not None or timeout is not None):
            end = _deadline(deadline, timeout)
        future, group, duration = self._client._register(task_id, deadline, timeout, end=end)
        if future.done():
            try:
                return _copy_result(future.result())
            finally:
                self._client._unregister(task_id, group, future)
        wrapped = asyncio.wrap_future(future)
        try:
            result = await asyncio.wait_for(asyncio.shield(wrapped), duration)
            return _copy_result(result)
        except asyncio.TimeoutError:
            raise TaskTimeoutError("Task lost: create it again") from None
        finally:
            self._client._unregister(task_id, group, future)
            if not future.done():
                future.cancel()
            # Retrieve exceptions even when cancellation races a poll response.
            if not wrapped.done():
                wrapped.cancel()
            elif not wrapped.cancelled():
                wrapped.exception()

    async def run(self, task: Mapping[str, Any], *, deadline: Optional[float] = None,
                  timeout: Optional[float] = None) -> Task:
        if deadline is not None and timeout is not None:
            raise ValueError("supply deadline or timeout, not both")
        started = time.monotonic()
        end = _deadline(deadline, timeout) if deadline is not None or timeout is not None else None
        body = self._client._prepare(task)
        if end is None:
            end = started + body["timeout"] + 15
        task_id = await self._create_task(body, end=end)
        return _outcome(await self._wait(task_id, end=end), self._client._safe)

    async def close(self) -> None:
        self._client.close()

    async def __aenter__(self) -> "AsyncMailmanTasks":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


def _filters(from_: str, subject_includes: str) -> List[Task]:
    return [{"target": "from", "comparator": "includes", "value": from_},
            {"target": "subject", "comparator": "includes", "value": subject_includes}]


def code_task(*, email: str, from_: str, subject_includes: str, length: int = 6) -> Task:
    if type(length) is not int or length < 1 or length > 1024:
        raise InvalidRequestError("length must be an integer from 1 to 1024", status=400)
    return {"email": email, "mailFilters": _filters(from_, subject_includes),
            "mailElements": [{"name": "code", "target": "body", "regex": r"\b\d{" + str(length) + r"}\b"}]}


def link_task(*, email: str, from_: str, subject_includes: str, link_includes: str) -> Task:
    # Match href as text after the link formatter, avoiding CSS quoting injection.
    pattern = r"https?://[^\s]*" + re.escape(link_includes) + r"[^\s]*"
    return {"email": email, "mailFilters": _filters(from_, subject_includes),
            "mailElements": [{"name": "loginUrl", "target": "body", "selector": "a[href]",
                              "formatter": "link", "regex": pattern}]}
