# Mailman Tasks for Python

Official Python 3.8+ client for [Mailman's hosted Tasks API](https://getmailman.app/developers).
Version **0.1.0**, MIT license, zero runtime dependencies.

## Install

```sh
pip install mailman-tasks
```

Import as `mailman_tasks`. Get your API key in **Mailman ? Settings ? API & agents
? Tasks API**. Mailman must be open and connected for the key to work. Read the key
from your configuration or an environment variable; never embed it in source.

See the [developer page](https://getmailman.app/developers) and
[API guide](https://getmailman.app/tasks-api.md).

## Synchronous quick start

```python
import os
from mailman_tasks import MailmanTasks, code_task

with MailmanTasks(os.environ["MAILMAN_TASKS_KEY"], group="signup-bot") as client:
    client.verify()
    task = code_task(
        email="alias@example.com", from_="noreply@example.com",
        subject_includes="verification code", length=6,
    )
    task["timeout"] = 120
    # Create just before triggering the website to send its email.
    task_id = client.create_task(task)
    # Trigger the website's email here.
    result = client.wait(task_id)
    if result["status"] == "success":
        print(result["results"]["code"])
```

`wait()` returns any final status. `run(task)` combines create and wait, returning
`{id, status: "success", results}` and raising for task errors or timeouts.
For flows where the email is already being triggered, use `client.run(task)`.
When resending after a lost task, trigger a fresh email: the desktop uses each
matching email only once per address and filter set for an hour.

## Async quick start

```python
import asyncio
import os
from mailman_tasks import AsyncMailmanTasks, code_task

async def main():
    async with AsyncMailmanTasks(os.environ["MAILMAN_TASKS_KEY"], group="bot") as client:
        await client.verify()
        result = await client.run(code_task(
            email="alias@example.com", from_="noreply@example.com",
            subject_includes="verification code",
        ))
        print(result["results"]["code"])

asyncio.run(main())
```

Concurrent `await asyncio.gather(client.run(task1), client.run(task2))` calls share
a poller. Blocking urllib calls run outside the event loop; idle waits do not use
executor threads. Cancelling an async wait removes that waiter, without cancelling
the server task. Cancelling an async create or `run()` interrupts retry backoff and
prevents subsequent create attempts. A call already sent cannot be recalled and is
ambiguous: the server may have accepted it. Do not automatically recreate it.

## API and concurrency

Both clients accept `MailmanTasks(api_key, *, group="default",
base_url="https://tasks.getmailman.app", request_timeout=30, user_agent=None,
opener=None, metadata_limit=50000)`, with async methods on `AsyncMailmanTasks`.

- `verify()` returns `True`, or raises, including `AuthError` on 401.
- `create_task(task)` returns an id. Missing `receivedAt` is now in Unix **seconds**;
  missing `timeout` is **300 seconds**; missing `group` uses the client's group.
  The input is not mutated. Explicit timeout 0 or above 3600 normalizes to 3600,
  matching the server. Every HTTP request has a **total** `request_timeout`
  budget covering connection, headers and the complete body, including trickling
  responses. POST budgets are additionally capped by the remaining operation
  deadline. No ambiguous POST is retried.
- `poll(group=None)` issues one **destructive** GET and returns a list of results.
  Persist them immediately. It makes no automatic retries. Results also enter this
  client's dispatcher so a later `wait(id)` can receive them.
- `wait(id, *, deadline=None, timeout=None)` returns any completed result.
  `deadline` is an absolute Unix timestamp in seconds; `timeout` is a relative
  wait duration. Supply at most one. By default it waits until the locally created
  task's creation time + effective task timeout + 15 seconds; an unknown id gets
  315 seconds and uses the client's group. Internal deadlines use monotonic time;
  an absolute Unix `deadline` is converted once at entry, so later wall-clock jumps
  cannot extend or shorten an existing operation. Deadlines cap poll sleeps and
  decide whether to start a GET; they never shorten an in-flight GET. A waiter
  times out promptly while the daemon worker finishes the request using its own
  full `request_timeout` budget and buffers the response for later waits.
- `run(task, *, deadline=None, timeout=None)` creates and waits, raising
  `TaskFailedError` or `TaskTimeoutError` for unsuccessful results. Its `timeout`
  bounds the entire local operation, including create attempts, retry sleeps and
  waiting. The default budget is the task timeout + 15 seconds from the start of
  `run()`. No new attempt starts after expiry. An already sent destructive GET
  finishes independently, so other tasks' codes are not lost. Use
  `task["timeout"]` for the server's lifetime.
- `close()` and sync/async context managers stop polling and release waiters.
  Explicit `close()` may abandon in-flight HTTP requests; their results are
  discarded and cannot be recovered from the server. Without an explicit close,
  the last waiter leaving never aborts an in-flight GET: the daemon worker stores
  its response (one-hour retention), then stops without another poll.
- `code_task(email=..., from_=..., subject_includes=..., length=6)` extracts
  `results["code"]` using a correctly escaped digit regex.
- `link_task(email=..., from_=..., subject_includes=..., link_includes=...)`
  extracts `results["loginUrl"]`. It uses `a[href]`, the link formatter, and a
  regex containing the escaped literal substring (including quotes or metacharacters).

The sync client is thread-safe. Each client starts **one worker per group** with
the first waiter and stops it when there are no waiters left. A task's explicit
`group` is remembered and used by its wait. Share one client for concurrent jobs;
independent clients or processes must use different groups to avoid consuming one
another's results. Do not change client configuration while it is in use.

All poll attempts, including direct `poll()` calls and retries, share the per-group
pacing gate: at least **3 seconds**, or **1 second** immediately after a full page
of 100, and longer when Retry-After/backoff requires it. Backoff and auth delays
are shared by direct polls and worker restarts. Polling 401s wait about 10 seconds and keep trying until each wait's deadline. Polling 429s honor
Retry-After; 408, 503 and network failures back off 1, 2, 4 ... up to 30 seconds
with jitter, subject to the minimum polling interval.

Results enter the bounded dispatcher before waiters are notified. Unclaimed
results live for **one hour**, up to **50,000 per client** across groups; oldest
entries are evicted at capacity. A wait consumes its stored result only after
receiving it successfully, preserving delivery across deadline/cancellation races.
Live task metadata is retained until completion or its original deadline, with
`metadata_limit=50000` slots by default (configurable on either client). Concurrent
creates reserve slots before sending. At capacity, `create_task` raises a local
`TooManyTasksError` before any HTTP attempt; collect results or wait for task expiry to
free slots. Live task groups and deadlines are never silently evicted. Expired
ids retain their group and deadline in a separate bounded expiry store (at most
`metadata_limit` entries, retained for one hour after expiry is observed). Waiting
on a retained expired id raises `TaskTimeoutError` immediately, even with an
explicit longer timeout. After expiry-store eviction/retention ends, an id is
unknown and uses the fallback described above. Completed
results retain their group routing in the result buffer until consumed or expired.
Inactive group locks, pacing state and retry counters retire after the pacing gap;
one client timer handles retirement without racing workers or queued callers.
Consume results promptly; the buffer is not durable storage.

## Errors and limits

All exceptions inherit `MailmanError`, with `status` (or `None` for local/network
errors), `message`, and `retry_after` in seconds (or `None` when absent).

| Exception | Meaning |
| --- | --- |
| `AuthError` | 401: "The key was rejected, or Mailman isn't open on your computer." |
| `InvalidRequestError` | Local validation or HTTP 400 |
| `PayloadTooLargeError` | HTTP 413 |
| `RateLimitError` | HTTP 429 |
| `PendingLimitError` | 429 pending task limit exceeded; a `RateLimitError` subclass |
| `ServerBusyError` | HTTP 503 |
| `RequestTimeoutError` | HTTP 408 |
| `NetworkError` | Transport failure, including a client-side request timeout; a create may or may not exist |
| `TooManyTasksError` | Client tracking capacity reached; no request was sent |
| `TaskFailedError` | Task status `error`; has `error_status`, `error_message`, camelCase aliases `errorStatus`, `errorMessage`, and `result` |
| `TaskTimeoutError` | Server status `timeout`, or local deadline: "Task lost: create it again" |

Create retries **only explicit 429 or 503 responses**, for at most three HTTP
attempts, sleeping for the **longest** of Retry-After and jittered exponential
backoff (including when Retry-After is zero). Retry sleeps are bounded by the
remaining operation deadline and are interruptible on async cancellation. It never
retries a pending-limit response, 400, 413, 401, or an ambiguous connection failure,
reset or client-side timeout. A failed create may have reached the server.
The default transport refuses redirects so credentials cannot follow another host.
Truncated HTTP error bodies retain their known status and Retry-After. Unexpected
worker failures wake affected waiters with `MailmanError` and clean up the worker
registration so a later wait can restart it; already buffered results survive.
Keys are sent only in the Authorization header and are redacted from HTTP errors.
`base_url` is an advanced/test override; use the default hosted URL in production.
Injected openers implement urllib's `open(request, timeout=...)`, return context
managed responses, and raise `urllib.error.HTTPError` for HTTP failures. They must
be safe for concurrent calls and preserve TLS verification and redirect safety.
The client supervises each exchange in a daemon thread to enforce its total
budget; the default urllib handlers track and shut down sockets on HTTP timeout
or explicit close. Injected transports must release their own resources when
`timeout` expires; late responses from abandoned exchanges are discarded.

Task validation follows the API contract: all known string fields at
most 1024 characters, 1–10 filters and elements, at most 10 `orValues`, exact
case-sensitive enum values, positive integer `receivedAt`, unsigned integer
`timeout`, and body elements requiring a selector or regex. Unknown fields are
ignored by the server. Regex/selector syntax is checked by the desktop and may
produce `TaskFailedError` rather than a local validation error.

Server limits: 64 KiB create bodies; 10,000 pending tasks per key; 5,000 requests
per minute per IP and 10,000 per key; at most 100 results per poll. Uncollected
results expire server-side after an hour. See the [developer guide](https://getmailman.app/developers).

## Development

From a checkout of this repository:

```sh
python -m venv .venv
# Activate .venv for your shell, then:
python -m pip install -e . mypy build twine
python -m unittest discover -s tests -v
python -m mypy mailman_tasks
python -m build
python -m twine check dist/*
```

Unit tests use a local mock HTTP server and require no API key or live account.
CI runs tests and mypy on Python 3.8, 3.10, 3.12 and 3.13 using GitHub-hosted runners.
See [SECURITY.md](SECURITY.md) for private vulnerability reporting and
[CHANGELOG.md](CHANGELOG.md) for release notes.
