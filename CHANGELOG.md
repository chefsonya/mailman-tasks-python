# Changelog

## 0.2.0

- Reject millisecond `receivedAt` values (at or above 100,000,000,000) locally with `InvalidRequestError`.
- Always expose a success `results` dict, including when no elements matched; use safe element access in README examples.
- Retain delivered results for repeat waits for up to one hour, within the existing buffer cap; return copies to protect retained values.
- Pace polls from request completion, rechecking the monotonic gate after every wake, so transport delays cannot shorten the minimum gap.
- Cover HTML, empty, invalid UTF-8 and other non-JSON HTTP error bodies with regression tests; preserve typed errors and status.
- The packaged README includes the correct Mailman → Settings → Tasks API key path.

## 0.1.0

- Initial public release of the official Mailman Tasks API client.
- Thread-safe synchronous and asyncio clients, code/link task helpers, and typed errors.
- Shared per-group polling, bounded result retention, monotonic deadlines, and safe create retries.
- Python 3.8+, type hints and `py.typed`, with no runtime dependencies.
