"""Public exceptions; response messages are sanitized by the transport."""
from typing import Any, Dict, Optional


class MailmanError(Exception):
    def __init__(self, message: str, *, status: Optional[int] = None,
                 retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.retry_after = retry_after


class AuthError(MailmanError):
    pass


class InvalidRequestError(MailmanError):
    pass


class PayloadTooLargeError(MailmanError):
    pass


class RateLimitError(MailmanError):
    pass


class PendingLimitError(RateLimitError):
    pass


class ServerBusyError(MailmanError):
    pass


class RequestTimeoutError(MailmanError):
    pass


class NetworkError(MailmanError):
    """Transport failure; a submitted create may or may not exist."""


class TooManyTasksError(MailmanError):
    """The client tracking cap was reached before sending a create."""


class TaskFailedError(MailmanError):
    def __init__(self, result: Dict[str, Any]) -> None:
        details = result.get("results", {})
        self.error_status = self.errorStatus = details.get("errorStatus", "")
        self.error_message = self.errorMessage = details.get("errorMessage", "")
        self.result = result
        super().__init__(self.error_message or self.error_status or "Task failed")


class TaskTimeoutError(MailmanError):
    """No result in time. ``reason`` is ``"timeout"`` when the server completed the
    task as timeout (no matching email arrived in time), or ``"deadline"`` when the
    wait ended without a result: the task is lost, so trigger a fresh email and
    create it again."""

    def __init__(self, message: str, *, reason: str = "deadline") -> None:
        super().__init__(message)
        self.reason = reason
