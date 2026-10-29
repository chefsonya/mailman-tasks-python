"""Mailman Tasks API, Python 3.8+, standard library only."""
from .client import AsyncMailmanTasks, MailmanTasks, code_task, link_task
from .errors import (
    MailmanError, AuthError, InvalidRequestError, PayloadTooLargeError,
    RateLimitError, PendingLimitError, ServerBusyError, RequestTimeoutError,
    NetworkError, TooManyTasksError, TaskFailedError, TaskTimeoutError,
)

__version__ = "0.1.0"
__all__ = ["MailmanTasks", "AsyncMailmanTasks", "code_task", "link_task",
           "MailmanError", "AuthError", "InvalidRequestError", "PayloadTooLargeError",
           "RateLimitError", "PendingLimitError", "ServerBusyError", "RequestTimeoutError",
           "NetworkError", "TooManyTasksError", "TaskFailedError", "TaskTimeoutError"]
