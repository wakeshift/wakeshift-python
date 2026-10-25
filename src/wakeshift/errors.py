"""Errors the SDK raises."""

from __future__ import annotations


class WakeshiftError(Exception):
    """An error response from Wakeshift.

    ``code`` is the API's error code (``private_topic``, ``other_client``,
    ``unknown_subscription``, ``bad_request``…), ``status`` the HTTP status.
    A request that got no answer has code ``unreachable`` and status 0.
    """

    def __init__(self, status: int, code: str, message: str, *, reason: str | None = None) -> None:
        text = f"wakeshift: {code or 'error'} ({status}): {message}" if status else f"wakeshift: {message}"
        super().__init__(text)
        self.status = status
        self.code = code
        #: Wakeshift's own message, without the prefix.
        self.detail = message
        #: Why, where the API says (callback_failed: what the callback did).
        self.reason = reason


def is_code(err: BaseException, code: str) -> bool:
    """Whether err is a WakeshiftError with code."""
    return isinstance(err, WakeshiftError) and err.code == code
