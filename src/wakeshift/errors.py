"""Errors the SDK raises."""

from __future__ import annotations


class WakeshiftError(Exception):
    """An error response from Wakeshift.

    ``code`` is the API's error code (``bad_arguments``, ``other_client``,
    ``unknown_event``, ``bad_request``…), ``status`` the HTTP status.
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


class IdReusedError(WakeshiftError):
    """An emit's id was used within the last day for a different emit
    (another event, arguments, recipients or payload): a WakeshiftError with
    code ``id_reused`` (status 409). Nothing was sent. A retry of the same
    emit isn't refused: it returns the first emit's result with
    ``duplicate=True``."""

    def __init__(self, message: str) -> None:
        super().__init__(409, "id_reused", message)


def is_code(err: BaseException, code: str) -> bool:
    """Whether err is a WakeshiftError with code."""
    return isinstance(err, WakeshiftError) and err.code == code
