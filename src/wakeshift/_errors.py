"""Errors the SDK raises: one hierarchy, with the same class names and codes as
the TypeScript and Go SDKs and the HTTP API (sdk/typescript/test/errors.json).

- ``WakeshiftError``: everything the SDK raises.
- ``APIError``: talking to Wakeshift failed. The core answered with an error
  (``status``, ``code``, ``reason``, ``request_id``), or didn't answer.
- ``APIConnectionError``: no answer (``unreachable`` or ``timeout``), an
  ``APIError`` whose ``status`` is None.
- ``IdReusedError``: an emit's id was used for a different emit, an
  ``APIError``.
- ``UsageError``: caught before sending (bad arguments, configuration, an
  undeclared subscribe, a disabled client…), also a ``ValueError``.

``except APIError`` catches outages too, as in the OpenAI and Anthropic SDKs.
Tell codes apart with ``err.code``.
"""

from __future__ import annotations

from typing import Any, Literal

#: Every code an SDK or the HTTP API uses, in errors.json's order: the API's,
#: then the SDK's own (unreachable onward).
ErrorCode = Literal[
    "bad_request",
    "bad_arguments",
    "bad_payload",
    "unknown_event",
    "unknown_session",
    "unknown_conversation",
    "other_client",
    "private_topic",
    "subscription_limit",
    "subscription_stopped",
    "id_reused",
    "rate_limited",
    "too_large",
    "unauthenticated",
    "no_grant",
    "unknown_workspace",
    "unknown_publish_url",
    "bad_signing_secret",
    "unsupported",
    "internal",
    "bad_response",
    "callback_failed",
    "unreachable",
    "timeout",
    "config",
    "disabled",
    "undeclared_subscribe",
    "no_call",
    "call_ended",
    "identify_failed",
    "user_required",
]

_CORE = "Check that WAKESHIFT_CORE, or the core option, is the address of the console your key came from."

#: Each code's fix, one clause, the same in every SDK (errors.json codes).
FIXES: dict[str, str] = {
    "bad_request": "Check the call against the API reference.",
    "bad_arguments": "Pass arguments and a payload that match the event's schemas.",
    "bad_payload": "Send a payload that matches the event's payload schema.",
    "unknown_event": "Define the event in this app with define_event; the SDK sends the manifest itself.",
    "unknown_session": "Use the session_key from this conversation's own results.",
    "unknown_conversation": "Use the session_key from this conversation's own results.",
    "other_client": "Only the client that made a subscription can change it.",
    "private_topic": "Subscribe only to topics this app and caller may read.",
    "subscription_limit": "End subscriptions that are no longer needed.",
    "subscription_stopped": "The person stopped this subscription; subscribe again only if they ask.",
    "id_reused": "Use a new id for a different emit; reuse an id only to retry the same emit.",
    "rate_limited": "Slow down, and retry after the Retry-After interval.",
    "too_large": "Keep text under 1000 bytes and the payload under 64 KB.",
    "unauthenticated": "Check WAKESHIFT_KEY: the key was revoked, or belongs to another console.",
    "no_grant": "Ask the user to grant this app access first.",
    "unknown_workspace": "Use a workspace ID from wake.workspace.",
    "unknown_publish_url": "Create a new publish URL: this one was revoked or never existed.",
    "bad_signing_secret": "Pass a Standard Webhooks secret: whsec_ and base64.",
    "unsupported": "Upgrade the SDK, or leave out the option this Wakeshift doesn't support.",
    "internal": "Retry later; if it persists, contact Wakeshift with the request ID.",
    "bad_response": _CORE,
    "callback_failed": "Make the webhook URL answer Wakeshift's verification with a 2xx.",
    "unreachable": _CORE,
    "timeout": "The outcome is unknown; an emit sent again with the same id emits once.",
    "config": "Fix the option or value the message names.",
    "disabled": "Set WAKESHIFT_KEY, or check wake.enabled before this call.",
    "undeclared_subscribe": "Add the event to the tool's subscribes.",
    "no_call": "Call it inside a tool call that subscribes, or pass the request's context.",
    "call_ended": "Await subscribe, unsubscribe and resolve_session before the tool returns.",
    "identify_failed": "Check identify: it raised, failed or took over 10 seconds.",
    "user_required": "Nothing to change if anonymous callers are meant to be refused; otherwise sign the caller in.",
}


def anchor(code: str) -> str:
    """The docs anchor for a code."""
    return f"https://wakeshift.ai/docs/errors#{code}"


class WakeshiftError(Exception):
    """Everything the SDK raises. ``code`` is one of ``ErrorCode`` (a plain
    ``str``, so a code a newer Wakeshift sends still reads)."""

    code: str

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


class APIError(WakeshiftError):
    """Talking to Wakeshift failed: the core answered with an error, or (an
    APIConnectionError) didn't answer. ``detail`` is Wakeshift's own message,
    without the prefix; ``reason`` says why where the API says
    (callback_failed: what the callback did)."""

    status: int | None

    def __init__(
        self,
        message: str,
        *,
        code: str,
        status: int | None,
        reason: str | None = None,
        request_id: str | None = None,
    ) -> None:
        text = f"wakeshift: {code or 'error'} ({status}): {message}" if status is not None else message
        super().__init__(text, code=code)
        self.status = status
        self.detail = message
        self.reason = reason
        self.request_id = request_id


class APIConnectionError(APIError):
    """Wakeshift didn't answer: ``unreachable`` (the connection failed) or
    ``timeout`` (no answer in time: the outcome is unknown, so retry with the
    same id). ``status`` is None; ``__cause__`` is the transport's error."""

    def __init__(self, message: str, *, code: str = "unreachable") -> None:
        super().__init__(message, code=code, status=None)


class IdReusedError(APIError):
    """An emit's id was used within the last day for a different emit
    (another event, arguments, recipients or payload). Nothing was sent. A
    retry of the same emit isn't refused: it returns the first emit's result
    with ``duplicate=True``."""

    def __init__(self, message: str, *, request_id: str | None = None) -> None:
        super().__init__(message, code="id_reused", status=409, request_id=request_id)


class UsageError(WakeshiftError, ValueError):
    """A mistake caught before sending anything: bad arguments or a bad
    payload (``issues``: each ``{"message", "path"}``), configuration, an
    undeclared subscribe, a disabled client, no call, a call that ended, or
    a failed identify. The message says what failed, the fix, and a docs
    link."""

    def __init__(self, message: str, *, code: str, issues: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message, code=code)
        self.issues: list[dict[str, Any]] = list(issues or [])
        self._what = ""


def usage(code: str, what: str, *, issues: list[dict[str, Any]] | None = None) -> UsageError:
    """A UsageError in the shared format: ``wakeshift: {what}. {fix} {anchor}``."""
    what = what.rstrip(". ")
    err = UsageError(f"wakeshift: {what}. {FIXES[code]} {anchor(code)}", code=code, issues=issues)
    err._what = what
    return err


def what_of(err: BaseException) -> str:
    """What failed, without the fix and link: for text the SDK hands an agent
    (a built-in tool's refusal), where the docs link means nothing. An
    APIError's Wakeshift message; any other error's type, never its message
    (it may carry a token)."""
    if isinstance(err, UsageError):
        return err._what or str(err)
    if isinstance(err, APIConnectionError):
        # No answer: what happened, without the developer's fix (an agent
        # told to retry could repeat work).
        return "Wakeshift didn't answer in time" if err.code == "timeout" else "Wakeshift couldn't be reached"
    if isinstance(err, APIError):
        return err.detail
    return type(err).__name__
