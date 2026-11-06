"""What the SDK logs, through ``logging.getLogger("wakeshift")``, with no
handler added: an app that configures no logging still sees warnings and
errors (``logging.lastResort``), and one that does routes them like any other
library's. Every line carries its code in ``extra`` (``wakeshift_code``, and
``wakeshift_event``, ``wakeshift_tool``… where they apply) and the code's fix
verbatim; the codes, levels and policies are the three SDKs'
(sdk/typescript/test/warnings.json). A line never carries a key, a session
key, a caller's token or headers, a payload, tool arguments, or the vendor's
own error message: only the error's type."""

from __future__ import annotations

import contextvars
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("wakeshift")


@dataclass(frozen=True)
class _Code:
    level: int
    fix: str
    #: once (per client, per the fields named in per), rate (at most once per
    #: window per client, with a count), or each.
    policy: str
    per: tuple[str, ...] = ()


_WINDOW = 300.0

CODES: dict[str, _Code] = {
    "no_key": _Code(logging.DEBUG, "Set WAKESHIFT_KEY to turn Wakeshift on.", "once"),
    "no_identify": _Code(
        logging.DEBUG,
        "Add identify to tie subscriptions to signed-in users, and require_user=True to refuse anonymous callers.",
        "once",
    ),
    "no_events": _Code(logging.WARNING, "Define events with define_event before attach.", "once"),
    "text_trimmed": _Code(
        logging.WARNING, "Keep text to a short headline and put the details in the payload.", "once", ("event",)
    ),
    "rollback_failed": _Code(logging.WARNING, "Nothing to change; Wakeshift retries the cleanup.", "rate"),
    "authorize_failed": _Code(
        logging.ERROR, "Fix authorize: it raised, failed or took over 10 seconds, so the subscription was refused.", "rate"
    ),
    "identify_error": _Code(
        logging.ERROR,
        "Make identify return within 10 seconds: return no user for a credential it can't verify, "
        "and raise only when the upstream can't answer.",
        "rate",
    ),
    "user_required": _Code(
        logging.INFO,
        "Nothing to change if anonymous callers are meant to be refused; otherwise sign the caller in, or set require_user=False.",
        "rate",
    ),
    "native_unavailable": _Code(
        logging.INFO,
        "To serve MCP Events clients, give the event personal, authorize or public, and sign callers in.",
        "once",
        ("tool", "reason"),
    ),
    "event_closed": _Code(logging.WARNING, "Add authorize, personal or public: true to reopen it.", "each"),
    "wrap_unknown_tool": _Code(logging.WARNING, "Check the tool's name in wrap.", "once", ("tool",)),
    "wrap_arg_conflict": _Code(
        logging.ERROR, "Rename the tool's session_key, harness, conversation_id or wakeshift field.", "once", ("tool",)
    ),
    "wrap_bad_event": _Code(
        logging.ERROR, "Return only events defined on this client, never null, from the wrap function.", "once", ("tool",)
    ),
    "description_too_long": _Code(
        logging.WARNING,
        "Shorten the tool's description: Claude Code keeps only its first 2048 characters, which cuts off Wakeshift's sentence.",
        "once",
        ("tool",),
    ),
    "builtin_name_conflict": _Code(
        logging.ERROR, "Rename your tool, or turn the built-in off with builtin_tools.", "once", ("tool",)
    ),
    "events_capability_kept": _Code(
        logging.INFO,
        "Nothing to change if you meant to serve your own MCP Events; Wakeshift's events reach MCP Events clients only "
        "through your tools.",
        "once",
    ),
    "on_subscribe_failed": _Code(logging.ERROR, "Fix the event's on_subscribe: it raised; the subscription stands.", "rate"),
    "watch_failed": _Code(
        logging.WARNING, "Check the watch's check function and the emit; the watch stopped without emitting.", "rate"
    ),
    "watch_stopped": _Code(
        logging.INFO, "Nothing to change: the check's error was marked final, so the watch stopped as asked.", "rate"
    ),
    "watch_timeout": _Code(logging.INFO, "Raise the watch's timeout if the work can take longer.", "once", ("event",)),
    "key_refused": _Code(logging.ERROR, "Check WAKESHIFT_KEY: the key was revoked, or belongs to another console.", "rate"),
    "block_failed": _Code(
        logging.WARNING,
        "Nothing to change; the tool's result went out as the tool gave it, without Wakeshift's addition.",
        "rate",
    ),
    "subscribe_in_flight": _Code(
        logging.WARNING, "Await subscribe, unsubscribe and resolve_session before the tool returns.", "once", ("tool",)
    ),
    "monitor_line_long": _Code(
        logging.WARNING,
        "Keep the event's text under 200 bytes and put the details in the payload: Claude Code's Monitor shows a 480-byte "
        "line, about 230 of which are Wakeshift's own fields.",
        "once",
        ("event",),
    ),
    "config": _Code(logging.ERROR, "Fix the configuration the message names; the same error was raised.", "each"),
}


class Log:
    """One client's logging: misuse codes once per client, failure codes at
    most once per five minutes per code with a count of what they stand for."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self._lock = threading.Lock()
        self._once: set[tuple[Any, ...]] = set()
        #: Per rate-limited code: when it last logged, and the occurrences since.
        self._rate: dict[str, tuple[float, int]] = {}

    def __call__(self, code: str, what: str, **fields: Any) -> None:
        """Logs code (what failed, then the code's fix), as its policy allows."""
        c = CODES[code]
        with self._lock:
            if c.policy == "once":
                key = (code, *(fields.get(f) for f in c.per))
                if key in self._once:
                    return
                self._once.add(key)
            elif c.policy == "rate":
                now = self.clock()
                last = self._rate.get(code)
                if last is not None and now - last[0] < _WINDOW:
                    self._rate[code] = (last[0], last[1] + 1)
                    return
                fields["count"] = (last[1] if last is not None else 0) + 1
                self._rate[code] = (now, 0)
        extra = {"wakeshift_code": code, **{f"wakeshift_{k}": v for k, v in fields.items()}}
        logger.log(c.level, "wakeshift: %s. %s", what.rstrip(". "), c.fix, extra=extra)


#: Set while the SDK's own HTTP client sends a request to Wakeshift.
_SDK_REQUEST: contextvars.ContextVar[bool] = contextvars.ContextVar("wakeshift_sdk_request", default=False)

#: The loggers httpx2 and httpcore2 write a line per request to.
_HTTP_LOGGERS = (
    "httpx2",
    "httpcore2.connection",
    "httpcore2.http11",
    "httpcore2.http2",
    "httpcore2.proxy",
    "httpcore2.socks",
)


class _QuietSDKRequests(logging.Filter):
    """Drops httpx2's and httpcore2's lines about the SDK's own requests to
    Wakeshift (one at INFO per request), and nothing else: the app's own
    requests, and how it configured those loggers, stay as they were."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not _SDK_REQUEST.get()


_QUIET = _QuietSDKRequests()


def quiet_http_logs() -> None:
    """Adds the filter to httpx2's and httpcore2's loggers, once."""
    for name in _HTTP_LOGGERS:
        lg = logging.getLogger(name)
        if _QUIET not in lg.filters:
            lg.addFilter(_QUIET)


def warn(what: str) -> None:
    """A warning with no shared code (a misuse only Python can make)."""
    logger.warning("wakeshift: %s", what)
