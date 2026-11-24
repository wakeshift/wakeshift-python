"""MCP Events push (SEP-3415 events/stream), served by attach as a relay,
as the TypeScript SDK's push.ts. The client
holds an events/stream request open on the vendor's server; the SDK opens
one upstream stream to Wakeshift for it (POST /api/apps/v1/events/stream,
NDJSON) and writes each frame to the client as a notification related to the
request, stamped with the request's ID. Wakeshift keeps the subscription, its
queue, cursors, heartbeats, consent and limits; the SDK keeps nothing that
has to outlive the request:

- the client's abort (its request's cancel scope) ends the upstream, and gets
  no result; an explicit cancel over stdio (notifications/cancelled) also
  ends the subscription (POST /events/stream/end);
- Wakeshift ending the stream (terminated, close) ends the client's, with the
  final result, and so does an upstream silent for three heartbeat
  intervals, the app's max_stream_ms passing, or identify and authorize
  failing (not refusing) for _max_unchecked_ms: the client reconnects with
  its cursor;
- the upstream has no read timeout, no retries and no breaker: a stream that
  fails to open answers the client with an error, and the client retries;
- frames, cursors, arguments and vouchers are never logged."""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypedDict

from ._errors import APIConnectionError, APIError, usage
from ._spec import INTERNAL_ERROR, INVALID_PARAMS, RESULT_TYPE, STREAM_REFUSAL, SUBSCRIPTION_ID_META, Codes

if TYPE_CHECKING:
    from ._client import Wakeshift


class PushOptions(TypedDict, total=False):
    """attach's push option, tuning push for the host (``push=False`` serves
    none). max_stream_ms ends each stream gracefully before the host's
    request duration cap (the client reconnects with its cursor);
    max_heartbeat_ms asks Wakeshift for heartbeats at least this often, under
    a load balancer's idle timeout (Wakeshift grants between 10 and 45
    seconds); reauthorize_ms is how often an open stream asks the event's
    authorize (and identify) again, 5 minutes by default, between 1 and 50
    minutes."""

    max_stream_ms: int
    max_heartbeat_ms: int
    reauthorize_ms: int


@dataclass(frozen=True)
class _PushConfig:
    """attach's push option, read: off, or on with its limits."""

    on: bool
    max_stream_ms: int | None = None
    max_heartbeat_ms: int | None = None
    reauthorize_ms: int = 300_000


# Where the SDK can tell it runs on a host with a request duration cap, each
# stream ends this long after it opened, under the cap.
_HOST_MAX_STREAM_MS: tuple[tuple[str, int, str], ...] = (
    ("VERCEL", 280_000, "Vercel"),
    ("AWS_LAMBDA_FUNCTION_NAME", 14 * 60_000, "AWS Lambda"),
    ("K_SERVICE", 55 * 60_000, "Cloud Run"),
)


#: How often an open stream asks authorize again, by default, and its bounds (ms).
_REAUTHORIZE_MS = 300_000
_REAUTHORIZE_MIN_MS = 60_000
_REAUTHORIZE_MAX_MS = 50 * 60_000

#: How long an open stream goes on while identify or authorize fail (an
#: error, not a refusal) at its rechecks: once its last successful check (or
#: its open) is this old, a failed recheck ends it with the final result, and
#: the client reconnects with its cursor, and retries while the checks fail.
#: 15 minutes: three failed rechecks at the default interval.
_MAX_UNCHECKED_MS = 15 * 60_000


def _max_unchecked_ms(reauthorize_ms: int) -> int:
    """The bound for a stream rechecked every reauthorize_ms: at least
    _MAX_UNCHECKED_MS, and at least three intervals, so a longer interval
    still allows three failed rechecks. Over about 17 minutes the core's
    50-minute stream age ends the stream first."""
    return max(_MAX_UNCHECKED_MS, 3 * reauthorize_ms)


_PUSH_KEYS = ("max_stream_ms", "max_heartbeat_ms", "reauthorize_ms")


def push_config(wake: Wakeshift, push: PushOptions | Literal[False] | None, env: Mapping[str, str] | None = None) -> _PushConfig:
    """attach's push option, checked, with max_stream_ms defaulted by the host it runs on (env)."""
    if push is False:
        return _PushConfig(on=False)
    if push is not None and not isinstance(push, Mapping):
        raise usage(
            "config", "attach's push is False (no push), or {'max_stream_ms': …, 'max_heartbeat_ms': …, 'reauthorize_ms': …}"
        )
    o: Mapping[str, Any] = push or {}
    for k, v in o.items():
        if k not in _PUSH_KEYS:
            raise usage("config", f"attach's push has no {k}: its keys are max_stream_ms, max_heartbeat_ms and reauthorize_ms")
        if v is not None and (not isinstance(v, int) or isinstance(v, bool) or v <= 0):
            raise usage("config", f"attach's push {k} is a whole number of milliseconds over 0")
    max_stream = o.get("max_stream_ms")
    if max_stream is None:
        env = os.environ if env is None else env
        for var, ms, host in _HOST_MAX_STREAM_MS:
            if env.get(var):
                max_stream = ms
                wake._log(
                    "push_max_stream",
                    f"this server runs on {host}, so each events/stream ends after {ms // 1000} seconds and its client reconnects",
                )
                break
    reauth = o.get("reauthorize_ms")
    reauth = _REAUTHORIZE_MS if reauth is None else min(max(reauth, _REAUTHORIZE_MIN_MS), _REAUTHORIZE_MAX_MS)
    return _PushConfig(on=True, max_stream_ms=max_stream, max_heartbeat_ms=o.get("max_heartbeat_ms"), reauthorize_ms=reauth)


def _outbound(ctx: Any) -> Any:
    """The request-scoped channel of an mcp 2.x request context (the
    dispatch context), whose raw notify writes on the request's own response
    stream. Private on ServerSession (_request_outbound): the SDK pins the
    mcp versions it's tested on."""
    session = getattr(ctx, "session", None)
    out = getattr(session, "_request_outbound", None)
    return out if callable(getattr(out, "notify", None)) else None


def streams_for(ctx: Any) -> bool:
    """Whether a request's response is a stream (SSE, or stdio), so
    notifications sent before the result reach the client: not the
    StreamableHTTPSessionManager's json_response mode, which drops them. On
    2026-07-28 HTTP the request's channel has an SSE sink only when the
    response may stream; on earlier protocols its transport can send
    requests only when it streams. None (no request): the host's own
    setting, where it can be read."""
    out = _outbound(ctx)
    if out is None or getattr(ctx, "request_id", None) is None:
        return False
    if hasattr(out, "sink"):
        return out.sink is not None
    transport = getattr(out, "transport", None)
    return bool(getattr(transport, "can_send_request", True))


#: What an events/stream gets when identify or authorize failed (raised or
#: timed out) as it opened, which reconnects do: a retryable error (Internal
#: error, as when Wakeshift can't be reached), never Forbidden, which tells
#: the client to stop. The client retries, and the stream opens once the
#: app's check answers. A refusal is still Forbidden.
CHECK_FAILED = "this app couldn't check access to this event just now: try again shortly"


def check_failed(wake: Wakeshift, event: str) -> Exception:
    """The retryable error for an events/stream whose identify or authorize
    failed as it opened (CHECK_FAILED), logged as push_open_check_failed with
    the event's name only."""
    from mcp.shared.exceptions import MCPError

    wake._log(
        "push_open_check_failed",
        f"identify or authorize failed when an events/stream of {event} opened, so the client was answered with a "
        "retryable error",
        event=event,
    )
    return MCPError(INTERNAL_ERROR, CHECK_FAILED)


#: The message for a refusal Wakeshift sent without one of its own.
_UPSTREAM_FAILED = "push upstream failed"

#: The longest wait a 503's Retry-After gets before the client's stream ends, in seconds.
_MAX_RETRY_AFTER = 10.0


def _retry_after(v: str | None) -> float:
    """A Retry-After in seconds (0 when missing or not a number of seconds), capped."""
    try:
        s = float((v or "").strip())
    except ValueError:
        return 0.0
    if not math.isfinite(s) or s < 0:
        return 0.0
    return min(s, _MAX_RETRY_AFTER)


#: The final result of an events/stream: Wakeshift ended it, and the client
#: reconnects with its cursor (or, after terminated, stops).
STREAM_RESULT: dict[str, Any] = {"resultType": RESULT_TYPE}

# Before the first active frame says what Wakeshift granted, the grant is the spec's default.
_DEFAULT_HEARTBEAT_MS = 30_000


def _terminated_code(name: Any, codes: Codes) -> int:
    """A terminated frame's error name as the client's dialect's code."""
    return {
        "Forbidden": codes.forbidden,
        "NotFound": codes.not_found,
        "Unsupported": codes.unsupported,
        "ResourceExhausted": codes.resource_exhausted,
        "InvalidParams": INVALID_PARAMS,
    }.get(name if isinstance(name, str) else "", INTERNAL_ERROR)


def refusal_of(status: int, text: str, codes: Codes, event: str, wake: Wakeshift | None = None) -> Exception:
    """The client's JSON-RPC error for Wakeshift refusing an events/stream
    before it opens (status and body), in the client's dialect's codes, as
    events/poll maps them, with the limits' data."""
    from mcp.shared.exceptions import MCPError

    from ._events import _rpc_error_of, fill

    e: Mapping[str, Any] = {}
    try:
        data = json.loads(text)
        if isinstance(data, Mapping) and isinstance(data.get("error"), Mapping):
            e = data["error"]
    except ValueError:
        pass

    def string(k: str) -> str:
        v = e.get(k)
        return v if isinstance(v, str) else ""

    code = string("code")
    # Wakeshift's own message, never a body that isn't its JSON (a proxy's page).
    message = string("message")
    mx = e.get("max")
    if code in ("not_found", "unknown_event"):
        return MCPError(codes.not_found, message or fill("native_unknown_event", event=json.dumps(event)), {"kind": "event"})
    message = message or _UPSTREAM_FAILED
    if code in ("subscription_limit", "stream_limit"):
        limit = "streams" if code == "stream_limit" or e.get("limit") == "streams" else "subscriptions"
        out: dict[str, Any] = {"limit": limit}
        if isinstance(mx, int) and not isinstance(mx, bool):
            out["max"] = mx
        return MCPError(codes.resource_exhausted, message, out)
    if code == "unsupported":
        return MCPError(codes.unsupported, STREAM_REFUSAL, {"feature": "deliveryMode", "value": "push"})
    if code == "" and status >= 500:
        if wake is not None:
            wake._log(
                "push_upstream_failed",
                f"Wakeshift answered an events/stream with {status}, so the client was answered with an error",
                reason="status",
            )
        return MCPError(INTERNAL_ERROR, "Wakeshift couldn't be reached")
    return _rpc_error_of(APIError(message, code=code, status=status, reason=string("reason") or None), codes, event, "push")


@dataclass
class PushRun:
    """What events/stream relays for one request: everything already checked."""

    wake: Wakeshift
    ctx: Any
    event: str
    args: Any
    #: The call as Wakeshift takes it (poll's).
    call: dict[str, Any]
    voucher: str | None
    cursor: str | None
    max_age_ms: int | None
    #: The client's heartbeatMs, if it suggested one.
    heartbeat_ms: int | None
    codes: Codes
    config: _PushConfig
    #: The first active frame said the stream created its subscription.
    on_created: Callable[[Mapping[str, Any]], None]
    #: Asks authorize (and identify) again, every config.reauthorize_ms:
    #: True allowed, False refused (the stream ends as a revocation does),
    #: None couldn't tell (it raised or timed out: the stream goes on).
    #: None: nothing to ask again.
    reauthorize: Callable[[], Awaitable[bool | None]] | None = None
    #: The terminated message a refusal sends.
    refused_message: str = ""


#: What the reader hands on when the upstream ends, or breaks off.
#: events/stream's retryable error when Wakeshift couldn't be reached, or its
#: stream ended before notifications/events/active.
UNREACHABLE = "Wakeshift couldn't be reached"
#: events/stream's retryable error when Wakeshift is at its stream limit (its
#: 503), after Retry-After and jitter.
STREAM_BUSY = "Wakeshift can't open this stream just now: try again shortly"

_EOF = object()
_BROKEN = object()


async def relay_push(r: PushRun) -> dict[str, Any]:
    """Serves one events/stream: opens the upstream and relays it until
    either end closes. Returns the final result when Wakeshift (or the SDK,
    for silence or max_stream_ms) ends the stream; the client's abort
    cancels it, and nothing is answered. Raises the client's JSON-RPC error
    when the upstream refuses before it opens."""
    import anyio
    from mcp.shared.exceptions import MCPError

    wake, ctx = r.wake, r.ctx
    rid = ctx.request_id
    out = _outbound(ctx)
    assert out is not None

    def meta(own: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return {**(own or {}), SUBSCRIPTION_ID_META: rid}

    def final() -> dict[str, Any]:
        # The final StreamEventsResult carries the subscription ID too. Before
        # notifications/events/active, a stream that ends is an error instead:
        # SEP-3415 has a stream either refused at once or confirmed first. The
        # client retries (-32603), with its cursor.
        if first:
            raise MCPError(INTERNAL_ERROR, UNREACHABLE)
        return {**STREAM_RESULT, "_meta": meta()}

    # The heartbeat asked for: the client's suggestion, at most the app's max_heartbeat_ms.
    asked = r.heartbeat_ms
    if r.config.max_heartbeat_ms is not None:
        asked = r.config.max_heartbeat_ms if asked is None else min(asked, r.config.max_heartbeat_ms)
    body: dict[str, Any] = {
        "call": r.call,
        "name": r.event,
        "arguments": r.args,
        "cursor": r.cursor,
        "max_age_ms": r.max_age_ms,
        "heartbeat_ms": asked,
    }
    if r.voucher is not None:
        body["voucher"] = r.voucher
    cursor = r.cursor
    grant = asked or _DEFAULT_HEARTBEAT_MS
    ending = ""
    # No notifications/events/active relayed yet.
    first = True
    try:
        try:
            async with wake._open_stream("/events/stream", body) as up:
                if up.status == 503:
                    # The core is at its stream limit: the client's stream ends
                    # after Retry-After (at most 10 s) and up to 1 s of jitter, so
                    # clients don't reconnect straight into a full instance. The
                    # client's abort cancels the wait (and gets no result).
                    await anyio.sleep(_retry_after(up.retry_after) + random.random())
                    raise MCPError(INTERNAL_ERROR, STREAM_BUSY)
                if up.status >= 300:
                    try:
                        text = await up.text()
                    except Exception:
                        text = ""
                    raise refusal_of(up.status, text, r.codes, r.event, wake)
                start = anyio.current_time()
                loop_end = math.inf if r.config.max_stream_ms is None else start + r.config.max_stream_ms / 1000
                reauth_every = r.config.reauthorize_ms / 1000
                next_auth = math.inf if r.reauthorize is None else start + reauth_every
                # The last successful check: the open's, until a recheck succeeds.
                checked_at = start
                max_unchecked = _max_unchecked_ms(r.config.reauthorize_ms) / 1000
                last = start
                # One frame at a time: the reader waits until the relay took
                # the last one, so the SDK buffers at most one per stream.
                frames: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)

                async def read() -> None:
                    try:
                        async for ln in up.lines():
                            await frames.put(ln)
                        await frames.put(_EOF)
                    except Exception:
                        await frames.put(_BROKEN)

                reader = asyncio.ensure_future(read())
                try:
                    while True:
                        now = anyio.current_time()
                        if now >= loop_end:
                            ending = "max_stream"
                            return final()
                        if now >= next_auth and r.reauthorize is not None:
                            allowed = await r.reauthorize()
                            if allowed is True:
                                checked_at = now
                            elif allowed is None:
                                # Failed, not refused: the stream goes on, until
                                # its last successful check is max_unchecked old.
                                wake._log(
                                    "push_recheck_failed",
                                    f"identify or authorize failed when an open events/stream of {r.event} was checked "
                                    "again, so the stream goes on",
                                    event=r.event,
                                )
                                if anyio.current_time() - checked_at >= max_unchecked:
                                    wake._log(
                                        "push_recheck_stale",
                                        f"an open events/stream of {r.event} went {round(max_unchecked / 60)} minutes "
                                        "without a successful check of identify and authorize, so it was ended for its "
                                        "client to reconnect",
                                        event=r.event,
                                    )
                                    ending = "unchecked"
                                    return final()
                            if allowed is False:
                                # Refused now: ended as a revocation is. A
                                # reconnect asks authorize again, and is refused there.
                                ending = "reauthorize"
                                error = {
                                    "code": r.codes.forbidden,
                                    "message": r.refused_message,
                                    "data": {"reason": "revoked"},
                                }
                                if first:
                                    raise MCPError(error["code"], error["message"], error["data"])
                                await out.notify("notifications/events/terminated", {"error": error, "_meta": meta()})
                                return final()
                            next_auth = anyio.current_time() + reauth_every
                            continue
                        silent_at = last + 3 * grant / 1000
                        if now >= silent_at:
                            wake._log(
                                "push_upstream_failed",
                                "Wakeshift's events/stream went silent for three heartbeat intervals, so the client's "
                                "stream was ended for it to reconnect",
                                reason="silent",
                            )
                            return final()
                        got: Any = None
                        with anyio.move_on_after(min(silent_at, loop_end, next_auth) - now):
                            got = await frames.get()
                        if got is None:
                            continue
                        if got is _EOF:
                            # The upstream ended without close: the client reconnects with its cursor.
                            wake._log(
                                "push_upstream_failed",
                                "Wakeshift's events/stream ended without closing, so the client's stream was ended for it "
                                "to reconnect",
                                reason="ended",
                            )
                            return final()
                        if got is _BROKEN:
                            raise ConnectionError("the upstream broke off")
                        last = anyio.current_time()
                        line: str = got
                        try:
                            frame = json.loads(line)
                        except ValueError:
                            frame = {}
                        if not isinstance(frame, dict):
                            frame = {}
                        kind = frame.get("type")
                        if kind == "active":
                            hb = frame.get("heartbeat_ms")
                            if isinstance(hb, int) and not isinstance(hb, bool) and hb > 0:
                                grant = hb
                            if isinstance(frame.get("cursor"), str):
                                cursor = frame["cursor"]
                            if first and frame.get("created") is True:
                                r.on_created(frame)
                            first = False
                            params: dict[str, Any] = {"cursor": frame.get("cursor"), "truncated": bool(frame.get("truncated"))}
                            if isinstance(hb, int) and not isinstance(hb, bool):
                                params["heartbeatMs"] = hb
                            await out.notify("notifications/events/active", {**params, "_meta": meta()})
                        elif kind == "event":
                            o = frame.get("occurrence")
                            o = o if isinstance(o, dict) else {}
                            if isinstance(o.get("cursor"), str):
                                cursor = o["cursor"]
                            own = o.get("_meta") if isinstance(o.get("_meta"), Mapping) else None
                            await out.notify("notifications/events/event", {**o, "_meta": meta(own)})
                        elif kind == "heartbeat":
                            if isinstance(frame.get("cursor"), str):
                                cursor = frame["cursor"]
                            await out.notify(
                                "notifications/events/heartbeat", {"cursor": frame.get("cursor", cursor), "_meta": meta()}
                            )
                        elif kind == "terminated":
                            e = frame.get("error")
                            e = e if isinstance(e, dict) else {}
                            error: dict[str, Any] = {
                                "code": _terminated_code(e.get("name"), r.codes),
                                "message": e.get("message") if isinstance(e.get("message"), str) else "",
                            }
                            if "data" in e:
                                error["data"] = e["data"]
                            if first:
                                # Before active: the stream's refusal, as its JSON-RPC error.
                                raise MCPError(error["code"], error["message"], error.get("data"))
                            await out.notify("notifications/events/terminated", {"error": error, "_meta": meta()})
                            return final()
                        elif kind == "close":
                            return final()
                        # A frame this SDK doesn't know: skipped, so a newer core can add one.
                finally:
                    reader.cancel()
        except APIConnectionError:
            wake._log(
                "push_upstream_failed",
                "an events/stream couldn't reach Wakeshift, so the client was answered with an error",
                reason="unreachable",
            )
            raise MCPError(INTERNAL_ERROR, UNREACHABLE) from None
    except anyio.get_cancelled_exc_class():
        # The client's abort. An explicit cancel ends the subscription; over
        # HTTP an abort can't be told from a dropped connection, so the
        # subscription lapses on its own.
        cancel = getattr(out, "cancel_requested", None)
        explicit = cancel is not None and cancel.is_set()
        if explicit and cursor is not None and not _over_http(ctx):
            with anyio.CancelScope(shield=True), anyio.move_on_after(5):
                await _end_subscription(r, cursor)
        raise
    except MCPError:
        raise
    except Exception:
        # The upstream broke off, or the client's stream couldn't be written:
        # the client reconnects with its cursor.
        if not ending:
            wake._log(
                "push_upstream_failed",
                "Wakeshift's events/stream broke off, so the client's stream was ended for it to reconnect",
                reason="broken",
            )
        return final()


def _over_http(ctx: Any) -> bool:
    from ._client import _request_info

    _, _, headers = _request_info(ctx)
    return bool(headers)


async def _end_subscription(r: PushRun, cursor: str) -> None:
    """Ends the stream's subscription after the client's explicit cancel
    (stdio): Wakeshift audits it as unsubscribed."""
    try:
        await r.wake._request(
            "POST", "/events/stream/end", {"call": r.call, "name": r.event, "arguments": r.args, "cursor": cursor}
        )
    except Exception:
        r.wake._log(
            "push_upstream_failed",
            "ending a cancelled events/stream's subscription failed, so it lapses on its own",
            reason="end_failed",
        )
