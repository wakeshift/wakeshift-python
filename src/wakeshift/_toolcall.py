"""Tools that subscribe (docs/subscribe-from-tools.md). A tool registered
with ``@attached.tool(subscribes=[...])``, or registered elsewhere and named in
attach's ``wrap``, gets a :class:`ToolCall` (a parameter, or
``wake.current_call()``) and subscribes the calling conversation with
``await call.subscribe(event, args)``. Attach works at the protocol layer: its
middleware takes Wakeshift's arguments out of every tool call before the host
or any vendor middleware sees them, adds them to tools/list, serves attach's
own tools, and adds one Wakeshift block to whatever a tool that subscribes
returns, so a conversation can't be left unwakeable by a forgotten argument
or an unreturned result; a call that fails ends the subscriptions it made::

    attached = wake.attach(server, identify=identify)

    @attached.tool(subscribes=[turn])
    async def join(table: str, call: wakeshift.ToolCall) -> str:
        \"\"\"Take a seat at a table.\"\"\"
        sub = await call.subscribe(turn, {"table": table}, reason=f"Playing at {table}")
        await casino.seat(call.user, table, sub.id)
        return f"You're seated at {table}."
"""

# No `from __future__ import annotations`: the wrapper's annotations are
# evaluated objects MCPServer reads at runtime.

import asyncio
import contextvars
import copy
import dataclasses
import inspect
import json
import re
import typing
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypeVar

from ._client import (
    CONVERSATION_ARG,
    HARNESS_ARG,
    SESSION_ARG,
    SUBSCRIBE_VOUCHERS,
    Recognized,
    _call_of,
    _client_capabilities,
    _request_info,
    _takes_tasks,
    _transport_session,
)
from ._errors import UsageError, usage
from ._log import logger, warn
from ._secrets import SessionKey, _reveal, _session
from ._spec import EXTENSION_KEY, TOP_LEVEL_KEY, bridged_client, dialect_of
from ._text import WAKESHIFT_FIELD_SCHEMA, wake_args

if TYPE_CHECKING:
    from ._client import Wakeshift
    from ._events import Identity, Request, WakeEvent
    from ._hosts import Host

    Identify = Callable[[Request], "Identity | None | Awaitable[Identity | None]"]
    Wrap = Mapping[str, Sequence[WakeEvent[Any, Any]]] | Callable[[str], Sequence[WakeEvent[Any, Any]] | None]

_F = TypeVar("_F", bound=Callable[..., Any])
_A = TypeVar("_A")

SubscriptionState = Literal["created", "renewed", "native", "unavailable", "disabled"]
UnavailableReason = Literal["client_unsupported", "no_user", "refused"]

_DURATION = re.compile(r"^(\d+(?:\.\d+)?)([smhd])$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
#: replay's bound: at most 24 hours back.
MAX_REPLAY = 86400


def _seconds(name: str, value: "timedelta | int | float | str | None", *, most: float | None = None) -> int | float | None:
    """A duration as seconds (an int when whole): a timedelta, seconds, or a
    string like "30m" (s, m, h or d); above zero, and at most most seconds."""
    if value is None:
        return None
    if isinstance(value, bool):
        secs = None
    elif isinstance(value, timedelta):
        secs = value.total_seconds()
    elif isinstance(value, int | float):
        secs = float(value)
    elif isinstance(value, str) and (m := _DURATION.match(value)):
        secs = float(m.group(1)) * _UNITS[m.group(2)]
    else:
        secs = None
    if secs is None:
        raise usage("bad_arguments", f'{name} is a timedelta, a number of seconds, or a string like "30m" (s, m, h or d)')
    if secs <= 0 or (most is not None and secs > most):
        bound = " and at most 24 hours" if most == MAX_REPLAY else ""
        raise usage("bad_arguments", f"{name} is more than zero{bound}")
    return int(secs) if secs.is_integer() else secs


#: The line a text-carried beacon is on (the core's beacon.Text), which a
#: harness's hook reads only as the last line of a result's text.
BEACON_LINE_PREFIX = "(For Wakeshift's hook on this computer, not for you: wakeshift-beacon: "

#: The tools/call _meta keys that carry a harness's conversation (the core's
#: harnesses ConversationMeta, dotted for nested objects;
#: sdk/typescript/test/conversation-meta.json, which a Go test keeps current).
CONVERSATION_META = (
    "agent-session-id",
    "antigravity.google/conversation_id",
    "vscode.conversationId",
    "wakeshift/conversation",
    "x-codex-turn-metadata.thread_id",
)

#: The connector beacon's _meta key.
CONNECTOR_META_KEY = "wakeshift/connector"

_MAX_NATIVE_LINES = 20


#: The structuredContent field a wrapped tool's structured result carries
#: Wakeshift's block in (without a beacon line), for hosts that show the
#: model only structuredContent. The three SDKs use the same name.
WAKESHIFT_FIELD = "wakeshift"


def with_wakeshift_field(schema: Any) -> Any:
    """An object output schema with an optional ``wakeshift`` string added
    (a copy; the schema itself is left alone), so a client that checks
    structuredContent against it takes the block. Anything else, or a schema
    that already names it, comes back as it was. The shared fixture
    sdk/typescript/test/wrapped-block.json (output_schemas) pins it."""
    if not isinstance(schema, Mapping) or schema.get("type") != "object":
        return schema
    props = schema.get("properties")
    if props is not None and not isinstance(props, Mapping):
        return schema
    if props is not None and WAKESHIFT_FIELD in props:
        return schema
    return {
        **schema,
        "properties": {**(props or {}), WAKESHIFT_FIELD: dict(WAKESHIFT_FIELD_SCHEMA)},
    }


@dataclass(frozen=True)
class Subscription:
    """What ``call.subscribe`` did, by ``state``:

    - ``created``, ``renewed``: a conversation subscription, new or renewed;
      ``id`` and ``user`` address it later (an emit's
      ``Recipient(user=sub.user, subscription=sub.id)``, ``event.end``).
    - ``native``: the client subscribes itself through MCP Events; the
      block tells it what to subscribe to.
    - ``unavailable``: nothing happened; ``reason`` is ``client_unsupported``
      (an event only the app's tools subscribe to, and a client that
      subscribes only itself), ``no_user`` (a native client and no
      signed-in user) or ``refused`` (the event's authorize refused).
    - ``disabled``: the client is disabled."""

    state: SubscriptionState
    id: str = ""
    user: str | None = None
    #: The conversation's session: a SessionKey, which never prints.
    session: SessionKey | None = None
    #: What Wakeshift recognized the agent's program as.
    recognized: Recognized | None = None
    reason: UnavailableReason | None = None


def _meta_string(meta: Mapping[str, Any], key: str) -> str:
    """meta's string at key: the full key first, then the dotted path (the
    core's metaString)."""
    v = meta.get(key)
    if isinstance(v, str):
        return v if len(v) <= 200 else ""
    cur: Any = meta
    parts = key.split(".")
    for i, p in enumerate(parts):
        if not isinstance(cur, Mapping):
            return ""
        if i == len(parts) - 1:
            s = cur.get(p)
            return s if isinstance(s, str) and len(s) <= 200 else ""
        cur = cur.get(p)
    return ""


def conversation_key(meta: Mapping[str, Any] | None) -> str:
    """The conversations a harness named in a tool call's _meta, joined ("" for none)."""
    if not meta:
        return ""
    return "|".join(x for x in (_meta_string(meta, k) for k in CONVERSATION_META) if x)


def _same_json(a: Any, b: Any) -> bool:
    return (a or {}) == (b or {})


# ── The block ─────────────────────────────────────────────────────────────


def _drop_trailers(result: dict[str, Any]) -> None:
    """Takes every beacon line off the end of result's last text (dropping
    its block if nothing else is in it). Only Wakeshift's own trailer may end
    a wrapped tool's text: a hook acts on the beacon there, and one in the
    app's text may be someone else's, relayed (a chat message carrying a
    beacon minted for another session)."""
    while True:
        content = list(result.get("content") or [])
        i = len(content) - 1
        while i >= 0 and not (isinstance(content[i], dict) and content[i].get("type") == "text"):
            i -= 1
        if i < 0:
            return
        block = content[i]
        text = str(block.get("text", "")).rstrip(" \t\r\n")
        rest, line = "", text
        j = text.rfind("\n")
        if j >= 0:
            rest, line = text[:j], text[j + 1 :]
        if not line.startswith(BEACON_LINE_PREFIX) or not line.endswith(")"):
            return
        rest = rest.rstrip(" \t\r\n")
        if not rest:
            del content[i]
        else:
            content[i] = {**block, "text": rest}
        result["content"] = content


def merge_block(
    result: dict[str, Any],
    text: str,
    trailer: str = "",
    meta: Mapping[str, Any] | None = None,
    *,
    string: bool = False,
) -> dict[str, Any]:
    """A tool result (the wire form) with Wakeshift's block added: its text as
    a block after the app's content, then a text-carried beacon last, its
    _meta merged. Beacon lines ending the app's own text are dropped
    (_drop_trailers), whether or not there's a block to add. A result whose
    own _meta carries a connector beacon gets no second one, in _meta or its
    text. ``string``: the tool returned a string, which stays
    one text block. A structured result (structuredContent an object) gets
    the text in its ``wakeshift`` field too. The shared fixture
    sdk/typescript/test/wrapped-block.json pins it."""
    out = dict(result)
    out["content"] = list(out.get("content") or [])
    text = (text or "").strip()
    own_meta = out.get("_meta") if isinstance(out.get("_meta"), dict) else None
    meta_beacon = own_meta is not None and CONNECTOR_META_KEY in own_meta
    _drop_trailers(out)
    if text:
        out["content"].append({"type": "text", "text": text})
    if not meta_beacon and trailer:
        out["content"].append({"type": "text", "text": trailer})
    for k, v in (meta or {}).items():
        if k == CONNECTOR_META_KEY and meta_beacon:
            continue
        merged = dict(out.get("_meta") or {})
        merged[k] = v
        out["_meta"] = merged
    if string and all(isinstance(c, dict) and c.get("type") == "text" for c in out["content"]):
        joined = "\n\n".join(str(c.get("text", "")) for c in out["content"] if c.get("text"))
        out["content"] = [{"type": "text", "text": joined}] if joined else []
    if text and isinstance(out.get("structuredContent"), dict):
        out["structuredContent"] = {**out["structuredContent"], WAKESHIFT_FIELD: text}
    return out


# ── The call and its scope ────────────────────────────────────────────────

_SENTINEL = object()

#: The tools/call scope attach's middleware makes for every tool call.
_SCOPE: contextvars.ContextVar["_Scope | None"] = contextvars.ContextVar("wakeshift_tool_scope", default=None)


#: The built-ins the server's own tools/list handler added in this request
#: (Attachment._inject_builtins), for attach's middleware outside it.
_LISTED: contextvars.ContextVar["set[str] | None"] = contextvars.ContextVar("wakeshift_listed", default=None)

#: The call testing.with_call makes current, and for which client.
_TEST_CALL: contextvars.ContextVar["tuple[Wakeshift, ToolCall] | None"] = contextvars.ContextVar(
    "wakeshift_test_call", default=None
)


class _Asked:
    """One request attach's middleware is answering: identify's answer for it,
    shared by everything that needs it."""

    __slots__ = ("attached", "future")

    def __init__(self, attached: "Attachment") -> None:
        self.attached = attached
        self.future: asyncio.Future[Any] | None = None


_ASKED: contextvars.ContextVar["_Asked | None"] = contextvars.ContextVar("wakeshift_asked", default=None)


def _token_of(rc: Any) -> Any:
    """The access token the server's auth verified for this HTTP request (its
    scope's user), else the MCP SDK's for the current context."""
    req = getattr(rc, "request", None)
    scope = getattr(req, "scope", None)
    user = scope.get("user") if isinstance(scope, Mapping) else None
    tok = getattr(user, "access_token", None)
    if tok is not None:
        return tok
    try:
        from mcp.server.auth.middleware.auth_context import get_access_token
    except ImportError:  # pragma: no cover
        return None
    return get_access_token()


#: The characters of a tool's description Claude Code keeps.
_MAX_DESCRIPTION = 2048

#: Wakeshift's three tool arguments.
WAKE_ARGS = (SESSION_ARG, HARNESS_ARG, CONVERSATION_ARG)

#: The tools attach serves itself, whose names no other tool may take.
BUILTIN_NAMES = ("subscribe_events", "unsubscribe_events", "list_subscriptions", "wakeshift_wait")


@dataclass
class _Created:
    name: str
    arguments: dict[str, Any]
    session: SessionKey | None
    #: The call's pending-addition record, which made it.
    pending: str = ""
    #: The event, and its arguments as parsed, for its on_subscribe.
    event: Any = None
    parsed: Any = None
    #: call.subscribe's context=, and the contextvars it ran in: on_subscribe gets the one, runs in the other.
    context: Any = field(default=None, repr=False)
    vars: contextvars.Context | None = field(default=None, repr=False)


class _Scope:
    """One tools/call as attach's middleware sees it: Wakeshift's arguments it
    took out of the call, and, for a tool that subscribes, its ToolCall and
    what it subscribed, for the block and the rollback."""

    def __init__(self, attached: "Attachment", rc: Any, tool: str, wake_args: dict[str, Any]) -> None:
        self.attached = attached
        #: The request's context, as the tool's handler gets it.
        self.rc = rc
        #: The tools/call's tool name, as the client called it.
        self.tool = tool
        #: session_key, harness and conversation_id, taken out of the arguments.
        self.wake_args = wake_args
        self.call: ToolCall | None = None
        #: A registered tool's wrapper has run in this scope.
        self.entered = False
        self.string = False
        #: The wrapped tool's output check, for a framework that doesn't
        #: check a result against the output schema itself (FastMCP).
        self.check: Callable[[Any], str | None] | None = None
        self.lock = asyncio.Lock()
        #: The loop the call is answered on: its subscribes run there.
        self.loop = asyncio.get_running_loop()
        self.busy = 0
        #: Set while nothing is in flight.
        self.idle = asyncio.Event()
        self.idle.set()
        self.ended = False
        self.pending = ""
        self.armed = False
        self.created: list[_Created] = []
        #: The call's subscriptions still live (created or renewed, and not
        #: unsubscribed in the same call), by event and canonical arguments.
        self.live: set[tuple[str, str]] = set()
        #: What each of the call's subscribes came back as, by event and
        #: canonical arguments (a watch skips an unavailable one).
        self.states: dict[tuple[str, str], str] = {}
        #: Watches started in the call for subscriptions it created: a rollback stops them.
        self.watches: list[Any] = []
        self.native: list[dict[str, Any]] = []
        #: call.subscribe's context= for a tools-only event's native line, by
        #: event and canonical arguments: kept with the voucher the line hands
        #: out (never sent: the lines above are).
        self.native_contexts: dict[tuple[str, str], Any] = {}
        #: Wakeshift's notes from the call's unsubscribes (one that ended
        #: nothing because the call passed no session key), for the block.
        self.notes: list[str] = []

    @property
    def wrapped(self) -> bool:
        return self.call is not None

    async def end(self) -> None:
        """Closes the call to further requests, after waiting for any in
        flight (logged once per tool: the tool should have awaited them)."""
        await asyncio.sleep(0)  # tasks the tool started begin
        if self.busy > 0 and self.call is not None:
            self.attached._wake._log(
                "subscribe_in_flight",
                f"{self.call.tool} returned while a subscribe was in flight, so it was waited for",
                tool=self.call.tool,
            )
        await self.idle.wait()
        self.ended = True

    def acquire_busy(self) -> None:
        self.busy += 1
        self.idle.clear()

    def release_busy(self) -> None:
        self.busy -= 1
        if self.busy == 0:
            self.idle.set()

    def add_native(self, name: str, args: dict[str, Any], options: Mapping[str, Any] | None = None) -> None:
        """A native line, once per event and arguments. options: call.subscribe's
        once, ttl and replay, for a tools-only event's line: the core keeps them
        with the subscribe voucher and applies them when it's redeemed."""
        key = json.dumps(args, sort_keys=True, separators=(",", ":"))
        for line in self.native:
            if line["name"] == name and json.dumps(line["arguments"], sort_keys=True, separators=(",", ":")) == key:
                return
        if len(self.native) >= _MAX_NATIVE_LINES:
            logger.debug("wakeshift: a call subscribed a native client to more than 20 events; only 20 are named")
            return
        self.native.append({"name": name, "arguments": args, **(options or {})})


class ToolCall:
    """One call of a tool that subscribes (registered with ``@attached.tool``,
    or named in attach's ``wrap`` and found with ``wake.current_call()``):
    who's calling, and the calls that subscribe the calling conversation.
    Only the SDK makes one."""

    def __init__(
        self,
        _token: object,
        *,
        scope: "_Scope | None",
        context: Any,
        user: str | None,
        tool: str,
        native: bool,
        native_conversation: str | None,
        session: SessionKey | None,
        harness: str | None,
        conversation: str | None,
        client: str | None = None,
        declared: "list[WakeEvent[Any, Any]] | None" = None,
        checked: bool = False,
        wake: "Wakeshift | None" = None,
    ) -> None:
        if _token is not _SENTINEL:
            raise TypeError("wakeshift: only the SDK makes a ToolCall: take it as a tool's parameter, or wake.current_call()")
        #: None for a disabled client's call, which asks Wakeshift nothing.
        self._scope = scope
        self._wake = wake if wake is not None else scope.attached._wake if scope is not None else None
        #: The tool's Context: MCPServer's or FastMCP's, or a low-level
        #: handler's ServerRequestContext.
        self.context = context
        #: Who attach's identify says is calling (None for none).
        self.user = user
        #: The tool being called.
        self.tool = tool
        #: The client subscribes to the app's events itself (MCP Events:
        #: ChatGPT, an app's house agents): subscribe then makes no
        #: conversation subscription, and the block tells it what to subscribe to.
        self.native = native
        #: A native client's conversation, when it names one (ChatGPT's
        #: _meta["openai/session"]).
        self.native_conversation = native_conversation
        #: The conversation's session, set once Wakeshift resolved the
        #: conversation (a subscribe that reached it, or resolve_session);
        #: never the raw session_key argument, which a caller can forge. A
        #: SessionKey: it never prints.
        self.session: SessionKey | None = None
        # The session_key the call passed: sent to Wakeshift, never shown.
        self._passed: SessionKey | None = session
        #: What Wakeshift recognized the agent's program as: None until a
        #: subscribe or resolve_session.
        self.recognized: Recognized | None = None
        self._harness = harness
        self._conversation = conversation
        # The events the tool declares (None: none declared), and whether its
        # subscribes are checked against them (a low-level call can't declare).
        self._declared = declared
        self._checked = checked
        #: Why Wakeshift's part of this call is refused (identify failed, or
        #: require_user and no user), if it is.
        self._refused: str | None = None
        rc = getattr(context, "request_context", context)
        named, meta, headers = _request_info(rc)
        # The request's client name, else identify's.
        self._client = named or client or ""
        self._meta = meta
        self._ua = headers.get("user-agent", "") if headers else ""
        self._headers = headers
        self._tasks = _takes_tasks(meta, headers)

    def __repr__(self) -> str:
        return f"ToolCall(tool={self.tool!r}, user={self.user!r}, native={self.native!r})"

    def _wire(self, session: SessionKey | None = None) -> dict[str, Any]:
        """The call as Wakeshift takes it."""
        c = _call_of(
            client_name=self._client,
            meta=self._meta,
            user=self.user,
            session=_reveal(session if session is not None else self.session or self._passed),
            user_agent=self._ua,
            harness=self._harness,
            conversation=self._conversation,
            tool=self.tool,
            tasks=self._tasks,
            # A native caller's transport session binds an anonymous voucher;
            # only to a core that hands them out (an older one refuses fields
            # it doesn't know).
            transport_session=_transport_session(self._headers) if self.native and self._vouchers_known() else None,
        )
        return c.to_json()

    def _vouchers_known(self) -> bool:
        wake = self._wake
        return wake is not None and SUBSCRIBE_VOUCHERS in wake._features

    def _check_identity(self) -> None:
        if self._refused == "failed":
            raise usage("identify_failed", f"identify failed for this call of {self.tool}, so it can't subscribe")
        if self._refused == "anonymous":
            raise usage("user_required", f"require_user refused this call of {self.tool}: identify gave no user")

    async def _enter(self) -> _Scope:
        s = self._scope
        if s is None:
            raise usage("disabled", "Wakeshift is off (no app key), so this call can't ask it anything")
        self._check_identity()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not s.loop:
            raise usage(
                "no_call",
                f"{self.tool or 'the tool'} awaited its call off the event loop answering it (in a worker thread, say): "
                "subscribe from an async def tool",
            )
        s.acquire_busy()
        try:
            await s.lock.acquire()
        except BaseException:
            s.release_busy()
            raise
        if s.ended:
            s.lock.release()
            s.release_busy()
            raise usage("call_ended", f"{self.tool or 'the tool'} already returned, so its call can't subscribe any more")
        return s

    def _leave(self, s: _Scope) -> None:
        s.lock.release()
        s.release_busy()

    def _check_event(self, event: "WakeEvent[Any, Any]") -> None:
        if getattr(event, "_wake", None) is not self._wake:
            raise usage("unknown_event", f"{getattr(event, 'name', event)} is another app's event: this app doesn't offer it")

    def _check_declared(self, event: "WakeEvent[Any, Any]") -> None:
        """Raises UsageError undeclared_subscribe when the tool doesn't declare event."""
        if not self._checked:
            return
        d = self._declared
        if d is not None and any(e is event or e.name == event.name for e in d):
            return
        raise usage("undeclared_subscribe", f"{self.tool} subscribed to {event.name}, which isn't in its subscribes")

    async def resolve_session(self) -> SessionKey | None:
        """Finds or starts the calling conversation's session without
        subscribing it to anything (it doesn't make the conversation
        wakeable), and returns it (a SessionKey, which never prints). None,
        with no request to Wakeshift, when nothing names the conversation:
        no session_key, no conversation_id and no conversation in the call's
        _meta (a Codex thread)."""
        s = await self._enter()
        try:
            if s.pending and self.session:
                return self.session
            wire = self._wire()
            meta = wire.get("meta")
            if not (
                wire.get("session")
                or wire.get("conversation")
                or conversation_key(meta)
                or (meta and _meta_string(meta, "threadId"))
            ):
                # Nothing names the conversation: Wakeshift would only start a new one.
                return None
            out = await s.attached._wake._request_app("POST", "/calls/session", {"call": wire})
            s.pending = out.get("pending") or ""
            self.session = _session(out.pop("session", None))
            self.recognized = _recognized(out.get("agent"))
            return self.session
        finally:
            self._leave(s)

    async def subscribe(
        self,
        event: "WakeEvent[_A, Any]",
        args: "_A | Mapping[str, Any]",
        *,
        once: bool = False,
        replay: timedelta | int | float | str | None = None,
        ttl: timedelta | int | float | str | None = None,
        reason: str | None = None,
        label: str | None = None,
        context: Any = None,
    ) -> Subscription:
        """Subscribes the calling conversation to event with args, or renews
        its subscription. The tool's reply then carries what the conversation
        needs to be woken (one block for the call, however many subscribes it
        makes), and if the call fails, the subscription is ended. event is one
        the tool declares (``subscribes``). Your tool decides who may: authorize
        isn't consulted for a conversation, so a tool that subscribes to a
        resource the caller names checks the caller may see it. A personal
        event needs a user (identify's).

        A client that subscribes itself (MCP Events) gets no conversation
        subscription: the block tells it what to subscribe to, if the event
        is open to it (personal, authorize or public), the caller is signed
        in, and the event's authorize allows; otherwise the result is
        ``state="unavailable"`` with the reason.

        once: ends the subscription after its first delivery. replay
        (a timedelta, seconds or "5m"; at most 24 hours): also delivers the
        latest occurrence with these arguments from that far back, so one
        emitted just before the subscribe isn't missed. ttl: ends it after
        that long without a delivery or a renewal. reason: shown in the
        console. label: names a new session, for an app acting for its agents.
        context: any value of yours (the caller's credential, say) for the
        event's on_subscribe, as ``Subscribed.context``: never sent to
        Wakeshift, never logged. on_subscribe runs, after the call succeeds,
        in a copy of the contextvars this subscribe ran in."""
        self._check_event(event)
        self._check_declared(event)
        a_obj, a_json = event._parse_args(args)
        replay_s = _seconds("replay", replay, most=MAX_REPLAY)
        ttl_s = _seconds("ttl", ttl)
        if self._scope is None:
            return Subscription(state="disabled")  # Wakeshift is off: the local checks, and nothing else
        self._check_identity()
        run_in = contextvars.copy_context()
        s = await self._enter()
        try:
            if self.native:
                options: dict[str, Any] = {}
                if once:
                    options["once"] = True
                if ttl_s is not None:
                    options["ttl"] = ttl_s
                if replay_s is not None:
                    options["replay"] = replay_s
                return await self._native(s, event, a_obj, a_json, options, context)
            if event.personal and not self.user:
                raise usage(
                    "bad_arguments",
                    f"{event.name} is personal: subscribe needs a user (attach's identify), who the subscription belongs to",
                )
            body: dict[str, Any] = {"call": self._wire(), "name": event.name, "arguments": a_json, "defer_addition": True}
            if s.pending:
                body["pending"] = s.pending
            if ttl_s is not None:
                body["ttl"] = ttl_s
            if replay_s is not None:
                body["replay"] = replay_s
            if once:
                body["once"] = True
            if reason:
                body["reason"] = reason
            if label:
                body["label"] = label
            out = await s.attached._wake._request_app("POST", "/event-subscriptions", body)
            if out.get("pending"):
                s.pending = out["pending"]
            s.armed = True
            self.session = _session(out.pop("session", None))
            rec = _recognized(out.get("agent"))
            self.recognized = rec
            es = out.get("event_subscription") or {}
            created = bool(es.get("created"))
            if created:
                s.created.append(
                    _Created(event.name, a_json, self.session, out.get("pending") or "", event, a_obj, context, run_in)
                )
            else:
                # Renewed in the same call: on_subscribe gets the latest context.
                for c in s.created:
                    if _live_key(c.name, c.arguments) == _live_key(event.name, a_json):
                        c.context, c.vars = context, run_in
            s.live.add(_live_key(event.name, a_json))
            s.states[_live_key(event.name, a_json)] = "created" if created else "renewed"
            return Subscription(
                state="created" if created else "renewed",
                id=es.get("id", ""),
                user=es.get("user") or self.user,
                session=self.session,
                recognized=rec,
            )
        finally:
            self._leave(s)

    async def _native(
        self,
        s: "_Scope",
        event: "WakeEvent[Any, Any]",
        a_obj: Any,
        a_json: dict[str, Any],
        options: Mapping[str, Any] | None = None,
        context: Any = None,
    ) -> Subscription:
        """A subscribe for a client that subscribes itself: a native line in
        the block, after the event's own checks, the same ones the client's
        follow-up events/subscribe meets (SEC M5)."""
        from ._events import Subscriber

        key = _live_key(event.name, a_json)
        s.states[key] = "unavailable"
        if not s.attached._serves_events:
            return self._unavailable(event, "client_unsupported")
        if event.access == "tools":
            # While the core hands out subscribe vouchers, the line carries one,
            # so the client subscribes with what the tool gave it, signed in or
            # not (docs/subscribe-vouchers.md §7.1).
            if not await s.attached._wake._vouchers_offered():
                return self._unavailable(event, "client_unsupported")
            s.states[key] = "native"
            s.add_native(event.name, a_json, options)
            if context is not None:
                s.native_contexts[key] = context
            return Subscription(state="native", user=self.user or "", recognized=self.recognized)
        if not self.user:
            return self._unavailable(event, "no_user")
        if event._authorize is not None and not await event._allows(
            Subscriber(user=self.user, client=self._client, native=True, request=s.attached._request(s.rc)), a_obj
        ):
            return self._unavailable(event, "refused")
        s.states[key] = "native"
        s.add_native(event.name, a_json)
        return Subscription(state="native", user=self.user, recognized=self.recognized)

    def _unavailable(self, event: "WakeEvent[Any, Any]", reason: UnavailableReason) -> Subscription:
        assert self._wake is not None
        self._wake._log(
            "native_unavailable",
            f"{self.tool} couldn't subscribe this MCP Events client to {event.name} ({reason})",
            tool=self.tool,
            event=event.name,
            reason=reason,
        )
        return Subscription(state="unavailable", reason=reason)

    async def unsubscribe(self, event: "WakeEvent[_A, Any]", args: "_A | Mapping[str, Any]") -> bool:
        """Ends the calling conversation's subscription to event with args,
        reporting whether one ended. For a native caller it ends the user's
        own (webhook) subscriptions to the event with those arguments."""
        if self._scope is None:
            self._check_event(event)
            event._parse_args(args)
            return False
        s = await self._enter()
        try:
            self._check_event(event)
            _, a_json = event._parse_args(args)
            wake = s.attached._wake
            if self.native:
                if not self.user:
                    return False
                listed = await wake._request_app("POST", "/event-subscriptions/list", {"user": self.user})
                ended = False
                for sub in listed.get("subscriptions") or []:
                    if (
                        sub.get("event") != event.name
                        or sub.get("destination") != "webhook"
                        or not _same_json(sub.get("arguments"), a_json)
                    ):
                        continue
                    out = await wake._request_app("POST", "/event-subscriptions/end", {"user": self.user, "id": sub.get("id")})
                    ended = ended or bool(out.get("ended"))
                return ended
            out = await wake._request_app(
                "POST", "/event-subscriptions/unsubscribe", {"call": self._wire(), "name": event.name, "arguments": a_json}
            )
            # addition: the call passed no session key, so it ended nothing;
            # the block says to call again with it.
            add = out.get("addition")
            note = str(add.get("text") or "").strip() if isinstance(add, Mapping) else ""
            if note and note not in s.notes:
                s.notes.append(note)
            ended = bool(out.get("ended"))
            if ended:
                s.live.discard(_live_key(event.name, a_json))
            return ended
        finally:
            self._leave(s)


#: The core's line handing out a subscribe voucher (stable: tests rely on it):
#: the arguments greedy, so the voucher last on the line is the one taken.
_VOUCHER_LINE = re.compile(
    r"To be woken by (\S+) with (\{.*\}), subscribe to it \(MCP Events\) with wakeshift_voucher set to "
    r"(wsv_[a-z2-7]{32}) in its arguments\."
)


def vouchers_in(text: str) -> list[tuple[str, Any, str]]:
    """The subscribe vouchers a block hands out: (event, arguments, voucher) per line."""
    out: list[tuple[str, Any, str]] = []
    for line in text.splitlines():
        m = _VOUCHER_LINE.search(line)
        if m is None:
            continue
        try:
            args = json.loads(m.group(2))
        except ValueError:
            continue
        out.append((m.group(1), args, m.group(3)))
    return out


def _live_key(name: str, args: Mapping[str, Any]) -> tuple[str, str]:
    return name, json.dumps(args, sort_keys=True, separators=(",", ":"))


def _recognized(agent: Any) -> Recognized:
    a = agent if isinstance(agent, Mapping) else {}
    return Recognized(harness=a.get("harness"), via=a.get("via"), claimed=a.get("claimed"))


def _speaks_events(capabilities: Mapping[str, Any] | None, client_name: str, user_agent: str = "") -> bool:
    """A client that speaks MCP Events: it declared the extension, or it's one known to (ChatGPT);
    never a harness Wakeshift wakes itself (BRIDGED_CLIENTS: its name wins)."""
    if bridged_client(client_name, user_agent):
        return False
    caps = capabilities or {}
    exts = caps.get("extensions")
    ext = exts.get(EXTENSION_KEY) if isinstance(exts, Mapping) else None
    return ext is not None or client_name.lower() == "openai-mcp"


# ── The attachment ────────────────────────────────────────────────────────


def _is_type(annotation: Any, cls: type) -> bool:
    if inspect.isclass(annotation):
        return issubclass(annotation, cls)
    if typing.get_origin(annotation) is Annotated:
        return _is_type(typing.get_args(annotation)[0], cls)
    origin = typing.get_origin(annotation)
    if origin is not None:
        return any(_is_type(a, cls) for a in typing.get_args(annotation) if a is not type(None))
    return False


def _hints(fn: Callable[..., Any], context_type: type) -> dict[str, Any]:
    """fn's type hints, resolved. ToolCall and the framework's Context resolve
    even when a module's ``from __future__ import annotations`` leaves them as
    strings naming a local import; any other name that doesn't resolve is a
    TypeError that says which (#66)."""
    try:
        return typing.get_type_hints(
            fn, localns={"ToolCall": ToolCall, "Context": context_type, context_type.__name__: context_type}, include_extras=True
        )
    except NameError as err:
        raise TypeError(
            f"wakeshift: {getattr(fn, '__name__', fn)}: can't resolve the annotation {err.name!r}. With "
            "`from __future__ import annotations`, import the types a tool's parameters use at module level"
        ) from None


def _props(schema: Any) -> set[str]:
    if not isinstance(schema, Mapping):
        return set()
    p = schema.get("properties")
    return set(p) if isinstance(p, Mapping) else set()


def _with_wake_args(schema: Any, subscribing: bool) -> dict[str, Any]:
    """An input schema with Wakeshift's arguments added (all three for a tool
    that subscribes to something, session_key for one with subscribes=[]),
    a copy; a property the schema has already is kept."""
    s: dict[str, Any] = dict(schema) if isinstance(schema, Mapping) else {"type": "object"}
    props: dict[str, Any] = dict(s.get("properties") or {})
    for k, v in wake_args(subscribing).items():
        props.setdefault(k, v)
    return {**s, "properties": props}


class Attachment:
    """Wakeshift attached to an MCP server (``wake.attach``). Register a tool
    that subscribes with ``@attached.tool(subscribes=[...])``; tools
    registered elsewhere are named in attach's ``wrap`` and find their call
    with ``wake.current_call()``."""

    def __init__(
        self,
        wake: "Wakeshift",
        host: "Host",
        identify: "Identify | None",
        *,
        require_user: bool = False,
        is_native_client: "Callable[[Request], bool] | None" = None,
        off: bool = False,
    ) -> None:
        self._wake = wake
        self._host = host
        self._identify = identify
        self._require_user = require_user
        self._is_native_client = is_native_client
        #: A disabled client's attachment: it adds nothing, and its tools are
        #: registered as written, their calls disabled.
        self._off = off
        # attach serves the wait tool (wait=True): only then does a call say
        # its client takes tasks.
        self._wait = False
        #: Wakeshift serves MCP Events here (False: the server has its own).
        self._serves_events = True
        #: Wakeshift advertises MCP Events here: it serves them, and an event
        #: is open natively (personal, authorize or public).
        self._advertises = False
        #: The events each tool that subscribes declares, by name: the ones
        #: registered with tool() and the ones wrap names.
        self._declared: dict[str, list[WakeEvent[Any, Any]]] = {}
        self._registered: set[str] = set()
        self._wrap: dict[str, list[WakeEvent[Any, Any]]] = {}
        #: wrap as a function: the events of a tool named at run time.
        self._wrap_fn: Callable[[str], Sequence[WakeEvent[Any, Any]] | None] | None = None
        #: wrap's tools left unwrapped: their own schemas name Wakeshift's arguments.
        self._unwrapped: set[str] = set()
        self._checks: dict[str, Callable[[Any], str | None]] = {}
        #: attach's own tools, served here: name -> (handler, its tools/list entry).
        self._builtins: dict[str, tuple[Callable[..., Awaitable[Any]], dict[str, Any]]] = {}
        #: The properties each tool's own input schema has, from tools/list.
        self._own: dict[str, set[str]] = {}
        self._task_server: Any = None
        #: An event is open to subscribe_events (personal, authorize or public).
        self._any_open = False
        #: How the server lists attach's own tools itself (Host.list_builtins):
        #: "handler", "registry" or "" (attach's middleware adds them).
        self._lists_builtins = ""
        #: The built-ins in the server's tool registry ("registry").
        self._registered_builtins: set[str] = set()
        self._warned: set[str] = set()
        self._tasks: set[asyncio.Task[Any]] = set()

    def _warn_once(self, key: str, message: str) -> None:
        if key in self._warned:
            return
        self._warned.add(key)
        warn(message.removeprefix("wakeshift: "))

    # ── Registering ───────────────────────────────────────────────────────

    @typing.overload
    def tool(self, name: _F, /) -> _F: ...

    @typing.overload
    def tool(
        self,
        name: str | None = None,
        description: str | None = None,
        *,
        subscribes: "Sequence[WakeEvent[Any, Any]]" = (),
        **options: Any,
    ) -> Callable[[_F], _F]: ...

    def tool(
        self,
        name: Any = None,
        description: str | None = None,
        *,
        subscribes: "Sequence[WakeEvent[Any, Any]]" = (),
        **options: Any,
    ) -> Any:
        """Registers a tool on the server attach was given, as the
        server's own ``@server.tool`` would (its options pass through: on
        FastMCP, ``Tool.from_function``'s, plus MCPServer's
        ``structured_output``), with a parameter annotated ``ToolCall`` the
        SDK fills. ``subscribes`` lists the events the tool subscribes the
        calling conversation to (``[]``, the default: none). tools/list gives
        the tool Wakeshift's arguments and a sentence naming those events;
        the reply gets Wakeshift's block; a call that fails ends the
        subscriptions it made. Returns the function as it was; ``@attached.tool``
        works without parentheses too::

            @attached.tool(subscribes=[build_finished])
            async def watch_build(build: int, call: ToolCall) -> str: ..."""
        with self._wake._config_logged():
            if callable(name):
                self._register(name, None, None, {}, [])
                return name
            if not self._host.registers_tools and not self._off:
                raise TypeError(
                    "wakeshift: a low-level Server has no tools to register: name the tool in attach's wrap "
                    '(wake.attach(server, wrap={"watch": [event]})) and call wake.current_call() in your on_call_tool handler'
                )
            declared = self._declared_of(name, subscribes)

        def decorator(fn: _F) -> _F:
            with self._wake._config_logged():
                self._register(fn, name, description, options, declared)
            return fn

        return decorator

    def _declared_of(self, name: str | None, subscribes: Any) -> "list[WakeEvent[Any, Any]]":
        """The events a tool declares, each one of this app's."""
        what = f"@attached.tool({name!r}, subscribes=...)" if name else "@attached.tool(subscribes=...)"
        if isinstance(subscribes, str | bytes | Mapping) or not isinstance(subscribes, list | tuple):
            raise usage("config", f"{what} lists the events the tool subscribes to ([] for none)")
        for e in subscribes:
            if getattr(e, "_wake", None) is not self._wake:
                raise usage(
                    "config",
                    f"{what} lists {getattr(e, 'name', e)!r}, which isn't one of this app's events: it's another "
                    "client's, or not an event (wake.define_event)",
                )
        return list(subscribes)

    def _register(
        self,
        fn: Callable[..., Any],
        name: str | None,
        description: str | None,
        kwargs: dict[str, Any],
        declared: "list[WakeEvent[Any, Any]]",
    ) -> None:
        Context = self._host.context_type  # noqa: N806

        is_async = inspect.iscoroutinefunction(fn)
        if not is_async and declared:
            raise TypeError(
                f"wakeshift: {getattr(fn, '__name__', fn)}: a tool that subscribes must be async def, to await "
                "call.subscribe (a plain def tool runs in a worker thread, so it can't)"
            )
        tool_name = name or fn.__name__
        if tool_name in BUILTIN_NAMES:
            raise usage("config", f"a tool named {tool_name} would take the name of Wakeshift's own tool: rename it")
        hints = _hints(fn, Context)
        sig = inspect.signature(fn)
        # A tool that subscribes nothing needs no ToolCall parameter: it's a
        # plain registration (with a key, it still takes session_key, for
        # call.unsubscribe through wake.current_call()).
        call_param = next((p for p in sig.parameters if _is_type(hints.get(p), ToolCall)), None)
        if call_param is None and declared:
            raise TypeError(
                f"wakeshift: {fn.__name__}: a tool that subscribes needs a parameter annotated wakeshift.ToolCall (call: ToolCall)"
            )
        own = set(sig.parameters) - {call_param}
        clash = [k for k in WAKE_ARGS if k in own]
        if clash:
            raise usage(
                "config",
                f"{tool_name}'s input names {clash[0]}, one of Wakeshift's arguments (session_key, harness, "
                "conversation_id): rename the parameter",
            )
        ctx_param = next((p for p in sig.parameters if p != call_param and _is_type(hints.get(p), Context)), None)
        params: list[inspect.Parameter] = []
        annotations: dict[str, Any] = {}
        for p in sig.parameters.values():
            if p.name == call_param:
                continue
            ann = hints.get(p.name, p.annotation)
            params.append(p.replace(annotation=ann))
            if ann is not inspect.Parameter.empty:
                annotations[p.name] = ann
        ctx_name = ctx_param or "wakeshift_context"
        if ctx_param is None and not self._off:
            if ctx_name in own:
                raise TypeError(f"wakeshift: {fn.__name__}: a parameter named {ctx_name} is the SDK's")
            params.append(inspect.Parameter(ctx_name, inspect.Parameter.KEYWORD_ONLY, default=None, annotation=Context))
            annotations[ctx_name] = Context
        # Each kind of parameter in its place (a stable sort keeps their order).
        params.sort(key=lambda p: p.kind)
        ret = hints.get("return", sig.return_annotation)
        if ret is not inspect.Signature.empty:
            annotations["return"] = ret
        attached = self

        async def run(**kw: Any) -> Any:
            if is_async:
                return await fn(**kw)
            # A plain def runs in a worker thread, as MCPServer and FastMCP run
            # one; the thread gets a copy of the context, so current_call() works.
            import anyio.to_thread

            return await anyio.to_thread.run_sync(lambda: fn(**kw))

        async def wrapper(**kw: Any) -> Any:
            if attached._off:
                # Wakeshift is off: the tool as written, with a call that asks nothing.
                if call_param is None:
                    return await run(**kw)
                return await run(**kw, **{call_param: attached._wake._disabled_call(tool_name, declared)})
            context = kw[ctx_name] if ctx_param is not None else kw.pop(ctx_name, None)
            scope = _SCOPE.get()
            if scope is None or scope.attached is not attached or scope.entered:
                warn(attached._host.misplaced(tool_name).removeprefix("wakeshift: "))
                raise usage("no_call", f"{tool_name} ran outside a tool call on the server its attach was given")
            scope.entered = True
            call = scope.call
            if call is None:
                # Called by a name attach doesn't know (a FastMCP transform renamed it).
                call = await attached._new_call(scope, declared)
            if context is not None:
                call.context = context
            scope.check = attached._checks.get(tool_name)
            try:
                out = await run(**kw, **({call_param: call} if call_param is not None else {}))
            finally:
                await scope.end()
            scope.string = isinstance(out, str)
            return out

        wrapper.__name__ = fn.__name__
        wrapper.__qualname__ = getattr(fn, "__qualname__", fn.__name__)
        wrapper.__doc__ = fn.__doc__
        wrapper.__module__ = fn.__module__
        wrapper.__signature__ = inspect.Signature(params, return_annotation=ret)  # type: ignore[attr-defined]
        wrapper.__annotations__ = annotations
        # A tool returning str replies with text: MCPServer and FastMCP would
        # give it an output schema and structuredContent {"result": …}, which
        # some hosts show the model instead of the text.
        added = self._host.add_tool(
            wrapper,
            name=tool_name,
            description=description,
            text=ret is str and not self._off,
            options=kwargs,
            wake_args=None if self._off else bool(declared),
        )
        if added.output_names_wakeshift:
            self._host.remove_tool(tool_name)
            raise usage("config", f"{tool_name}'s output schema names wakeshift, the field Wakeshift's block goes in: rename it")
        if self._off:
            return
        if added.check is not None:
            self._checks[tool_name] = added.check
        self._registered.add(tool_name)
        self._declared[tool_name] = declared

    def _set_wrap(self, wrap: "Wrap | None") -> None:
        if wrap is None:
            return
        if callable(wrap) and not isinstance(wrap, Mapping):
            self._wrap_fn = wrap
            return
        if not isinstance(wrap, Mapping):
            raise usage(
                "config",
                'attach\'s wrap maps tool names to the events each subscribes to ({"watch": [event]}), or is a '
                "function of a tool's name returning them (or None)",
            )
        for tool_name, events in wrap.items():
            if not isinstance(tool_name, str) or not tool_name:
                raise usage("config", "attach's wrap is keyed by tool names")
            if tool_name in BUILTIN_NAMES:
                raise usage("config", f"attach's wrap names {tool_name}, Wakeshift's own tool")
            self._wrap[tool_name] = self._wrap_events(f"attach's wrap[{tool_name!r}]", events)
            self._declared[tool_name] = list(self._wrap[tool_name])

    def _wrap_events(self, what: str, events: Any) -> "list[WakeEvent[Any, Any]]":
        """wrap's events for one tool, checked: a list of this app's events."""
        if isinstance(events, str | bytes | Mapping) or not isinstance(events, list | tuple):
            raise usage("config", f"{what} lists the events the tool subscribes to ([] for none)")
        for e in events:
            if getattr(e, "_wake", None) is not self._wake:
                raise usage(
                    "config",
                    f"{what} lists {getattr(e, 'name', e)!r}, which isn't one of this app's events (wake.define_event)",
                )
        return list(events)

    def _events_of(self, name: Any) -> "list[WakeEvent[Any, Any]] | None":
        """The events a tool subscribes to: registered or named in wrap, else
        what wrap's function says for it, asked each time (never for
        Wakeshift's own tools); None for a tool that doesn't subscribe, or
        whose answer isn't a list of this client's events (wrap_bad_event)."""
        if not isinstance(name, str):
            return None
        declared = self._declared.get(name)
        if declared is not None or self._wrap_fn is None or name in BUILTIN_NAMES:
            return declared
        events = self._wrap_fn(name)
        if events is None:
            return None
        try:
            return self._wrap_events(f"attach's wrap({name!r})", events)
        except UsageError:
            # As in every SDK: logged, and the tool is served unwrapped.
            self._wake._log(
                "wrap_bad_event",
                f"attach's wrap function returned something other than a list of this client's events for {name}, "
                "so it's served unwrapped",
                tool=name,
            )
            return None

    # ── Who's calling ─────────────────────────────────────────────────────

    def _request(self, rc: Any) -> "Request":
        """The request behind rc, as identify sees it: its headers, the access
        token the server's auth verified for this very HTTP request (not a
        session's first one), and the client's name."""
        from ._events import Request

        client, _, headers = _request_info(rc)
        return Request(headers=dict(headers) if headers else {}, token=_token_of(rc), client_name=client or None, raw=rc)

    async def _who(self, rc: Any) -> "tuple[Identity, str | None]":
        """Who's behind the request: identify's answer, asked at most once per
        request and only when something needs it, and why Wakeshift's part
        of the request is refused, if it is ("failed": identify raised or
        took too long; "anonymous": require_user and no user)."""
        st = _ASKED.get()
        if st is None or st.attached is not self:
            return await self._ask(rc)
        if st.future is None:
            st.future = asyncio.ensure_future(self._ask(rc))
        return await asyncio.shield(st.future)

    async def _ask(self, rc: Any) -> "tuple[Identity, str | None]":
        from ._events import LIMITS, Identity

        who: Any = None
        if self._identify is not None:
            try:
                r = self._identify(self._request(rc))
                if inspect.isawaitable(r):
                    r = await asyncio.wait_for(r, LIMITS["identify"])
                who = r
                if who is not None and not isinstance(who, Identity):
                    raise TypeError("identify returns an Identity or None")
            except Exception as err:
                # Wakeshift's part of the request is refused, never run as
                # anonymous; the app hears the error's type, never its message.
                self._wake._log(
                    "identify_error",
                    f"identify failed for this request ({type(err).__name__}), so Wakeshift's part of it was refused",
                    error_type=type(err).__name__,
                )
                return Identity(), "failed"
        who = who or Identity()
        if self._require_user and not who.user:
            return Identity(), "anonymous"
        return who, None

    def _native_of(self, context: Any, rc: Any, client: str) -> tuple[bool, str | None]:
        """Whether a call comes from a client that subscribes to the app's
        events itself, on stateless servers too (which never see initialize),
        in this order: the app's override (is_native_client); ChatGPT's
        _meta["openai/session"]; a client name (initialize's, or _meta's, else
        identify's) or User-Agent of a known native client; never a harness
        Wakeshift wakes itself (BRIDGED_CLIENTS: its name wins over
        capabilities); a client that declares MCP Events. The conversation is ChatGPT's, when it names one.
        Never, on a host without MCP Events (FastMCP 2/3): every client gets
        the block."""
        if not getattr(self._host, "native_events", True):
            return False, None
        _, meta, headers = _request_info(rc)
        conv = meta.get("openai/session")
        conversation = conv if isinstance(conv, str) and conv else None
        if self._is_native_client is not None and self._is_native_client(self._request(rc)):
            return True, conversation
        if conversation:
            return True, conversation
        ua = headers.get("user-agent", "") if headers else ""
        if dialect_of(client, ua) == "chatgpt":
            return True, None
        if bridged_client(client, ua):
            return False, None
        caps = _client_capabilities(rc)
        exts = caps.get("extensions")
        if isinstance(exts, Mapping) and EXTENSION_KEY in exts:
            return True, None
        meta_caps = meta.get("io.modelcontextprotocol/clientCapabilities")
        if isinstance(meta_caps, Mapping):
            mexts = meta_caps.get("extensions")
            if isinstance(mexts, Mapping) and EXTENSION_KEY in mexts:
                return True, None
        return False, None

    async def _new_call(self, scope: _Scope, declared: "list[WakeEvent[Any, Any]]") -> "ToolCall":
        """The ToolCall for a call of a tool that subscribes."""
        rc = scope.rc
        who, refused = await self._who(rc)
        wa = scope.wake_args
        call = ToolCall(
            _SENTINEL,
            scope=scope,
            context=rc,
            user=who.user,
            client=who.client,
            tool=scope.tool,
            native=False,
            native_conversation=None,
            session=_session(wa.get(SESSION_ARG)),
            harness=_str(wa.get(HARNESS_ARG)),
            conversation=_str(wa.get(CONVERSATION_ARG)),
            declared=declared,
            checked=True,
        )
        call._refused = refused
        if refused == "anonymous":
            self._wake._log(
                "user_required",
                f"require_user refused this call of {scope.tool}: identify gave no user, so Wakeshift's part of it was refused",
                tool=scope.tool,
            )
        call.native, call.native_conversation = self._native_of(rc, rc, call._client)
        call._tasks = call._tasks and self._wait
        scope.call = call
        return call

    def _declared_for(self, name: str) -> "list[WakeEvent[Any, Any]] | None":
        """The events the tool called name declares, or None: it isn't a tool
        that subscribes (or wrap names it, but its own schema names
        Wakeshift's arguments, so it's left unwrapped)."""
        if name in self._registered:
            return self._declared[name]
        events = self._events_of(name)
        if events is None or name in self._unwrapped:
            return None
        own = self._own_props(name)
        if own is not None and self._wrap_conflict(name, own, set()):
            return None
        return events

    def _own_props(self, name: str) -> set[str] | None:
        """The properties the tool's own input schema names: the host's own
        record where it keeps one, else the last tools/list's; None if unknown."""
        props = self._host.own_input_props(name)
        return props if props is not None else self._own.get(name)

    def _wrap_conflict(self, name: str, props: set[str], output: set[str]) -> bool:
        clash = [k for k in WAKE_ARGS if k in props] or (["wakeshift"] if "wakeshift" in output else [])
        if not clash:
            return False
        self._unwrapped.add(name)
        self._wake._log(
            "wrap_arg_conflict",
            f"{name}, named in wrap, already has a {clash[0]} of its own, so it's served unwrapped",
            tool=name,
        )
        return True

    # ── The middleware ────────────────────────────────────────────────────

    def _strip(self, ctx: Any) -> tuple[Any, str, dict[str, Any]]:
        """Takes Wakeshift's arguments out of a tools/call (unless the tool
        names them itself), before the host validates them and before vendor
        middleware or the tool sees them: deleted from the request's own
        argument dict, so no later reader finds the raw value. Returns the
        context to go on with, the tool's name and what was taken out."""
        params = ctx.params if isinstance(getattr(ctx, "params", None), Mapping) else {}
        raw_name = params.get("name")
        name = raw_name if isinstance(raw_name, str) else ""
        args = params.get("arguments")
        if not isinstance(args, Mapping):
            return ctx, name, {}
        keep: set[str] = set()
        if name not in self._declared and name not in self._builtins:
            keep = self._own_props(name) or set()
        names = [k for k in WAKE_ARGS if k in args and k not in keep]
        if not names:
            return ctx, name, {}
        # The session key is a SessionKey from here on: it never prints.
        taken = {k: (_session(args[k]) if k == SESSION_ARG else args[k]) for k in names}
        if isinstance(args, dict) and isinstance(params, dict):
            for k in names:
                del args[k]
            return ctx, name, taken
        rest = {k: v for k, v in args.items() if k not in names}
        return dataclasses.replace(ctx, params={**params, "arguments": rest}), name, taken

    async def _middleware(self, ctx: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
        token = _ASKED.set(_Asked(self))
        listed = _LISTED.set(set())
        try:
            return await self._serve(ctx, call_next)
        finally:
            _LISTED.reset(listed)
            _ASKED.reset(token)

    async def _serve(self, ctx: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
        method = getattr(ctx, "method", "")
        ts = self._task_server
        if ts is not None and ts.handles(ctx):
            return await ts.answer(ctx)
        if method == "tools/call":
            return await self._tools_call(ctx, call_next)
        result = await call_next(ctx)
        if method in ("initialize", "server/discover") and isinstance(result, dict) and self._advertises:
            caps = result.setdefault("capabilities", {})
            if isinstance(caps, dict):
                caps[TOP_LEVEL_KEY] = {"listChanged": False}
                caps.setdefault("extensions", {})[EXTENSION_KEY] = {"listChanged": False}
        elif method == "tools/list" and isinstance(result, dict) and isinstance(result.get("tools"), list):
            await self._amend_tools_list(ctx, result)
        if ts is not None:
            ts.amend(ctx, method, result)
        return result

    def _inject_builtins(self, result: Any) -> Any:
        """A tools/list handler's first page with attach's own tools added,
        innermost (Host.list_builtins "handler"), so the app's own tools/list
        middleware sees them: a tool of the server's with one's name is
        dropped (builtin_name_conflict). Each name added goes in the request's
        _LISTED, for attach's middleware outside."""
        import mcp_types as types

        tools = result.get("tools") if isinstance(result, dict) else getattr(result, "tools", None)
        if not isinstance(tools, list):
            return result
        kept = []
        for t in tools:
            n = t.get("name") if isinstance(t, Mapping) else getattr(t, "name", None)
            if n in self._builtins:
                self._wake._log(
                    "builtin_name_conflict",
                    f"a tool registered elsewhere is named {n}, like Wakeshift's own, so Wakeshift's is served",
                    tool=n,
                )
                continue
            kept.append(t)
        typed = not isinstance(result, dict)
        for _, entry in self._builtins.values():
            kept.append(types.Tool.model_validate(copy.deepcopy(entry)) if typed else copy.deepcopy(entry))
        listed = _LISTED.get()
        if listed is not None:
            listed.update(self._builtins)
        if typed:
            return result.model_copy(update={"tools": kept})
        return {**result, "tools": kept}

    async def _amend_tools_list(self, ctx: Any, result: dict[str, Any]) -> None:
        tools: list[Any] = result["tools"]
        params = ctx.params if isinstance(getattr(ctx, "params", None), Mapping) else {}
        first = params.get("cursor") is None
        complete = first and not result.get("nextCursor")
        # The built-ins the server listed itself, as the app's own middleware left them.
        ours = (_LISTED.get() or set()) | self._registered_builtins
        listed: set[str] = set()
        kept: list[Any] = []
        for t in tools:
            if not isinstance(t, Mapping):
                kept.append(t)
                continue
            n = t.get("name")
            if isinstance(n, str):
                listed.add(n)
                if n not in self._registered:
                    self._own[n] = _props(t.get("inputSchema"))
                if n in self._builtins and n in ours:
                    kept.append(t)  # listed by the server itself: kept as its middleware left it
                    continue
                if n in self._builtins:
                    # Wakeshift's own tool wins over one registered elsewhere with its name.
                    self._wake._log(
                        "builtin_name_conflict",
                        f"a tool registered elsewhere is named {n}, like Wakeshift's own, so Wakeshift's is served",
                        tool=n,
                    )
                    continue
                if n not in self._registered and n not in self._unwrapped and self._events_of(n) is not None:
                    self._wrap_conflict(n, _props(t.get("inputSchema")), _props(t.get("outputSchema")))
            kept.append(t)
        tools[:] = kept
        if complete:
            for n in self._wrap:
                if n not in listed:
                    self._wake._log("wrap_unknown_tool", f"wrap names {n}, which this server doesn't list", tool=n)
        self._amend_inputs(tools)
        self._amend_list(tools)
        if any(isinstance(t, Mapping) and self._listed_as_wrapped(t.get("name")) and self._events_of(t["name"]) for t in tools):
            # The app's name, as the console has it (asked once, never waited for long).
            self._describe_list(tools, await self._wake._app_name())
        # list_subscriptions and unsubscribe_events only where something on
        # this server can make a subscription: a tool that subscribes to an
        # event, or an event open to subscribe_events.
        can_subscribe = (
            self._any_open
            or any(self._declared.values())
            or any(
                isinstance(t, Mapping) and self._listed_as_wrapped(t.get("name")) and self._events_of(t["name"]) for t in tools
            )
        )
        if first:
            tools.extend(copy.deepcopy(entry) for name, (_, entry) in self._builtins.items() if name not in ours)
        if not can_subscribe:
            hidden = {"list_subscriptions", "unsubscribe_events"} & set(self._builtins)
            tools[:] = [t for t in tools if not (isinstance(t, Mapping) and t.get("name") in hidden)]
        # Built-in tools carry execution only when the server's own tools do
        # (strict tool-list checks refuse a field the server never uses); the
        # wait tool keeps its own, which says it's answered with a task.
        if not any(isinstance(t, Mapping) and t.get("name") not in self._builtins and "execution" in t for t in tools):
            for i, t in enumerate(tools):
                if (
                    isinstance(t, Mapping)
                    and t.get("name") in self._builtins
                    and t.get("name") != "wakeshift_wait"
                    and "execution" in t
                ):
                    tools[i] = {k: v for k, v in t.items() if k != "execution"}
        client, _, headers = _request_info(ctx)
        ua = headers.get("user-agent", "") if headers else ""
        if self._serves_events and _speaks_events(_client_capabilities(ctx), client, ua):
            # A client that speaks MCP Events subscribes natively.
            tools[:] = [t for t in tools if not (isinstance(t, Mapping) and t.get("name") == "subscribe_events")]
        # The list differs by what each request declares (the wait tool,
        # subscribe_events), so a cached copy is this client's only
        # (SEP-2549): a public scope the server set becomes private.
        if result.get("cacheScope") == "public":
            result["cacheScope"] = "private"

    def _amend_inputs(self, tools: list[Any]) -> None:
        """Each tool that subscribes, named in wrap, gets Wakeshift's arguments
        (a registered one has them already, from its registration)."""
        for i, t in enumerate(tools):
            n = t.get("name") if isinstance(t, Mapping) else None
            if isinstance(t, Mapping) and isinstance(n, str) and self._listed_as_wrapped(n):
                tools[i] = {**t, "inputSchema": _with_wake_args(t.get("inputSchema"), bool(self._events_of(n)))}

    def _listed_as_wrapped(self, name: Any) -> bool:
        return isinstance(name, str) and name not in self._unwrapped and self._events_of(name) is not None

    def _input_schema_lookup(self, orig: Callable[[str], Any]) -> Callable[[str], Any]:
        """The host's get_tool_input_schema, answering with the schemas
        tools/list gives (attach's own tools, and Wakeshift's arguments on a
        tool that subscribes), so header validation on protocol 2026-07-28
        sees what the client was shown (#72)."""

        def lookup(name: str) -> Any:
            if name in self._builtins:
                return self._builtins[name][1]["inputSchema"]
            schema = orig(name)
            if schema is not None and self._listed_as_wrapped(name):
                return _with_wake_args(schema, bool(self._events_of(name)))
            return schema

        return lookup

    async def _call_builtin(self, name: str, ctx: Any, wake_args: dict[str, Any]) -> dict[str, Any]:
        fn, entry = self._builtins[name]
        params = ctx.params if isinstance(getattr(ctx, "params", None), Mapping) else {}
        args = params.get("arguments")
        named = entry["inputSchema"].get("properties") or {}
        kw = {k: v for k, v in (args.items() if isinstance(args, Mapping) else ()) if k in named and k not in WAKE_ARGS}
        result = await fn(**kw, wake_args=wake_args, ctx=ctx)
        out: dict[str, Any] = result.model_dump(by_alias=True, mode="json", exclude_none=True)
        if str(getattr(ctx, "protocol_version", "")) >= "2026-07-28":
            out.setdefault("resultType", "complete")
        return out

    async def _tools_call(self, ctx: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
        ctx, name, wake_args = self._strip(ctx)
        if name in self._builtins:
            return await self._call_builtin(name, ctx, wake_args)
        scope = _Scope(self, ctx, name, wake_args)
        token = _SCOPE.set(scope)
        # Found by the request's context, as a tool's Context gives it
        # (FastMCP 2/3's bridge wraps it in its own request).
        key = id(getattr(ctx, "request_context", None) or ctx)
        self._wake._scopes[key] = scope
        try:
            declared = self._declared_for(name)
            if declared is not None:
                await self._new_call(scope, declared)
            try:
                result = await call_next(ctx)
            finally:
                if scope.call is not None and not scope.ended:
                    await scope.end()
        except BaseException as err:
            if scope.wrapped:
                await self._rollback_shielded(scope)
            neutral = _refused_words(scope, err)
            if neutral:
                # The tool passed on call.subscribe's identify refusal: neutral
                # words for the agent; the log has the developer's fix.
                return {"content": [{"type": "text", "text": neutral}], "isError": True}
            raise
        finally:
            _SCOPE.reset(token)
            self._wake._scopes.pop(key, None)
        if not scope.wrapped:
            return result
        if scope.call is not None and scope.call._refused:
            # Wakeshift's part of the call is refused: the tool's own reply, as
            # it was, unless it shows the refusal's developer words.
            if isinstance(result, dict) and result.get("isError"):
                shown = " ".join(str(c.get("text", "")) for c in result.get("content") or [] if isinstance(c, dict))
                neutral = _refused_words(scope, shown)
                if neutral:
                    return {**result, "content": [{"type": "text", "text": neutral}]}
            return result
        if not isinstance(result, dict) or result.get("isError"):
            await self._rollback_shielded(scope)
            return result
        bad = scope.check(result) if scope.check else None
        if bad:
            # As MCPServer refuses such a result: the call fails, and what it
            # subscribed is rolled back.
            await self._rollback_shielded(scope)
            assert scope.call is not None
            return {
                "content": [{"type": "text", "text": f"Error executing tool {scope.call.tool}: {bad}"}],
                "isError": True,
            }
        try:
            out = await self._add_block(scope, result)
        except Exception as err:
            if not scope.armed and not scope.created and scope.native:
                # Only native lines, so nothing to roll back: the tool's result
                # goes out as it would without Wakeshift (docs/subscribe-vouchers.md §7.2).
                assert scope.call is not None
                self._wake._log(
                    "native_unavailable",
                    f"Wakeshift's addition to a result of {scope.call.tool} failed, so it carries no line for the MCP Events client",
                    tool=scope.call.tool,
                    event=scope.native[0]["name"],
                    reason="addition_failed",
                )
                plain = dict(result)
                _drop_trailers(plain)
                return plain
            # Wakeshift couldn't give the block: the result goes out as the
            # tool gave it, as it would without Wakeshift, never an error (the
            # tool may have done paid or visible work). The conversation
            # wouldn't learn how it's woken, so what the call subscribed is
            # rolled back.
            await self._rollback_shielded(scope)
            assert scope.call is not None
            self._wake._log(
                "block_failed",
                f"Wakeshift's addition to a result of {scope.call.tool} failed ({type(err).__name__}), so the result "
                "went out without it and the call's subscriptions were rolled back",
                tool=scope.call.tool,
                error_type=type(err).__name__,
            )
            out = dict(result)
            _drop_trailers(out)
            return out
        # The call succeeded: each subscription it made and kept is new.
        from ._watch import fire

        assert scope.call is not None
        for c in scope.created:
            if c.event is not None and _live_key(c.name, c.arguments) in scope.live:
                fire(self._wake, c.event, c.parsed, scope.call.user, "tool", scope.call.tool, context=c.context, run_in=c.vars)
        return out

    async def _rollback_shielded(self, scope: _Scope) -> None:
        """Rolls back under asyncio.shield, so a canceled request still ends
        what it made."""
        if not scope.created:
            return
        task = asyncio.ensure_future(self._rollback(scope))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        # Shielded: a canceled request still ends what it made. The
        # cancellation still reaches the caller, after the rollback is under way.
        await asyncio.shield(task)

    async def _rollback(self, scope: _Scope) -> None:
        """Ends every subscription the call created and no other call has
        renewed since, with reason rolled_back
        (audited as such, no "ended" notice), and stops the watches the call
        started for them."""
        assert scope.call is not None
        for w in scope.watches:
            w.stop.set()
        for c in scope.created:
            try:
                await self._wake._request_app(
                    "POST",
                    "/event-subscriptions/unsubscribe",
                    {
                        "call": scope.call._wire(session=c.session),
                        "name": c.name,
                        "arguments": c.arguments,
                        "reason": "rolled_back",
                        "pending": c.pending,
                    },
                )
            except Exception as err:
                self._wake._log(
                    "rollback_failed",
                    f"ending {scope.call.tool}'s subscription to {c.name} after the call failed raised {type(err).__name__}",
                    tool=scope.call.tool,
                    event=c.name,
                )

    async def _add_block(self, scope: _Scope, result: dict[str, Any]) -> dict[str, Any]:
        """result with the call's one block, from /calls/addition. A call that
        names no conversation, and subscribed nothing, gets nothing but the
        notes of its unsubscribes. Only a failure for a call that subscribed
        (or names native lines) raises."""
        call = scope.call
        assert call is not None
        add = await self._fetch_addition(scope, call)
        if add is None and not scope.notes:
            # Nothing to add, but a beacon line ending the app's text is still dropped.
            out = dict(result)
            _drop_trailers(out)
            return out
        add = add or {}
        if scope.native_contexts:
            self._remember_vouchers(scope, str(add.get("text") or ""))
        # Notes from the call's unsubscribes come first, whatever else it gets.
        text = "\n\n".join(t for t in (" ".join(scope.notes), str(add.get("text") or "").strip()) if t)
        out = merge_block(
            result,
            text,
            str(add.get("trailer") or ""),
            add.get("meta") if isinstance(add.get("meta"), Mapping) else None,
            string=scope.string,
        )
        if text and out.get("structuredContent") is not None and not isinstance(out.get("structuredContent"), dict):
            self._warn_once(
                "output:" + call.tool,
                f"wakeshift: {call.tool} returns structuredContent that isn't an object, so it can't carry how this "
                "conversation is woken: hosts that show only structuredContent won't see it",
            )
        return out

    def _remember_vouchers(self, scope: _Scope, text: str) -> None:
        """Keeps each subscribe's context= with the voucher the block hands out
        for its event and arguments (by the voucher's hash), for its redemption."""
        for event, args, token in vouchers_in(text):
            for line in scope.native:
                if line["name"] == event and _same_json(line["arguments"], args):
                    context = scope.native_contexts.get(_live_key(line["name"], line["arguments"]))
                    if context is not None:
                        self._wake._remember_voucher(token, context)
                    break

    async def _fetch_addition(self, scope: _Scope, call: ToolCall) -> Mapping[str, Any] | None:
        """/calls/addition's answer for the call (_add_block), or None if it
        names no conversation and subscribed nothing, or the courtesy
        failed."""
        if scope.armed and not scope.live and not scope.native:
            # Every subscription the call made, it also ended: no block (it
            # would tell the agent to start listening for nothing).
            return None
        if call.native:
            # The manifest's answer first, so a native call's wire knows
            # whether the core hands out vouchers.
            await self._wake._vouchers_offered()
        wire = call._wire()
        if not scope.pending and not scope.native and not wire.get("session") and not wire.get("conversation"):
            if not conversation_key(wire.get("meta")):
                return None
        body: dict[str, Any] = {"call": wire}
        if scope.pending:
            body["pending"] = scope.pending
        if scope.native:
            body["native"] = scope.native
        try:
            return await self._wake._request_app("POST", "/calls/addition", body)
        except Exception as err:
            if scope.armed or scope.native:
                raise
            # A courtesy (the call subscribed nothing): debug only.
            logger.debug("wakeshift: Wakeshift's addition to %s's result failed: %s", call.tool, type(err).__name__)
            return None

    def _describe_list(self, tools: list[Any], app: str) -> None:
        """tools/list's tools with the subscribes sentence after each declaring
        tool's description, naming the app (app: its name, "" for "this app")."""
        from ._events import _with_subscribes

        for i, t in enumerate(tools):
            if not isinstance(t, Mapping):
                continue
            n = t.get("name")
            evs = self._events_of(n) if self._listed_as_wrapped(n) else None
            if not evs:
                continue
            d = t.get("description") if isinstance(t.get("description"), str) else ""
            described = _with_subscribes(d or "", [e.description for e in evs], app)
            if described != d:
                tools[i] = {**t, "description": described}
            if len(described) > _MAX_DESCRIPTION:
                self._wake._log(
                    "description_too_long",
                    f"{n}'s description is {len(described)} characters with Wakeshift's sentence, over {_MAX_DESCRIPTION}",
                    tool=n,
                )

    def _amend_list(self, tools: list[Any]) -> None:
        """tools/list's tools with each wrapped tool's object output schema
        naming ``wakeshift`` (with_wakeshift_field), which its structured
        results carry."""
        for i, t in enumerate(tools):
            n = t.get("name") if isinstance(t, Mapping) else None
            if (
                isinstance(t, Mapping)
                and isinstance(n, str)
                and self._listed_as_wrapped(n)
                and isinstance(t.get("outputSchema"), Mapping)
            ):
                schema = with_wakeshift_field(t["outputSchema"])
                if schema is not t["outputSchema"]:
                    tools[i] = {**t, "outputSchema": schema}


def _str(v: Any) -> str | None:
    return v.strip() if isinstance(v, str) and v.strip() else None


def _refused_words(scope: Any, shown: Any) -> str:
    """Neutral words for the agent in place of an exception, or an error
    result's text, that passes on the call's identify refusal (the
    developer's fix stays in the log, identify_error or user_required); "" otherwise."""
    call = getattr(scope, "call", None)
    refused = getattr(call, "_refused", None) if call is not None else None
    if not refused:
        return ""
    from ._errors import FIXES, UsageError
    from ._events import _refused_text

    def shows(text: str) -> bool:
        return FIXES["identify_failed"] in text or FIXES["user_required"] in text

    if isinstance(shown, str):
        passed_on = shows(shown)
    else:
        # The host may wrap the tool's exception (MCPServer's UnexpectedToolError).
        passed_on, seen = False, 0
        while isinstance(shown, BaseException) and seen < 5 and not passed_on:
            passed_on = (isinstance(shown, UsageError) and shown.code in ("identify_failed", "user_required")) or shows(
                str(shown)
            )
            shown, seen = shown.__cause__ or shown.__context__, seen + 1
    return _refused_text(refused, sentence=True) if passed_on else ""
