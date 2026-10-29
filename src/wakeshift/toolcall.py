"""One way to subscribe from a tool (docs/subscribe-from-tools.md): a tool
registered with ``@attached.tool()`` gets a :class:`ToolCall`, and subscribes
the calling conversation with ``await call.subscribe(event, args)`` (ends it
with ``call.unsubscribe``). Attach's middleware adds one Wakeshift block to
whatever the tool returns, so a conversation can't be left unwakeable by a
forgotten argument, a wrong tool name or an unreturned result; and a call
that fails ends the subscriptions it made. A port of the Go SDK's
appkit.Events (pkg/appkit/toolcall.go, attachcall.go)::

    attached = wake.attach(server, identify=lambda ctx: Identity(user=user_of(ctx)))

    @attached.tool()
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
import dataclasses
import inspect
import json
import typing
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, Optional

from pydantic import BaseModel, Field

from .client import (
    CONVERSATION_ARG,
    CONVERSATION_DESCRIPTION,
    HARNESS_ARG,
    HARNESS_DESCRIPTION,
    SESSION_ARG,
    SESSION_DESCRIPTION,
    Recognised,
    _call_of,
    _client_capabilities,
    _request_info,
    _tool_of,
)
from .errors import WakeshiftError
from .spec import EXTENSION_KEY, dialect_of

if TYPE_CHECKING:
    from .client import Wakeshift
    from .events import Identity, WakeEvent


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


class CallEnded(RuntimeError):
    """A ToolCall used after the tool that got it returned: subscribe before
    returning."""


class WakeOutput(BaseModel):
    """Subclass it for a wrapped tool's return type to carry Wakeshift's block
    in its structured output too: the wrapper sets ``wakeshift`` to the
    block's text (without a beacon line), for hosts that show the model only
    structuredContent. The block stays in the content as well."""

    wakeshift: str | None = Field(
        default=None, description="What Wakeshift adds for this conversation: how it's woken by this app's events"
    )


@dataclass(frozen=True)
class EventsOptions:
    """Attach's event features."""

    #: Asked first whether a wrapped tool's call comes from a client that
    #: subscribes to the app's events itself (ToolCall.native), for callers
    #: the SDK can't tell (an app's own house agents). It gets the tool's
    #: Context. False leaves it to the SDK's own checks.
    native: Callable[[Any], bool] | None = None


@dataclass(frozen=True)
class Subscription:
    """The calling conversation's subscription, made or renewed by
    ``call.subscribe``. ``id`` and ``user`` address it later: an emit's
    ``Recipient(user=sub.user, subscription=sub.id)``, or
    ``event.end(sub.user, sub.id)``."""

    id: str
    user: str
    #: False for a renewal.
    created: bool = False
    #: The conversation's session. A secret: don't show, store or log it.
    session: str = dataclasses.field(default="", repr=False)
    #: What Wakeshift recognised the agent's program as.
    recognised: Recognised = dataclasses.field(default_factory=Recognised)
    #: The caller subscribes itself (ToolCall.native): nothing was made, id
    #: is "", and the block tells it what to subscribe to.
    native: bool = False


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


def _take_trailer(result: dict[str, Any]) -> str:
    """Takes a beacon line off the end of result's text (dropping its block
    if nothing else is in it), returning it ("" if there's none)."""
    content = result.get("content") or []
    if not content:
        return ""
    last = content[-1]
    if not isinstance(last, dict) or last.get("type") != "text":
        return ""
    text = str(last.get("text", "")).rstrip(" \t\r\n")
    rest, line = "", text
    i = text.rfind("\n")
    if i >= 0:
        rest, line = text[:i], text[i + 1 :]
    if not line.startswith(BEACON_LINE_PREFIX) or not line.endswith(")"):
        return ""
    rest = rest.rstrip(" \t\r\n")
    if not rest:
        result["content"] = content[:-1]
    else:
        result["content"] = [*content[:-1], {**last, "text": rest}]
    return line


def merge_block(
    result: dict[str, Any],
    text: str,
    trailer: str = "",
    meta: Mapping[str, Any] | None = None,
    *,
    string: bool = False,
    wake_output: bool = False,
) -> dict[str, Any]:
    """A tool result (the wire form) with Wakeshift's block added: its text as
    a block after the app's content, then a text-carried beacon last, its
    _meta merged. A result that already carries a beacon keeps it, last, and
    gets no second one. ``string``: the tool returned a string, which stays
    one text block. ``wake_output``: the tool's output opted in (WakeOutput),
    so structuredContent gets the text too. The shared fixture
    sdk/typescript/test/wrapped-block.json pins it."""
    out = dict(result)
    out["content"] = list(out.get("content") or [])
    text = (text or "").strip()
    own_meta = out.get("_meta") if isinstance(out.get("_meta"), dict) else None
    meta_beacon = own_meta is not None and CONNECTOR_META_KEY in own_meta
    existing = _take_trailer(out)
    if text:
        out["content"].append({"type": "text", "text": text})
    has = meta_beacon or bool(existing)
    if existing:
        out["content"].append({"type": "text", "text": existing})
    elif not has and trailer:
        out["content"].append({"type": "text", "text": trailer})
    for k, v in (meta or {}).items():
        if k == CONNECTOR_META_KEY and has:
            continue
        merged = dict(out.get("_meta") or {})
        merged[k] = v
        out["_meta"] = merged
    if string and all(isinstance(c, dict) and c.get("type") == "text" for c in out["content"]):
        joined = "\n\n".join(str(c.get("text", "")) for c in out["content"] if c.get("text"))
        out["content"] = [{"type": "text", "text": joined}] if joined else []
    if text and wake_output and isinstance(out.get("structuredContent"), dict):
        out["structuredContent"] = {**out["structuredContent"], "wakeshift": text}
    return out


# ── The call and its scope ────────────────────────────────────────────────

_SENTINEL = object()

#: The tools/call scope Attach's middleware makes, which a wrapped tool fills.
_SCOPE: contextvars.ContextVar["_Scope | None"] = contextvars.ContextVar("wakeshift_tool_scope", default=None)


@dataclass
class _Created:
    name: str
    arguments: dict[str, Any]
    session: str
    #: The call's pending-addition record, which made it.
    pending: str = ""


class _Scope:
    """One tools/call as Attach's middleware sees it: what its wrapped tool
    subscribed, for the block and the rollback."""

    def __init__(self, attached: "Attached", request_id: Any) -> None:
        self.attached = attached
        self.request_id = request_id
        self.call: ToolCall | None = None
        self.wake_output = False
        self.string = False
        self.lock = asyncio.Lock()
        self.busy = 0
        self.ended = False
        self.pending = ""
        self.armed = False
        self.created: list[_Created] = []
        self.native: list[dict[str, Any]] = []

    @property
    def wrapped(self) -> bool:
        return self.call is not None

    async def end(self) -> None:
        """Closes the call to further requests, after waiting for any in
        flight (warning once per tool: the tool should have awaited them)."""
        await asyncio.sleep(0)  # tasks the tool started begin
        if self.busy > 0 and self.call is not None:
            self.attached._warn_once(
                "inflight:" + self.call.tool,
                "wakeshift: a wrapped tool returned while a subscribe was in flight; it was waited for: await "
                "call.subscribe, call.unsubscribe and call.resolve_session before returning",
            )
        while self.busy > 0:
            async with self.lock:
                pass
            await asyncio.sleep(0)
        self.ended = True

    def add_native(self, name: str, args: dict[str, Any]) -> None:
        key = json.dumps(args, sort_keys=True, separators=(",", ":"))
        for line in self.native:
            if line["name"] == name and json.dumps(line["arguments"], sort_keys=True, separators=(",", ":")) == key:
                return
        if len(self.native) >= _MAX_NATIVE_LINES:
            tool = self.call.tool if self.call else ""
            self.attached._warn_once(
                "native:" + tool,
                "wakeshift: a wrapped tool subscribed a native client to more than 20 events in one call; only 20 are named",
            )
            return
        self.native.append({"name": name, "arguments": args})


class ToolCall:
    """One call of a tool registered with ``@attached.tool()``: who's calling,
    and the calls that subscribe the calling conversation. Only the SDK makes
    one."""

    def __init__(
        self,
        _token: object,
        *,
        scope: _Scope,
        context: Any,
        user: str | None,
        tool: str,
        native: bool,
        native_conversation: str | None,
        session: str | None,
        harness: str | None,
        conversation: str | None,
        client: str | None = None,
    ) -> None:
        if _token is not _SENTINEL:
            raise TypeError("wakeshift: a ToolCall is made by @attached.tool(): subscribe only from a wrapped tool")
        self._scope = scope
        #: The tool's Context (mcp.server.mcpserver.Context): MCP's own.
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
        #: The session_key passed; after a subscribe or resolve_session, the
        #: conversation's session. A secret: don't show, store or log it.
        self.session = session
        #: What Wakeshift recognised the agent's program as: None until a
        #: subscribe or resolve_session.
        self.recognised: Recognised | None = None
        self._harness = harness
        self._conversation = conversation
        rc = getattr(context, "request_context", context)
        named, meta, headers = _request_info(rc)
        # The request's client name, else identify's.
        self._client = named or client or ""
        self._meta = meta
        self._ua = headers.get("user-agent", "") if headers else ""

    def __repr__(self) -> str:
        return f"ToolCall(tool={self.tool!r}, user={self.user!r}, native={self.native!r})"

    def _wire(self, session: str | None = None) -> dict[str, Any]:
        """The call as Wakeshift takes it."""
        c = _call_of(
            client_name=self._client,
            meta=self._meta,
            user=self.user,
            session=session if session is not None else self.session,
            user_agent=self._ua,
            harness=self._harness,
            conversation=self._conversation,
            tool=self.tool,
        )
        return c.to_json()

    async def _enter(self) -> _Scope:
        s = self._scope
        s.busy += 1
        try:
            await s.lock.acquire()
        except BaseException:
            s.busy -= 1
            raise
        if s.ended:
            s.lock.release()
            s.busy -= 1
            raise CallEnded("wakeshift: the tool call has ended: subscribe before the tool returns")
        return s

    def _leave(self, s: _Scope) -> None:
        s.lock.release()
        s.busy -= 1

    def _check_event(self, event: "WakeEvent") -> None:
        if getattr(event, "_wake", None) is not self._scope.attached._wake:
            raise WakeshiftError(
                404, "unknown_event", f"{getattr(event, 'name', event)} is another app's event: this app doesn't offer it"
            )

    async def resolve_session(self) -> str:
        """Finds or starts the calling conversation's session without
        subscribing it to anything (it doesn't make the conversation
        wakeable), and returns it."""
        s = await self._enter()
        try:
            if s.pending and self.session:
                return self.session
            out = await s.attached._wake._request_app("POST", "/calls/session", {"call": self._wire()})
            s.pending = out.get("pending") or ""
            self.session = out.get("session") or ""
            self.recognised = _recognised(out.get("agent"))
            return self.session
        finally:
            self._leave(s)

    async def subscribe(
        self,
        event: "WakeEvent",
        args: Any,
        *,
        ttl: int | str | None = None,
        reason: str | None = None,
        label: str | None = None,
    ) -> Subscription:
        """Subscribes the calling conversation to event with args, or renews
        its subscription. The tool's reply then carries what the conversation
        needs to be woken (one block for the call, however many subscribes it
        makes), and if the call fails, the subscription is ended. For a native
        caller nothing is made: the block tells it to subscribe itself. Your
        tool decides who may: authorize isn't consulted. A personal event
        needs a user (identify's). ttl: seconds or "30d"; reason: shown in the
        console (at most 200 characters); label: names a new session, for an
        app acting for its agents."""
        s = await self._enter()
        try:
            self._check_event(event)
            _, a_json = event._parse_args(args)
            if self.native:
                s.add_native(event.name, a_json)
                return Subscription(id="", user=self.user or "", native=True)
            if event.personal and not self.user:
                raise WakeshiftError(
                    400,
                    "bad_arguments",
                    f"{event.name} is personal: subscribe needs a user (attach's identify), who the subscription belongs to",
                )
            body: dict[str, Any] = {"call": self._wire(), "name": event.name, "arguments": a_json, "defer_addition": True}
            if s.pending:
                body["pending"] = s.pending
            if ttl is not None:
                body["ttl"] = ttl
            if reason:
                body["reason"] = reason
            if label:
                body["label"] = label
            out = await s.attached._wake._request_app("POST", "/event-subscriptions", body)
            if out.get("pending"):
                s.pending = out["pending"]
            s.armed = True
            self.session = out.get("session") or ""
            rec = _recognised(out.get("agent"))
            self.recognised = rec
            es = out.get("event_subscription") or {}
            created = bool(es.get("created"))
            if created:
                s.created.append(_Created(event.name, a_json, self.session, out.get("pending") or ""))
            return Subscription(
                id=es.get("id", ""),
                user=es.get("user") or self.user or "",
                created=created,
                session=self.session,
                recognised=rec,
            )
        finally:
            self._leave(s)

    async def unsubscribe(self, event: "WakeEvent", args: Any) -> bool:
        """Ends the calling conversation's subscription to event with args,
        reporting whether one ended. For a native caller it ends the user's
        own (webhook) subscriptions to the event with those arguments."""
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
            return bool(out.get("ended"))
        finally:
            self._leave(s)


def _recognised(agent: Any) -> Recognised:
    a = agent if isinstance(agent, Mapping) else {}
    return Recognised(harness=a.get("harness"), via=a.get("via"), claimed=a.get("claimed"))


# ── The handle attach returns ─────────────────────────────────────────────

_WAKE_PARAMS: dict[str, Any] = {}


def _wake_params() -> dict[str, Any]:
    if not _WAKE_PARAMS:
        _WAKE_PARAMS.update(
            {
                SESSION_ARG: Annotated[Optional[str], Field(description=SESSION_DESCRIPTION)],  # noqa: UP045
                HARNESS_ARG: Annotated[Optional[str], Field(description=HARNESS_DESCRIPTION)],  # noqa: UP045
                CONVERSATION_ARG: Annotated[Optional[str], Field(description=CONVERSATION_DESCRIPTION)],  # noqa: UP045
            }
        )
    return _WAKE_PARAMS


def _is_type(annotation: Any, cls: type) -> bool:
    if inspect.isclass(annotation):
        return issubclass(annotation, cls)
    if typing.get_origin(annotation) is Annotated:
        return _is_type(typing.get_args(annotation)[0], cls)
    origin = typing.get_origin(annotation)
    if origin is not None:
        return any(_is_type(a, cls) for a in typing.get_args(annotation) if a is not type(None))
    return False


class Attached:
    """Wakeshift attached to an MCPServer (``wake.attach``): register tools
    with ``@attached.tool()`` so they can subscribe the calling conversation."""

    def __init__(
        self,
        wake: "Wakeshift",
        server: Any,
        identify: "Callable[[Any], Identity | None] | None",
        events: EventsOptions | None,
    ) -> None:
        self._wake = wake
        self._server = server
        self._identify = identify
        self._events = events or EventsOptions()
        self._wrapped: set[str] = set()
        self._warned: set[str] = set()
        self._tasks: set[asyncio.Task[Any]] = set()

    def _warn_once(self, key: str, message: str) -> None:
        if key in self._warned:
            return
        self._warned.add(key)
        self._wake._warn(message)

    def tool(
        self, name: str | None = None, description: str | None = None, **kwargs: Any
    ) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
        """Registers an async tool on the server attach was given, as
        ``server.tool()`` would (its options pass through), with a parameter
        annotated ``ToolCall`` the SDK fills. The tool's schema gains
        session_key, harness and conversation_id, and hides ``call``. The
        reply gets Wakeshift's block; a call that fails ends the
        subscriptions it made."""
        if callable(name):
            raise TypeError("wakeshift: use @attached.tool(), with the parentheses")

        def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
            self._register(fn, name, description, kwargs)
            return fn

        return decorator

    def _register(self, fn: Callable[..., Any], name: str | None, description: str | None, kwargs: dict[str, Any]) -> None:
        from mcp.server.mcpserver import Context

        if not inspect.iscoroutinefunction(fn):
            raise TypeError(f"wakeshift: {getattr(fn, '__name__', fn)}: @attached.tool() takes an async function")
        hints = typing.get_type_hints(fn, include_extras=True)
        sig = inspect.signature(fn)
        call_param = next((p for p in sig.parameters if _is_type(hints.get(p), ToolCall)), None)
        if call_param is None:
            raise TypeError(
                f"wakeshift: {fn.__name__}: @attached.tool() needs a parameter annotated wakeshift.ToolCall (call: ToolCall)"
            )
        ctx_param = next((p for p in sig.parameters if p != call_param and _is_type(hints.get(p), Context)), None)
        own = set(sig.parameters) - {call_param}
        params: list[inspect.Parameter] = []
        annotations: dict[str, Any] = {}
        for p in sig.parameters.values():
            if p.name == call_param:
                continue
            ann = hints.get(p.name, p.annotation)
            params.append(p.replace(annotation=ann))
            if ann is not inspect.Parameter.empty:
                annotations[p.name] = ann
        added_wake = [k for k in (SESSION_ARG, HARNESS_ARG, CONVERSATION_ARG) if k not in own]
        for k in added_wake:
            ann = _wake_params()[k]
            params.append(inspect.Parameter(k, inspect.Parameter.KEYWORD_ONLY, default=None, annotation=ann))
            annotations[k] = ann
        ctx_name = ctx_param or "wakeshift_context"
        if ctx_param is None:
            if ctx_name in own:
                raise TypeError(f"wakeshift: {fn.__name__}: a parameter named {ctx_name} is the SDK's")
            params.append(inspect.Parameter(ctx_name, inspect.Parameter.KEYWORD_ONLY, default=None, annotation=Context))
            annotations[ctx_name] = Context
        # Each kind of parameter in its place (a stable sort keeps their order).
        params.sort(key=lambda p: p.kind)
        ret = hints.get("return", sig.return_annotation)
        if ret is not inspect.Signature.empty:
            annotations["return"] = ret
        wake_output = _is_type(ret, WakeOutput)
        tool_name = name or fn.__name__
        attached = self

        async def wrapper(**kw: Any) -> Any:
            context = kw[ctx_name] if ctx_param is not None else kw.pop(ctx_name, None)
            wake_vals = {
                k: (kw.pop(k, None) if k in added_wake else kw.get(k)) for k in (SESSION_ARG, HARNESS_ARG, CONVERSATION_ARG)
            }
            rc = getattr(context, "request_context", context)
            scope = _SCOPE.get()
            if (
                scope is None
                or scope.attached is not attached
                or scope.call is not None
                or scope.request_id != getattr(rc, "request_id", None)
            ):
                attached._wake._warn(
                    f"wakeshift: {tool_name} ran without its attach's middleware in front: register it with the "
                    "Attached of the server it's on"
                )
                raise RuntimeError("wakeshift: a wrapped tool runs only on the server its attach was given")
            who = attached._identity_of(rc)
            call = ToolCall(
                _SENTINEL,
                scope=scope,
                context=context,
                user=who.user if who else None,
                client=who.client if who else None,
                tool=_tool_of(rc) or tool_name,
                native=False,
                native_conversation=None,
                session=_str(wake_vals[SESSION_ARG]),
                harness=_str(wake_vals[HARNESS_ARG]),
                conversation=_str(wake_vals[CONVERSATION_ARG]),
            )
            call.native, call.native_conversation = attached._native_of(context, rc, call._client)
            scope.call, scope.wake_output = call, wake_output
            try:
                out = await fn(**kw, **{call_param: call})
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
        self._server.add_tool(wrapper, name=tool_name, description=description, **kwargs)
        self._wrapped.add(tool_name)

    # ── Who's calling ─────────────────────────────────────────────────────

    def _identity_of(self, rc: Any) -> "Identity | None":
        return self._identify(rc) if self._identify else None

    def _native_of(self, context: Any, rc: Any, client: str) -> tuple[bool, str | None]:
        """Whether a call comes from a client that subscribes to the app's
        events itself, on stateless servers too (which never see initialize),
        in this order: the app's override (EventsOptions.native); ChatGPT's
        _meta["openai/session"]; a client name (client: initialize's, or
        _meta's, else identify's) or User-Agent of a known native client; a
        client that declares MCP Events. The conversation is ChatGPT's, when
        it names one."""
        _, meta, headers = _request_info(rc)
        conv = meta.get("openai/session")
        conversation = conv if isinstance(conv, str) and conv else None
        if self._events.native is not None and self._events.native(context):
            return True, conversation
        if conversation:
            return True, conversation
        ua = headers.get("user-agent", "") if headers else ""
        if dialect_of(client, ua) == "chatgpt":
            return True, None
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

    # ── The middleware's part ─────────────────────────────────────────────

    async def _tools_call(self, ctx: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
        scope = _Scope(self, getattr(ctx, "request_id", None))
        token = _SCOPE.set(scope)
        try:
            result = await call_next(ctx)
        except BaseException:
            if scope.wrapped:
                await self._rollback_shielded(scope)
            raise
        finally:
            _SCOPE.reset(token)
        if not scope.wrapped:
            return result
        if not isinstance(result, dict) or result.get("isError"):
            await self._rollback_shielded(scope)
            return result
        try:
            return await self._add_block(scope, result)
        except Exception as err:
            # The conversation is subscribed but wouldn't learn how it's
            # woken: the call fails, as one whose tool failed does.
            await self._rollback_shielded(scope)
            assert scope.call is not None
            self._wake._warn(f"wakeshift: the block for {scope.call.tool} failed, so its subscriptions were rolled back: {err}")
            detail = err.detail if isinstance(err, WakeshiftError) else str(err)
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"Wakeshift couldn't finish setting up this conversation's wakes ({detail}). Try again.",
                    }
                ],
                "isError": True,
            }

    async def _rollback_shielded(self, scope: _Scope) -> None:
        """Rolls back under asyncio.shield, so a cancelled request still ends
        what it made."""
        if not scope.created:
            return
        task = asyncio.ensure_future(self._rollback(scope))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            pass  # the rollback carries on

    async def _rollback(self, scope: _Scope) -> None:
        """Ends every subscription the call created and no other call has
        renewed since, with reason rolled_back
        (audited as such, no "ended" notice)."""
        assert scope.call is not None
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
                self._wake._warn(f"wakeshift: rolling back {scope.call.tool}'s subscription to {c.name} failed: {err}")

    async def _add_block(self, scope: _Scope, result: dict[str, Any]) -> dict[str, Any]:
        """result with the call's one block, from /calls/addition. A call that
        names no conversation, and subscribed nothing, gets nothing. Only a
        failure for a call that subscribed (or names native lines) raises."""
        call = scope.call
        assert call is not None
        wire = call._wire()
        if not scope.pending and not scope.native and not wire.get("session") and not wire.get("conversation"):
            if not conversation_key(wire.get("meta")):
                return result
        body: dict[str, Any] = {"call": wire}
        if scope.pending:
            body["pending"] = scope.pending
        if scope.native:
            body["native"] = scope.native
        try:
            add = await self._wake._request_app("POST", "/calls/addition", body)
        except Exception as err:
            if scope.armed or scope.native:
                raise
            self._wake._warn(f"wakeshift: Wakeshift's addition to {call.tool}'s result: {err}")
            return result
        text = str(add.get("text") or "").strip()
        out = merge_block(
            result,
            text,
            str(add.get("trailer") or ""),
            add.get("meta") if isinstance(add.get("meta"), Mapping) else None,
            string=scope.string,
            wake_output=scope.wake_output,
        )
        if text and not scope.wake_output and out.get("structuredContent") is not None:
            self._warn_once(
                "output:" + call.tool,
                f"wakeshift: {call.tool} returns structured content without wakeshift.WakeOutput: hosts that show only "
                "structuredContent won't see how this conversation is woken",
            )
        return out


def _str(v: Any) -> str | None:
    return v.strip() if isinstance(v, str) and v.strip() else None
