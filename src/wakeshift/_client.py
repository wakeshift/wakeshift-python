"""The Wakeshift client for an app (a port of the Node SDK's Wakeshift)."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import secrets
import threading
import time
import weakref
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, TypedDict, TypeVar, cast, overload
from urllib.parse import quote, urlsplit

import httpx2
from mcp_types import CallToolResult, TextContent
from pydantic import BaseModel

from ._errors import FIXES, APIConnectionError, APIError, IdReusedError, UsageError, anchor, usage
from ._key import AppKey, is_test_key, parse_key, sign_assertion
from ._log import _SDK_REQUEST, Log, quiet_http_logs
from ._retry import RETRY_PAUSES, refused
from ._secrets import SessionKey, redact_secrets

if TYPE_CHECKING:
    from ._events import BuiltinTools, Identity, Request, Subscriber, WakeEvent
    from ._toolcall import Attachment, ToolCall, _Scope
    from ._watch import Subscribed, _Watch

_A = TypeVar("_A", bound=BaseModel)
_P = TypeVar("_P", bound=BaseModel)

#: The servers attached so far, in any client: a second attach on one is refused.
_ATTACHED: weakref.WeakSet[Any] = weakref.WeakSet()
_ATTACHED_IDS: set[int] = set()

#: Wakeshift's production core, the default for every key but a wst_test_ one. Override with ``core`` or WAKESHIFT_CORE.
DEFAULT_CORE = "https://app.wakeshift.ai"

#: How long tools/list waits for the app's name (GET /app) before saying
#: "this app" ("wait", seconds), and how long after a failed or unanswered
#: fetch it doesn't ask again ("retry"). Tests shorten them.
_NAME_FETCH: dict[str, float] = {"wait": 3.0, "retry": 60.0}

#: The tool arguments Wakeshift adds: the conversation's session key, the
#: program the agent runs in, and its own ID for the conversation.
SESSION_ARG = "session_key"
HARNESS_ARG = "harness"
CONVERSATION_ARG = "conversation_id"


class ScopeEvent(TypedDict):
    """An event pattern a manifest's agent scope asks for, and what the person is told it's for."""

    pattern: str
    description: str


class WakeLimit(TypedDict):
    """At most max wake-ups in any seconds."""

    seconds: int
    max: int


class _AgentScopeRequired(TypedDict):
    wake: list[ScopeEvent]
    post: list[ScopeEvent]
    #: Days the person's consent lasts.
    ttl_days: int


class AgentScope(_AgentScopeRequired, total=False):
    """What the app may do with the agents in the workspaces it keeps (Manifest's agent)."""

    #: Limits on how often the app may wake the agent, on top of Wakeshift's own.
    wake_limits: list[WakeLimit]
    #: What the person should say to their agent once it's connected; at most 120 characters.
    start_prompt: str


class _EventTypeManifestRequired(TypedDict):
    name: str
    description: str
    arguments: dict[str, Any]
    payload: dict[str, Any]
    topic: str


class EventTypeManifest(_EventTypeManifestRequired, total=False):
    """An event type as the manifest lists it (what define_event makes)."""

    filter: dict[str, str]
    personal: bool
    delete_on_delivery: bool


class Manifest(TypedDict, total=False):
    """What the app tells Wakeshift it may do (publish_manifest)."""

    description: str
    agent: AgentScope
    #: Event types defined elsewhere, not with define_event: Wakeshift gets
    #: these, then the defined ones. Listing a defined one is an error.
    events: list[EventTypeManifest]
    #: Your app wakes its users' agents with what other people and their agents
    #: do there (messages, moves): the consent screen says agents of people they
    #: don't know can wake theirs.
    others_can_wake: bool
    #: Your MCP tools (as your server lists them) that agents' programs can run
    #: without asking each time: tools that only read, and routine ones the
    #: agent calls on every wake that reach only what it already joined. Never
    #: one that deletes, spends, or reaches people the user hasn't chosen. When
    #: a user connects Claude Code or Cursor CLI, Wakeshift offers to allow
    #: exactly these, by name, in the same question as its install. Letters,
    #: digits, _ and -; at most 20.
    preapprove_tools: list[str]


@dataclass
class _Call:
    """Who called one of the app's tools, as Wakeshift needs it (the call
    body of a subscribe, an unsubscribe and an addition)."""

    #: The MCP client's name (initialize's clientInfo.name); empty when unknown.
    client: str
    #: The app's own ID for the person (optional).
    user: str | None = None
    #: The tool call's _meta (Codex sends its thread there).
    meta: dict[str, Any] | None = None
    #: The session handle an earlier result gave the agent.
    session: str | None = None
    #: The MCP request's HTTP User-Agent.
    user_agent: str | None = None
    #: The conversation's ID as the agent passed it (conversation_id).
    conversation: str | None = None
    #: The program the agent says it runs in (harness): a hint, never a credential.
    harness: str | None = None
    #: The tool being called: a connector beacon in its result names it, and
    #: Wakeshift's hooks act on the beacon only from that tool's result.
    tool: str | None = None
    #: The client takes MCP tasks (protocol 2026-07-28 with the tasks
    #: extension), and attach serves the wait tool for it (tasks.py).
    tasks: bool = False
    #: For a native caller: the SHA-256 (hex) of the request's Mcp-Session-Id,
    #: when it has one. An anonymous subscribe voucher is bound to it.
    transport_session: str | None = None

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"client": self.client}
        for k in ("user", "meta", "session", "user_agent", "conversation", "harness", "tool", "transport_session"):
            v = getattr(self, k)
            if v:
                out[k] = v
        if self.tasks:
            out["tasks"] = True
        return out


@dataclass(frozen=True)
class Recognized:
    """What Wakeshift recognized the calling conversation's harness as, and by
    which signal; both None when nothing named one. claimed is what the agent said."""

    harness: str | None = None
    via: str | None = None
    claimed: str | None = None


@dataclass
class _Addition:
    """What Wakeshift adds to a tool result of attach's own tools: text for
    the agent, _meta for its harness."""

    text: str | None = None
    meta: dict[str, Any] | None = None

    @classmethod
    def from_json(cls, v: Mapping[str, Any] | None) -> _Addition:
        v = v or {}
        return cls(text=v.get("text") or None, meta=v.get("meta") or None)


def _apply_addition(a: _Addition, base: str | CallToolResult) -> CallToolResult:
    """Adds Wakeshift's text and _meta to a tool result (a copy)."""
    if isinstance(base, str):
        text = (base.strip() + "\n\n" + (a.text or "")).strip()
        out = CallToolResult(content=[TextContent(type="text", text=text)] if text else [])
    else:
        out = base.model_copy(deep=True)
        if a.text:
            out.content = [*out.content, TextContent(type="text", text=a.text)]
    if a.meta:
        merged = dict(out.meta or {})
        merged.update(a.meta)
        out.meta = merged
    return out


@dataclass(frozen=True)
class Recipient:
    """One recipient of a personal event's occurrence: ``Recipient(user=u)``,
    every subscription that user has to the event with the emit's arguments;
    ``Recipient(user=u, subscription=id)``, exactly that subscription of the
    user's (the id ``call.subscribe`` returned, for the user it returned);
    ``Recipient(native=u)``, only that user's native (MCP Events) subscriptions."""

    user: str | None = None
    subscription: str | None = None
    native: str | None = None


@dataclass(frozen=True)
class Emitted:
    """What an emit reached."""

    #: Conversations it reached: each woken now, or as soon as its agent can
    #: take it.
    matched: int
    #: A retry: an earlier emit, the same as this one, had its id, and this
    #: is that emit's result (nothing new was sent). The id on a different
    #: emit raises IdReusedError instead.
    duplicate: bool = False
    #: The user and native recipients with no live subscription it reached,
    #: whether ``to`` names one or many.
    unmatched: list[Recipient] = field(default_factory=list)
    #: The subscription recipients no longer live (or not that user's, or
    #: this event's): forget them. A duplicate reports the first emit's.
    ended: list[Recipient] = field(default_factory=list)
    #: The text was over 1000 bytes, so Wakeshift trimmed it to 1000 (ending
    #: with "…") and sent the occurrence; logged once per event (text_trimmed).
    text_trimmed: bool = False


def _recipient_wire(r: Recipient) -> str | dict[str, str]:
    if r.native is not None:
        return {"native": r.native}
    if r.subscription is not None:
        return {"user": r.user or "", "subscription": r.subscription}
    return r.user or ""


def _recipients_of(raw: Any) -> list[Recipient]:
    out: list[Recipient] = []
    for r in raw or []:
        if isinstance(r, str):
            out.append(Recipient(user=r))
        elif isinstance(r, Mapping):
            if isinstance(r.get("native"), str):
                out.append(Recipient(native=r["native"]))
            else:
                out.append(Recipient(user=r.get("user"), subscription=r.get("subscription")))
    return out


@dataclass(frozen=True)
class PublishURLSigning:
    """A publish URL's signing secret, as Wakeshift shows it: its fingerprint
    (the first 12 hex digits of its SHA-256), never the secret."""

    fingerprint: str
    set_at: str = ""
    previous_valid_until: str | None = None


@dataclass(frozen=True)
class PublishURL:
    """A publish URL (event.create_publish_url): a capability URL that emits
    its event when a third party posts to it. The URL is a credential: pass
    ``.url`` where it's needed; printing elides its token. signing is set
    when it was made with a signing secret."""

    id: str
    url: str = ""
    event: str = ""
    signing: PublishURLSigning | None = None

    def __repr__(self) -> str:
        return f"PublishURL(id={self.id!r}, url={redact_secrets(self.url)!r}, event={self.event!r}, signing={self.signing!r})"


@dataclass(frozen=True)
class AppInfo:
    """Which app the key is, as Wakeshift says (await wake.app())."""

    #: The app's ID.
    id: str
    #: The app's name.
    name: str
    #: The console's ID for the key, and its name.
    key_id: str = ""
    key_name: str = ""


def _parse_time(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Python < 3.11 accepts at most 6 fractional digits.
    if "." in s:
        head, _, rest = s.partition(".")
        frac = ""
        i = 0
        while i < len(rest) and rest[i].isdigit():
            frac += rest[i]
            i += 1
        s = head + "." + frac[:6] + rest[i:]
    return datetime.fromisoformat(s)


def _request_info(ctx: Any) -> tuple[str, dict[str, Any], Mapping[str, str]]:
    """The client's name, the request's _meta and its HTTP headers, from an MCP
    tool's Context (mcp.server.mcpserver.Context) or a low-level
    ServerRequestContext. Empty where the transport has none."""
    rc = getattr(ctx, "request_context", ctx)
    meta_obj = getattr(rc, "meta", None)
    if meta_obj is None:
        meta: dict[str, Any] = {}
    elif isinstance(meta_obj, Mapping):
        meta = dict(meta_obj)
    else:
        dump = getattr(meta_obj, "model_dump", None)
        meta = dump(by_alias=True, exclude_none=True) if dump else {}
    client = ""
    session = getattr(rc, "session", None)
    params = getattr(session, "client_params", None) if session is not None else None
    info = getattr(params, "client_info", None) if params is not None else None
    if info is not None and getattr(info, "name", ""):
        client = info.name
    if not client:
        ci = meta.get("io.modelcontextprotocol/clientInfo")
        if isinstance(ci, Mapping) and isinstance(ci.get("name"), str):
            client = ci["name"]
    headers: Mapping[str, str] = {}
    req = getattr(rc, "request", None)
    if req is not None and getattr(req, "headers", None) is not None:
        headers = req.headers
    return client, meta, headers


def _tool_of(ctx: Any) -> str:
    """The name of the tool being called, from an MCP tool's Context or a
    low-level ServerRequestContext (its tools/call params); empty if unknown."""
    rc = getattr(ctx, "request_context", ctx)
    if getattr(rc, "method", None) not in (None, "tools/call"):
        return ""
    params = getattr(rc, "params", None)
    name = params.get("name") if isinstance(params, Mapping) else getattr(params, "name", None)
    return name if isinstance(name, str) else ""


def _client_capabilities(ctx: Any) -> dict[str, Any]:
    """The capabilities the client declared, as a dict (empty if unknown)."""
    rc = getattr(ctx, "request_context", ctx)
    session = getattr(rc, "session", None)
    caps = getattr(session, "client_capabilities", None) if session is not None else None
    if caps is None:
        params = getattr(session, "client_params", None) if session is not None else None
        caps = getattr(params, "capabilities", None) if params is not None else None
    if caps is None:
        return {}
    dump = getattr(caps, "model_dump", None)
    return dump(by_alias=True, exclude_none=True) if dump else dict(caps)


def _arg(args: Mapping[str, Any] | None, name: str) -> str | None:
    v = (args or {}).get(name)
    if isinstance(v, SessionKey):
        return v.reveal()
    return v if isinstance(v, str) else None


def _call_of(
    *,
    client_name: str,
    meta: Mapping[str, Any] | None = None,
    user: str | None = None,
    session: str | None = None,
    user_agent: str | None = None,
    harness: str | None = None,
    conversation: str | None = None,
    tool: str | None = None,
    tasks: bool = False,
    transport_session: str | None = None,
) -> _Call:
    """Packs a tool call: tool is the name of the tool being called
    (Wakeshift's hooks act on a connector beacon only from that tool's result)."""
    return _Call(
        client=client_name or "",
        user=user or None,
        meta=dict(meta) if meta else None,
        session=session or None,
        user_agent=user_agent or None,
        harness=(harness or "").strip() or None,
        conversation=(conversation or "").strip() or None,
        tool=(tool or "").strip() or None,
        tasks=tasks,
        transport_session=transport_session or None,
    )


#: The core's feature: it hands out subscribe vouchers for tools-only events
#: to native callers (docs/subscribe-vouchers.md).
SUBSCRIBE_VOUCHERS = "subscribe_vouchers"


def _transport_session(headers: Mapping[str, str] | None) -> str | None:
    """The SHA-256 (hex) of a request's Mcp-Session-Id header, or None without
    one: what binds an anonymous subscribe voucher to the transport session it
    was handed out in."""
    sid = (headers.get("mcp-session-id", "") if headers else "").strip()
    return hashlib.sha256(sid.encode()).hexdigest() if sid else None


TASKS_EXTENSION = "io.modelcontextprotocol/tasks"
_MODERN = "2026-07-28"


def _takes_tasks(meta: Mapping[str, Any] | None, headers: Mapping[str, str] | None = None) -> bool:
    """Whether a request is protocol 2026-07-28 (its _meta or its
    MCP-Protocol-Version header says) and declares the MCP tasks extension in
    its own capabilities: a client that takes MCP tasks (tasks.py)."""
    meta = meta or {}
    version = meta.get("io.modelcontextprotocol/protocolVersion")
    if version is None and headers:
        version = headers.get("mcp-protocol-version")
    if version != _MODERN:
        return False
    caps = meta.get("io.modelcontextprotocol/clientCapabilities")
    ext = caps.get("extensions") if isinstance(caps, Mapping) else None
    return isinstance(ext, Mapping) and TASKS_EXTENSION in ext


def _call(
    ctx: Any, *, user: str | None, args: Mapping[str, Any] | None, tool: str, client: str | None = None, wait: bool = False
) -> _Call:
    """Packs the tool call from an MCP request context: its _meta and
    User-Agent, the client's name (the request's, else client: identify's),
    the user, and session_key, harness and conversation_id from args (the
    tool's arguments). It says the client takes tasks only if attach serves
    the wait tool (wait)."""
    name, meta, headers = _request_info(ctx)
    return _call_of(
        client_name=name or client or "",
        meta=meta,
        user=user,
        session=_arg(args, SESSION_ARG),
        user_agent=headers.get("user-agent", "") if headers else "",
        harness=_arg(args, HARNESS_ARG),
        conversation=_arg(args, CONVERSATION_ARG),
        tool=tool,
        tasks=wait and _takes_tasks(meta, headers),
    )


#: The hosts plain http is allowed to: loopback only.
_LOOPBACK = ("localhost", "127.0.0.1", "::1")


def _core_url(core: str) -> str:
    """The core's base URL, checked: https, or http to a loopback host."""
    u = urlsplit(core.strip())
    host = (u.hostname or "").lower()
    if not host or u.scheme not in ("https", "http") or (u.scheme == "http" and host not in _LOOPBACK):
        raise usage(
            "config",
            "the core URL (core= or WAKESHIFT_CORE) must be https:// (plain http only to localhost, 127.0.0.1 or ::1)",
        )
    return core.strip().rstrip("/")


class _Loop:
    """What the client keeps per running event loop: its own HTTP client
    (created on first use), and the locks that order its sends."""

    __slots__ = ("app_lock", "http", "manifest_lock", "watch_checks")

    def __init__(self) -> None:
        from ._watch import MAX_CONCURRENT_CHECKS

        self.http: httpx2.AsyncClient | None = None
        self.app_lock = asyncio.Lock()
        self.manifest_lock = asyncio.Lock()
        #: The watches' checks running at once (event.watch).
        self.watch_checks = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)


#: How long the core keeps a subscribe voucher redeemable (the core's
#: VoucherTTL), and so how long the context of the call that handed it out is kept.
VOUCHER_TTL = 4 * 3600.0
#: The most voucher contexts a client keeps at once (the oldest go first).
VOUCHER_CONTEXTS = 1000

#: The bytes an event's text may take on Claude Code's 480-byte Monitor line
#: and still leave its payload room: Wakeshift's own fields take about 230
#: (warnings.json monitor_line_long).
MONITOR_TEXT_BUDGET = 200


def _monitor_bytes(text: str) -> int:
    """text's length on the Monitor line: UTF-8 bytes of its JSON-escaped string, without the quotes."""
    return len(json.dumps(text, ensure_ascii=False).encode("utf-8")) - 2


class Wakeshift:
    """The Wakeshift client for an app: define events, attach to your MCP
    server so your tools can subscribe the conversations calling them, and
    emit.

    ``Wakeshift()`` reads the app key from ``WAKESHIFT_KEY`` and does no I/O.
    Without a key, Wakeshift is off (``enabled`` is False): the client keeps
    its shape, so nothing guards a call site; emits and subscribes do
    nothing, and the server runs as before. A malformed key raises, as does
    a missing one with ``required=True``.

    The client may be shared across threads and event loops: it keeps one
    HTTP client per running loop (a caller's ``http_client`` is used as it
    is, and never closed). ``await wake.aclose()`` closes the current loop's
    own; it's never required."""

    def __init__(
        self,
        key: str | None = None,
        *,
        core: str | None = None,
        required: bool = False,
        timeout: float = 20.0,
        max_retries: int = 2,
        http_client: httpx2.AsyncClient | None = None,
    ) -> None:
        self._log = Log()
        from_env = key is None
        if from_env:
            key = os.environ.get("WAKESHIFT_KEY", "")
        source = "WAKESHIFT_KEY" if from_env else "the key passed to Wakeshift()"
        raw = (key or "").strip()
        if not isinstance(timeout, int | float) or isinstance(timeout, bool) or timeout <= 0:
            raise usage("config", "timeout must be a number of seconds above zero")
        if not isinstance(max_retries, int) or isinstance(max_retries, bool) or max_retries < 0:
            raise usage("config", "max_retries must be zero or more")
        if not raw and required:
            raise usage("config", f"{source} is missing or empty, and required=True needs a key")
        self._key: AppKey | None = None
        if raw:
            try:
                self._key = parse_key(raw)
            except ValueError:
                # Never any part of the key, nor its length.
                raise usage(
                    "config",
                    f"{source} isn't a Wakeshift app key: it's cut short, mistyped, or another kind of token; "
                    "copy the app key again from the console",
                ) from None
        # Test keys come from staging and local consoles, and one sent to
        # production fails in a way that's hard to read: ask for the core.
        c = (core or "").strip() or os.environ.get("WAKESHIFT_CORE", "").strip()
        if not c and is_test_key(raw):
            raise usage(
                "config",
                "a test key has no default core: set WAKESHIFT_CORE, or pass core=, to the address of the console "
                "the key came from (for example https://app.staging.wakeshift.ai)",
            )
        self.core = _core_url(c or DEFAULT_CORE)
        # An older key names its app; a current one's is Wakeshift's to say (app()).
        self._app_id: str | None = self._key.app_id if self._key else None
        self._app: AppInfo | None = None
        #: When the name fetch for tools/list last failed or went unanswered (monotonic).
        self._name_failed_at: float | None = None
        self._timeout = float(timeout)
        self._max_retries = max_retries
        self._http = http_client
        self._loops: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, _Loop] = weakref.WeakKeyDictionary()
        self._loops_lock = threading.Lock()
        self._retry_pauses = RETRY_PAUSES
        self._events: dict[str, WakeEvent[Any, Any]] = {}
        # What publish_manifest was last given (None: nothing yet); whether
        # this process serves the app's tools (attach: only then is the
        # manifest sent automatically); whether Wakeshift has the manifest
        # for the events defined now; how many define_events there were;
        # how many manifests Wakeshift has taken from this process.
        self._manifest_base: dict[str, Any] | None = None
        self._serving = False
        self._published = False
        self._manifest_version = 0
        self._sends = 0
        #: What the core says it offers, from its last manifest answer
        #: (features): an old core says nothing, which is none.
        self._features: frozenset[str] = frozenset()
        #: _ensure_events' sends that failed, and the last one's error: a call
        #: that waited for a send that failed raises it, rather than sending again.
        self._ensure_failures = 0
        self._ensure_error: Exception | None = None
        #: The tool calls being answered, by their request context, for
        #: current_call(request) from code that lost the async context.
        self._scopes: dict[int, _Scope] = {}
        #: The running watches, by event and canonical arguments (event.watch).
        self._watches: dict[tuple[str, str], _Watch] = {}
        #: on_subscribe runs still going, kept so they aren't collected.
        self._background: set[asyncio.Task[Any]] = set()
        #: The context= of the call that handed out each subscribe voucher, by
        #: the voucher's SHA-256 (never the voucher), with when it expires:
        #: Subscribed.context for its redemption in this process.
        self._voucher_contexts: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._voucher_lock = threading.Lock()
        if self._key is None:
            self._log("no_key", "no app key, so Wakeshift is off: nothing is subscribed or emitted")
        else:
            quiet_http_logs()

    def __repr__(self) -> str:
        return f"Wakeshift(app={self._app_id!r}, core={self.core!r}, enabled={self.enabled})"

    @property
    def enabled(self) -> bool:
        """Whether Wakeshift is on: the client has an app key."""
        return self._key is not None

    def _need_key(self) -> AppKey:
        """The key, for a call that must ask Wakeshift; a disabled client raises."""
        if self._key is None:
            raise usage("disabled", "Wakeshift is off (no app key), so this call can't ask it anything")
        return self._key

    # ── Per event loop ─────────────────────────────────────────────────────

    def _loop(self) -> _Loop:
        loop = asyncio.get_running_loop()
        with self._loops_lock:
            st = self._loops.get(loop)
            if st is None:
                # A client whose loop closed is dropped, not closed.
                for old in [lp for lp in self._loops if lp.is_closed()]:
                    del self._loops[old]
                st = self._loops[loop] = _Loop()
            return st

    def _new_http(self) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(timeout=self._timeout)

    # ── Subscribe vouchers' contexts ───────────────────────────────────────

    def _remember_voucher(self, token: str, context: Any) -> None:
        """Keeps context for the voucher token handed out (by its hash), until
        it's redeemed or expires, at most VOUCHER_CONTEXTS of them."""
        key = hashlib.sha256(token.encode()).hexdigest()
        now = time.monotonic()
        with self._voucher_lock:
            self._drop_expired_vouchers(now)
            self._voucher_contexts.pop(key, None)
            self._voucher_contexts[key] = (context, now + VOUCHER_TTL)
            while len(self._voucher_contexts) > VOUCHER_CONTEXTS:
                self._voucher_contexts.popitem(last=False)

    def _take_voucher_context(self, token: str) -> Any:
        """The context kept for exactly this voucher, forgotten: None if this
        process handed none out with it, or it expired."""
        key = hashlib.sha256(token.encode()).hexdigest()
        with self._voucher_lock:
            self._drop_expired_vouchers(time.monotonic())
            kept = self._voucher_contexts.pop(key, None)
        return kept[0] if kept is not None else None

    def _drop_expired_vouchers(self, now: float) -> None:
        for k in [k for k, (_, until) in self._voucher_contexts.items() if until <= now]:
            del self._voucher_contexts[k]

    async def aclose(self) -> None:
        """Stops the watches running on the current event loop (their tasks
        end ``stopped``) and cancels on_subscribe work still running there,
        then closes the HTTP client this client opened on that loop (never one
        passed as ``http_client``). Idempotent: call it from a server's
        lifespan, or use ``async with Wakeshift() as wake``."""
        from ._watch import stop_all

        await stop_all(self)
        with self._voucher_lock:
            self._voucher_contexts.clear()
        loop = asyncio.get_running_loop()
        with self._loops_lock:
            st = self._loops.get(loop)
            http, st_http = (st.http, st) if st is not None else (None, None)
            if st_http is not None:
                st_http.http = None
        if http is not None:
            await http.aclose()

    async def __aenter__(self) -> Wakeshift:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ── The app ────────────────────────────────────────────────────────────

    @property
    def app_id(self) -> str:
        """The app's ID. An older key (wst_test_…, wst_live_…) names it; for
        a current key, which doesn't, call await wake.app() first."""
        self._need_key()
        if self._app_id is None:
            raise usage("config", "this key doesn't name its app: call await wake.app() first, then read app_id")
        return self._app_id

    async def app(self) -> AppInfo:
        """Which app the key is, as Wakeshift says (GET /api/apps/v1/app):
        asked once, then cached. After it, app_id is the app's ID whatever
        the key's format."""
        self._need_key()
        if self._app is not None:
            return self._app
        async with self._loop().app_lock:
            if self._app is None:
                out = await self._request("GET", "/app")
                k = out.get("key")
                key: dict[str, Any] = k if isinstance(k, dict) else {}
                if not isinstance(out.get("id"), str) or not out["id"]:
                    raise APIError("Wakeshift didn't say which app this key is", code="bad_response", status=200)
                info = AppInfo(
                    id=out["id"],
                    name=str(out.get("name") or ""),
                    key_id=str(key.get("key_id") or ""),
                    key_name=str(key.get("name") or ""),
                )
                if self._app_id is None:
                    self._app_id = info.id
                self._app = info
            return self._app

    async def _app_name(self) -> str:
        """The app's name for tools/list (app()'s), or "" when Wakeshift
        doesn't answer within _NAME_FETCH["wait"]: tools/list doesn't wait
        longer for it, and after a failure doesn't ask again for
        _NAME_FETCH["retry"], so a slow or failing Wakeshift neither stalls
        nor multiplies tools/list."""
        failed = self._name_failed_at
        if failed is not None and time.monotonic() - failed < _NAME_FETCH["retry"]:
            return ""
        try:
            return (await asyncio.wait_for(self.app(), _NAME_FETCH["wait"])).name.strip()
        except Exception:  # an error, or no answer in time
            self._name_failed_at = time.monotonic()
            return ""

    def _assertion(self) -> str:
        return sign_assertion(self._need_key(), self.core, app_id=self._app_id)

    # ── Publish URLs, users and the manifest ───────────────────────────────

    async def revoke_publish_url(self, id: str) -> None:
        """Stops an event's publish URL working (410 from then on)."""
        await self._request("DELETE", f"/events/publish-urls/{quote(id, safe='')}")

    async def forget_user(self, user: str) -> bool:
        """Deletes everything Wakeshift keeps for one of the app's users. False if there was nothing to forget."""
        return bool((await self._request("POST", "/users/forget", {"user": user}))["forgotten"])

    async def publish_manifest(self, manifest: Manifest) -> Manifest:
        """Tells Wakeshift what the app may do, replacing what it said before:
        manifest, with the events you defined (define_event) after its own
        events. An event both defined and listed in manifest's events is an
        error (ValueError), and nothing is sent. You needn't call it: the SDK
        sends the manifest automatically, merged into the one Wakeshift has (a
        process that serves your tools on its first call about events, any
        process on an emit of an event Wakeshift doesn't know yet). Call it to
        remove an event or clear a setting. Once called, manifest's settings are what every automatic
        send carries (even if this one failed), with the events defined by
        then. Returns the manifest as Wakeshift keeps it."""
        version = self._manifest_version  # before the body: a later define_event is sent again
        body = self._manifest_body(manifest)
        self._manifest_base = dict(manifest)
        # One send at a time, explicit or automatic: the last manifest
        # Wakeshift gets is the newest.
        async with self._manifest_send_lock():
            try:
                out = await self._request("PUT", "/manifest", body)
            except BaseException:
                self._published = False  # the next automatic send tries again
                raise
            self._published = self._manifest_version == version
            self._sends += 1
        self._log_changes(out)
        self._note_features(out)
        return cast(Manifest, out)

    def _log_changes(self, out: Mapping[str, Any]) -> None:
        """The manifest response's changes: an event Wakeshift moved to tools
        only, and how many subscriptions made other ways it ended."""
        for c in out.get("changed") or []:
            if isinstance(c, Mapping) and c.get("to") == "tools" and isinstance(c.get("event"), str):
                ended = c.get("ended") if isinstance(c.get("ended"), int) else 0
                self._log(
                    "event_closed",
                    f"{c['event']} now lets only this app's tools subscribe, and Wakeshift ended {ended} subscriptions "
                    "made other ways",
                    event=c["event"],
                    ended=ended,
                )

    def _manifest_send_lock(self) -> asyncio.Lock:
        return self._loop().manifest_lock

    def _manifest_body(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        """The manifest as Wakeshift gets it: its events, then the defined ones,
        refusing a name in both."""
        listed = list(manifest.get("events") or [])
        for name in self._events:
            if any(isinstance(e, Mapping) and e.get("name") == name for e in listed):
                raise usage("config", f"event {name} is defined with define_event and listed in the manifest's events: keep one")
        events = listed + [e.manifest for e in self._events.values()]
        body = {k: v for k, v in manifest.items() if k != "events"}
        if events:
            body["events"] = events
        return body

    async def _send_manifest(self) -> None:
        """The automatic send: the settings the app last published (only those
        it set), with the defined events, merged into the manifest Wakeshift
        has, so it never removes an event or clears a setting another process
        sent."""
        body = {
            k: v
            for k, v in self._manifest_body(self._manifest_base or {}).items()
            if v is not None and v != "" and v is not False and v != []
        }
        if "events" not in body:
            return  # nothing to add (attach refuses with no events, but never send an empty list)
        out = await self._request("POST", "/manifest/merge", body)
        self._sends += 1
        self._log_changes(out)
        self._note_features(out)

    def _note_features(self, out: Mapping[str, Any]) -> None:
        f = out.get("features")
        self._features = frozenset(x for x in f if isinstance(x, str)) if isinstance(f, list) else frozenset()

    async def _vouchers_offered(self) -> bool:
        """Whether the core hands out subscribe vouchers: its manifest answer
        reports subscribe_vouchers. Sends the manifest first if it hasn't been;
        a failed send is no."""
        try:
            await self._ensure_events()
        except Exception:
            return False
        return SUBSCRIBE_VOUCHERS in self._features

    async def _resend_manifest(self, seen: int) -> None:
        """An emit's answer to unknown_event: Wakeshift doesn't have the event
        (no process has sent a manifest with it yet, or a publish since
        removed it), so this process sends its own, merged, unless a send
        finished since the emit went (seen: the sends then). One send at a
        time, so emits that fail together send it once."""
        async with self._manifest_send_lock():
            if self._sends != seen:
                return
            version = self._manifest_version
            await self._send_manifest()
            if self._manifest_version == version:
                self._published = True

    # ── Events (MCP Events, natively or bridged) ───────────────────────────

    @overload
    def define_event(
        self,
        name: str,
        *,
        description: str,
        args: type[_A],
        payload: type[_P],
        text: Callable[[_P, _A], str] | None = None,
        personal: bool = False,
        authorize: Callable[[Subscriber, _A], bool | Awaitable[bool]] | None = None,
        public: bool = False,
        delete_on_delivery: bool = False,
        title: str | None = None,
        topic: str | None = None,
        filter: Mapping[str, str] | None = None,
        on_subscribe: Callable[[Subscribed[_A]], None | Awaitable[None]] | None = None,
    ) -> WakeEvent[_A, _P]: ...

    @overload
    def define_event(
        self,
        name: str,
        *,
        description: str,
        args: Mapping[str, Any],
        payload: Mapping[str, Any],
        text: Callable[[dict[str, Any], dict[str, Any]], str] | None = None,
        personal: bool = False,
        authorize: Callable[[Subscriber, dict[str, Any]], bool | Awaitable[bool]] | None = None,
        public: bool = False,
        delete_on_delivery: bool = False,
        title: str | None = None,
        topic: str | None = None,
        filter: Mapping[str, str] | None = None,
        on_subscribe: Callable[[Subscribed[dict[str, Any]]], None | Awaitable[None]] | None = None,
    ) -> WakeEvent[dict[str, Any], dict[str, Any]]: ...

    def define_event(
        self,
        name: str,
        *,
        description: str,
        args: Any,
        payload: Any,
        text: Callable[[Any, Any], str] | None = None,
        personal: bool = False,
        authorize: Callable[[Subscriber, Any], bool | Awaitable[bool]] | None = None,
        public: bool = False,
        delete_on_delivery: bool = False,
        title: str | None = None,
        topic: str | None = None,
        filter: Mapping[str, str] | None = None,
        on_subscribe: Callable[[Subscribed[Any]], None | Awaitable[None]] | None = None,
    ) -> WakeEvent[Any, Any]:
        """Defines one of the app's event types: what a subscriber picks
        (``args``), what each occurrence carries (``payload``), each a pydantic
        model or a JSON Schema dict, and optionally the line an agent reads
        first (``text(payload, args)``; without it, the description and the
        arguments). Who may subscribe: only the app's own tools, unless the
        event is ``personal`` (each subscription its subscriber's), has an
        ``authorize``, or is ``public``. ``on_subscribe`` runs once for each
        new subscription, whichever way it was made (a tool, subscribe_events,
        a native client), in the background, with a ``Subscribed``: where a
        service has no webhook, start ``event.watch`` there. Define every
        event before attach::

            crawl_finished = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
            await crawl_finished.emit(Job(job=job), Crawl(status="completed", pages=3))
        """
        from ._events import WakeEvent

        if name in self._events:
            raise usage("config", f"event {name} is defined twice")
        ev = WakeEvent[Any, Any](
            self,
            name,
            description=description,
            args=args,
            payload=payload,
            text=text,
            personal=personal,
            authorize=authorize,
            public=public,
            delete_on_delivery=delete_on_delivery,
            title=title,
            topic=topic,
            filter=filter,
            on_subscribe=on_subscribe,
        )
        self._events[name] = ev
        self._published = False  # the manifest changed: sent again when next needed
        self._manifest_version += 1
        return ev

    def attach(
        self,
        server: Any,
        *,
        identify: Callable[[Request], Identity | None | Awaitable[Identity | None]] | None = None,
        require_user: bool = False,
        wrap: Mapping[str, Sequence[WakeEvent[Any, Any]]] | Callable[[str], Sequence[WakeEvent[Any, Any]] | None] | None = None,
        builtin_tools: BuiltinTools | Literal[False] | None = None,
        is_native_client: Callable[[Request], bool] | None = None,
    ) -> Attachment:
        """Attaches Wakeshift to an MCP server before you run it: an
        MCPServer, a FastMCP server (4, or 2.12+ and 3.x without native MCP
        Events), or a low-level Server. It works at the
        protocol layer, so it wraps none of your callbacks: it serves the
        events you defined (MCP Events for clients that speak it; the
        subscribe_events, unsubscribe_events and list_subscriptions tools),
        gives your tools that subscribe Wakeshift's arguments and block, and
        takes Wakeshift's arguments out of every other tool call. Register a
        tool that subscribes with ``@attached.tool(subscribes=[event])``, or
        name tools registered elsewhere in ``wrap`` and find their call with
        ``wake.current_call()``. ``wrap`` maps tool names to the events each
        subscribes to, or is a function of a tool's name returning its events
        (None: it doesn't subscribe), asked on every tools/list and tools/call
        for a tool not otherwise declared, for tools named at run time::

            attached = wake.attach(server, identify=identify)
            wake.attach(lowlevel_server, wrap={"start_crawl": [crawl_finished]})
            wake.attach(lowlevel_server, wrap=lambda tool: [crawl_finished] if tool.startswith("crawl_") else None)

        ``identify`` tells who is behind a request (a ``wakeshift.Request``:
        its verified ``token``, its headers), from the app's own sign-in; it
        may be async, runs at most once per request and only when Wakeshift
        needs it. If it raises or takes over 10 seconds, Wakeshift's part of
        the request is refused (your tool still runs). ``require_user=True``
        refuses an anonymous caller the same way, so a session_key is never
        a bearer. ``is_native_client`` tells, for callers the SDK can't
        recognize (an app's own house agents), whether the client subscribes
        to events itself. On a disabled client, attach adds nothing."""
        with self._config_logged():
            return self._attach(
                server,
                identify=identify,
                require_user=require_user,
                wrap=wrap,
                builtin_tools=builtin_tools,
                is_native_client=is_native_client,
            )

    @contextlib.contextmanager
    def _config_logged(self) -> Iterator[None]:
        """A configuration mistake raised while attaching or registering a tool
        is logged too (config), since a framework may turn the raise into a
        bare HTTP 500; the message is the error's own, which names no secret."""
        try:
            yield
        except UsageError as err:
            if err.code == "config":
                self._log("config", err._what or str(err).removeprefix("wakeshift: "))
            raise
        except TypeError as err:
            if str(err).startswith("wakeshift: "):
                self._log("config", str(err).removeprefix("wakeshift: "))
            raise

    def _attach(
        self,
        server: Any,
        *,
        identify: Callable[[Request], Identity | None | Awaitable[Identity | None]] | None,
        require_user: bool,
        wrap: Mapping[str, Sequence[WakeEvent[Any, Any]]] | Callable[[str], Sequence[WakeEvent[Any, Any]] | None] | None,
        builtin_tools: BuiltinTools | Literal[False] | None,
        is_native_client: Callable[[Request], bool] | None,
    ) -> Attachment:
        from ._events import attach_events
        from ._hosts import check_sdks, host_of
        from ._toolcall import Attachment

        check_sdks(server)
        try:
            seen = server in _ATTACHED
        except TypeError:
            seen = id(server) in _ATTACHED_IDS
        if seen:
            raise usage("config", "this server is attached already: attach once per server")
        host = host_of(server)
        try:
            _ATTACHED.add(server)
        except TypeError:
            _ATTACHED_IDS.add(id(server))
        if self._key is None:
            # Wakeshift is off: an attachment that adds nothing, so the server runs as before.
            off = Attachment(self, host, None, off=True)
            off._set_wrap(wrap)  # still checked
            return off
        if identify is None:
            self._log("no_identify", "attach has no identify, so a session_key is a bearer for its conversation")
        attached = attach_events(
            self,
            server,
            list(self._events.values()),
            identify=identify,
            require_user=require_user,
            is_native_client=is_native_client,
            builtin_tools=builtin_tools,
            wrap=wrap,
            host=host,
        )
        self._serving = True  # the manifest is sent automatically from now on
        return attached

    def current_call(self, request: Any = None) -> ToolCall:
        """The ToolCall of the tool call being answered, for a tool that
        subscribes registered elsewhere (named in attach's ``wrap``), from
        anywhere in its handler's async call chain. Code that lost the async
        context passes the request's context (a low-level handler's ctx, or
        a tool's Context). Outside a tool call it raises UsageError
        ``no_call``; on a disabled client it returns a disabled call."""
        from ._toolcall import _SCOPE, _TEST_CALL

        given = _TEST_CALL.get()
        if given is not None and given[0] is self:
            return given[1]  # wakeshift.testing.with_call
        if self._key is None:
            return self._disabled_call("", None)
        scope = _SCOPE.get()
        if request is not None:
            raw = getattr(request, "raw", request)
            rc = getattr(raw, "request_context", raw)
            rc = getattr(rc, "_srctx", rc)  # FastMCP's request context wraps the MCP SDK's
            scope = self._scopes.get(id(rc), scope)
        if scope is None or scope.attached._wake is not self:
            raise usage("no_call", "current_call() was called outside a tool call")
        if scope.call is None:
            raise usage(
                "no_call",
                f"current_call() was called in {scope.tool or 'a tool'}, which isn't a tool that subscribes: name it in "
                "attach's wrap, or register it with @attached.tool(subscribes=[...])",
            )
        return scope.call

    def _disabled_call(self, tool: str, declared: list[WakeEvent[Any, Any]] | None) -> ToolCall:
        """A disabled client's call: its subscribes run their local checks and ask nothing."""
        from ._toolcall import _SENTINEL, ToolCall

        return ToolCall(
            _SENTINEL,
            scope=None,
            context=None,
            user=None,
            tool=tool,
            native=False,
            native_conversation=None,
            session=None,
            harness=None,
            conversation=None,
            declared=declared,
            checked=declared is not None,
            wake=self,
        )

    async def _ensure_events(self) -> None:
        """Sends the manifest with the defined events, once (again after a
        define_event), from a process that serves the app's tools (attach). A
        process that only emits sends it only when Wakeshift doesn't know an
        event it emits (_emit_event): its events may be fewer, or older, than
        its server's, and a merge never removes an event it leaves out.

        Calls that wait for one send share its outcome: if it fails, they
        raise its error rather than each sending again, one after another
        (as the TypeScript SDK's share one send). The next call sends again."""
        if self._published or not self._serving:
            return
        seen = self._ensure_failures
        async with self._manifest_send_lock():
            if self._published:
                return  # sent while this one waited
            if self._ensure_failures != seen and self._ensure_error is not None:
                raise self._ensure_error  # the send this one waited for failed
            version = self._manifest_version
            try:
                await self._send_manifest()
            except Exception as err:
                self._ensure_failures += 1
                self._ensure_error = err
                raise
            if self._manifest_version == version:
                self._published = True

    async def _emit_event(
        self,
        *,
        name: str,
        arguments: Any,
        payload: Any,
        text: str,
        id: str | None = None,
        to: str | list[Recipient] | None = None,
    ) -> Emitted:
        if self._key is None:
            return Emitted(matched=0)  # Wakeshift is off: checked, and sent nowhere
        # Text over 1000 bytes is trimmed by Wakeshift, which text_trimmed reports instead.
        if len(text.encode("utf-8")) <= 1000 and _monitor_bytes(text) > MONITOR_TEXT_BUDGET:
            self._log(
                "monitor_line_long",
                f"{name}'s text is over {MONITOR_TEXT_BUDGET} bytes, so it leaves the payload no room on Claude Code's Monitor line",
                event=name,
            )
        await self._ensure_events()
        body: dict[str, Any] = {"name": name, "arguments": arguments, "payload": payload, "text": text}
        if id:
            body["id"] = id
        elif self._max_retries:
            # An emit that may be sent again carries an id, the same on each
            # attempt, so a retry never wakes anyone twice.
            body["id"] = "sdk-" + secrets.token_hex(12)
        if isinstance(to, str):
            body["to"] = to
        elif to:
            # One plain user goes as a string (the single-user path), else a list.
            if len(to) == 1 and to[0].subscription is None and to[0].native is None:
                body["to"] = to[0].user
            else:
                body["to"] = [_recipient_wire(r) for r in to]
        # Wakeshift may not have the event yet (an emit at startup, before
        # any agent has connected to the app's server): send this process's
        # manifest, merged, then emit once more.
        seen = self._sends
        try:
            try:
                out = await self._request("POST", "/events/emit", body)
            except APIError as e:
                if e.status != 404 or e.code != "unknown_event":
                    raise
                await self._resend_manifest(seen)
                out = await self._request("POST", "/events/emit", body)
        except APIError as e:
            if e.status == 409 and e.code == "id_reused":
                raise IdReusedError(e.detail, request_id=e.request_id) from e
            raise
        trimmed = out.get("text_trimmed") is True
        if trimmed:
            self._log("text_trimmed", f"{name}'s text was over 1000 bytes, so Wakeshift trimmed it", event=name)
        return Emitted(
            matched=int(out["matched"]),
            duplicate=bool(out.get("duplicate")),
            unmatched=_recipients_of(out.get("unmatched")),
            ended=_recipients_of(out.get("ended")),
            text_trimmed=trimmed,
        )

    async def _request_app(
        self, method: str, path: str, body: Any = None, *, timeout: float | None = None, about_event: bool = False
    ) -> dict[str, Any]:
        """A call to the app API, after the manifest with the defined events is
        published. timeout: a longer one for this request (a held tasks/get).
        about_event: a call about one event (a publish URL, its status), which
        a fresh process may make before Wakeshift has its manifest: on
        unknown_event it sends its own, merged, and asks once more (#62)."""
        self._need_key()
        await self._ensure_events()
        seen = self._sends
        try:
            return await self._request(method, path, body, timeout=timeout)
        except APIError as e:
            if not about_event or e.status != 404 or e.code != "unknown_event":
                raise
        await self._resend_manifest(seen)
        return await self._request(method, path, body, timeout=timeout)

    # ── Users' workspaces ──────────────────────────────────────────────────

    async def workspace(self, user: str, name: str) -> str:
        """The workspace Wakeshift keeps for one of the app's users, created on
        first use. That user's conversations are kept there."""
        return (await self._request("POST", "/workspaces", {"external_id": user, "name": name}))["workspace"]

    async def add_agent(self, workspace: str, agent: str, display_name: str) -> str:
        """Creates an agent in one of the app's workspaces (if new), and its grant. Returns the grant's ID."""
        out = await self._request(
            "POST", f"/workspaces/{quote(workspace, safe='')}/agents", {"agent": agent, "display_name": display_name}
        )
        return out["grant"]

    async def check_agent(self, workspace: str, agent: str) -> None:
        """Checks that one of the app's agents still has a live grant. Raises
        APIError no_grant once revoked, unknown_workspace once forgotten."""
        await self._request("POST", f"/workspaces/{quote(workspace, safe='')}/agents/{quote(agent, safe='')}/token")

    # ── HTTP ───────────────────────────────────────────────────────────────

    async def _send(self, method: str, url: str, *, timeout: float | None = None, **kw: Any) -> tuple[httpx2.Response, str]:
        # A request the load balancer refused during a rollout is sent again (retry.py).
        limit = max(timeout or 0, self._timeout)
        h = self._http
        if h is None:
            st = self._loop()
            if st.http is None:
                st.http = self._new_http()
            h = st.http
        try:
            return await self._send_with(h, method, url, limit, **kw)
        except httpx2.TimeoutException as err:
            raise APIConnectionError(
                f"wakeshift: Wakeshift at {self.core} didn't answer in time. {FIXES['timeout']} {anchor('timeout')}",
                code="timeout",
            ) from err
        except httpx2.HTTPError as err:
            raise self._unreachable(str(err) or type(err).__name__) from err

    async def _send_with(
        self, h: httpx2.AsyncClient, method: str, url: str, timeout: float, **kw: Any
    ) -> tuple[httpx2.Response, str]:
        """Sends a request, again (up to max_retries times) when it never
        reached Wakeshift: the load balancer refused it, or the connection
        failed. An answer that may have come from Wakeshift, or a timeout,
        isn't sent again here: its outcome is unknown (an emit carries an id,
        so a retry of it wakes nobody twice)."""
        attempt = 0
        while True:
            # httpx2 logs a line per request at INFO: not for the SDK's own.
            quiet = _SDK_REQUEST.set(True)
            try:
                resp = await h.request(method, url, timeout=timeout, **kw)
            except httpx2.ConnectError:
                if attempt >= self._max_retries:
                    raise
            else:
                text = resp.text
                if attempt >= self._max_retries or not refused(method, resp):
                    return resp, text
            finally:
                _SDK_REQUEST.reset(quiet)
            await asyncio.sleep(self._retry_pauses[min(attempt, len(self._retry_pauses) - 1)])
            attempt += 1

    async def _request(self, method: str, path: str, body: Any = None, *, timeout: float | None = None) -> dict[str, Any]:
        headers = {"Authorization": "Bearer " + self._assertion()}
        kw: dict[str, Any] = {"headers": headers}
        if body is not None:
            headers["Content-Type"] = "application/json"
            kw["content"] = json.dumps(body)
        resp, text = await self._send(method, self.core + "/api/apps/v1" + path, timeout=timeout, **kw)
        try:
            data = json.loads(text) if text else None
        except ValueError:
            data = None
        if resp.status_code == 401 or (resp.status_code == 403 and '"unauthenticated"' in text):
            self._log("key_refused", f"Wakeshift refused this app's key ({resp.status_code})")
        if resp.status_code >= 300:
            # Wakeshift's code, message and reason where it sent them as strings, else the
            # body's start, else the status's text, as every SDK reads it (errors.json).
            e = data.get("error", {}) if isinstance(data, dict) else {}
            if not isinstance(e, dict):
                e = {}

            def string(k: str) -> str:
                v = e.get(k)
                return v if isinstance(v, str) else ""

            raise APIError(
                string("message") or text[:200] or resp.reason_phrase,
                code=string("code"),
                status=resp.status_code,
                reason=string("reason") or None,
                request_id=resp.headers.get("x-request-id") or None,
            )
        # A 2xx that isn't Wakeshift's JSON: a proxy's page, or a wrong core.
        if not isinstance(data, dict):
            raise APIError(
                f"Wakeshift at {self.core} answered {resp.status_code} without a JSON object. Check that WAKESHIFT_CORE, "
                "or the core option, is the address of the console your key came from",
                code="bad_response",
                status=resp.status_code,
                request_id=resp.headers.get("x-request-id") or None,
            )
        return data

    def _unreachable(self, why: str) -> APIConnectionError:
        # Name the core it went to: a wrong or missing WAKESHIFT_CORE is the usual cause.
        return APIConnectionError(
            f"wakeshift: can't reach Wakeshift at {self.core} ({why}). Check that WAKESHIFT_CORE, or the core option, "
            f"is the address of the console your key came from. {anchor('unreachable')}",
            code="unreachable",
        )
