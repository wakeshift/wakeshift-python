"""The Wakeshift client for an app (a port of the Node SDK's Wakeshift)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypedDict, cast
from urllib.parse import quote

import httpx
from mcp_types import CallToolResult, TextContent

from .errors import WakeshiftError
from .key import AppKey, is_test_key, parse_key, sign_assertion
from .retry import RETRY_PAUSES, refused

if TYPE_CHECKING:
    from .events import Identity, WakeEvent
    from .toolcall import Attached, EventsOptions

#: Wakeshift's production core, the default for every key but a wst_test_ one. Override with ``core`` or WAKESHIFT_CORE.
DEFAULT_CORE = "https://app.wakeshift.ai"

#: The tool argument an agent passes its session key in (a result gives it one).
SESSION_ARG = "session_key"
#: The tool argument an agent says which program it runs in with.
HARNESS_ARG = "harness"
#: The tool argument an agent passes its own ID for the conversation in, when its program gives it one.
CONVERSATION_ARG = "conversation_id"

# What the agent is told each argument is, in a wrapped tool's schema and
# attach's own tools' alike (the other SDKs use the same words, and the
# shared golden file events-list.json pins them).
SESSION_DESCRIPTION = (
    "This conversation's session_key, from an earlier result (a secret token, or a handle such as claude-code#2). "
    "Keep it to this conversation; don't share or log it."
)
HARNESS_DESCRIPTION = (
    "The program you're running in: codex, claude-code, cursor (local CLI or IDE), cursor-cloud (Cursor cloud agent), "
    "grok-bot, copilot-cli, opencode, antigravity, openclaw (even when it runs your turns in Codex), hermes, chatgpt, "
    "or your program's name if it isn't listed"
)
CONVERSATION_DESCRIPTION = (
    "Your own ID for this conversation, if your program gives you one. Cursor and Grok Bot: the output of "
    "`printenv CURSOR_CONVERSATION_ID`. Codex and Antigravity send it automatically; others leave it empty"
)

#: The JSON Schema properties of Wakeshift's tool arguments: what
#: @attached.tool() and attach's own tools add to their schemas.
WAKE_ARGS_SCHEMA: dict[str, dict[str, str]] = {
    SESSION_ARG: {"type": "string", "description": SESSION_DESCRIPTION},
    HARNESS_ARG: {"type": "string", "description": HARNESS_DESCRIPTION},
    CONVERSATION_ARG: {"type": "string", "description": CONVERSATION_DESCRIPTION},
}


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

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"client": self.client}
        for k in ("user", "meta", "session", "user_agent", "conversation", "harness", "tool"):
            v = getattr(self, k)
            if v:
                out[k] = v
        return out


@dataclass(frozen=True)
class Recognised:
    """What Wakeshift recognised the calling conversation's harness as, and by
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

    #: Conversations it woke (or will).
    matched: int
    #: An earlier emit had this occurrence's id.
    duplicate: bool = False
    #: The user and native recipients with no live subscription it reached.
    unmatched: list[Recipient] = field(default_factory=list)
    #: The subscription recipients no longer live (or not that user's, or
    #: this event's): forget them. A duplicate reports the first emit's.
    ended: list[Recipient] = field(default_factory=list)


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
class PublishUrl:
    """An event's publish URL (event.publish_url)."""

    id: str
    url: str = ""
    event: str = ""


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
    )


def _call(ctx: Any, *, user: str | None, args: Mapping[str, Any] | None, tool: str, client: str | None = None) -> _Call:
    """Packs the tool call from an MCP request context: its _meta and
    User-Agent, the client's name (the request's, else client: identify's),
    the user, and session_key, harness and conversation_id from args (the
    tool's arguments)."""
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
    )


class Wakeshift:
    """The Wakeshift client for an app: define events, attach to your MCP
    server so your tools can subscribe the conversations calling them, and
    emit."""

    def __init__(
        self,
        key: str,
        *,
        core: str | None = None,
        timeout: float = 20.0,
        on_warning: Callable[[str], None] | None = None,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        if not key:
            raise ValueError("wakeshift: no app key (WAKESHIFT_KEY?)")
        self._key: AppKey = parse_key(key)
        # An older key names its app; a current one's is Wakeshift's to say (app()).
        self._app_id: str | None = self._key.app_id
        self._app: AppInfo | None = None
        self._app_lock: asyncio.Lock | None = None
        # Test keys come from staging and local consoles, and one sent to
        # production fails in a way that's hard to read: ask for the core.
        c = core or os.environ.get("WAKESHIFT_CORE") or ("" if is_test_key(key) else DEFAULT_CORE)
        if not c:
            raise ValueError(
                "wakeshift: no core URL for this test key: set WAKESHIFT_CORE, or pass core=, to the address of the "
                "console the key came from (for example https://app.staging.wakeshift.ai)"
            )
        self.core = c.rstrip("/")
        self._timeout = timeout
        self._http = http
        self._retry_pauses = RETRY_PAUSES
        self._warn_fn = on_warning or (lambda m: print(m, file=sys.stderr))
        self._events: dict[str, WakeEvent] = {}
        # What publish_manifest was last given (None: nothing yet); whether
        # Wakeshift has the manifest for the events defined now; how many
        # define_events there were; and one automatic send at a time.
        self._manifest_base: dict[str, Any] | None = None
        self._published = False
        self._manifest_version = 0
        self._manifest_lock: asyncio.Lock | None = None

    # ── The app ────────────────────────────────────────────────────────────

    @property
    def app_id(self) -> str:
        """The app's ID. An older key (wst_test_…, wst_live_…) names it; for
        a current key, which doesn't, call await wake.app() first."""
        if self._app_id is None:
            raise RuntimeError("wakeshift: call await wake.app() first: this key doesn't name its app")
        return self._app_id

    async def app(self) -> AppInfo:
        """Which app the key is, as Wakeshift says (GET /api/apps/v1/app):
        asked once, then cached. After it, app_id is the app's ID whatever
        the key's format."""
        if self._app is not None:
            return self._app
        if self._app_lock is None:
            self._app_lock = asyncio.Lock()
        async with self._app_lock:
            if self._app is None:
                out = await self._request("GET", "/app")
                k = out.get("key")
                key: dict[str, Any] = k if isinstance(k, dict) else {}
                if not isinstance(out.get("id"), str) or not out["id"]:
                    raise WakeshiftError(200, "bad_response", "Wakeshift didn't say which app this key is")
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

    def _assertion(self) -> str:
        return sign_assertion(self._key, self.core, app_id=self._app_id)

    # ── Warnings ───────────────────────────────────────────────────────────

    def _warn(self, message: str) -> None:
        """Reports a problem through on_warning, every time."""
        self._warn_fn(message)

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
        error (ValueError), and nothing is sent. You needn't call it: Wakeshift
        gets the manifest automatically when the app first needs it. Once
        called, manifest is what every automatic send carries (even if this one
        failed), with the events defined by then. Returns the manifest as
        Wakeshift keeps it."""
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
        return cast(Manifest, out)

    def _manifest_send_lock(self) -> asyncio.Lock:
        if self._manifest_lock is None:
            self._manifest_lock = asyncio.Lock()
        return self._manifest_lock

    def _manifest_body(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        """The manifest as Wakeshift gets it: its events, then the defined ones,
        refusing a name in both."""
        listed = list(manifest.get("events") or [])
        for name in self._events:
            if any(isinstance(e, Mapping) and e.get("name") == name for e in listed):
                raise ValueError(
                    f"wakeshift: event {name} is defined with define_event and listed in the manifest's events: keep one"
                )
        events = listed + [e.manifest for e in self._events.values()]
        body = {k: v for k, v in manifest.items() if k != "events"}
        if events:
            body["events"] = events
        return body

    async def _send_manifest(self) -> None:
        """The automatic send: the manifest the app last published, or else the
        settings Wakeshift has (asked for first, so the send never wipes what
        was published separately), with the defined events."""
        if self._manifest_base is not None:
            await self._request("PUT", "/manifest", self._manifest_body(self._manifest_base))
            return
        stored = await self._request("GET", "/manifest")
        description = stored.get("description")
        kept: dict[str, Any] = {"description": description if isinstance(description, str) else ""}
        if isinstance(stored.get("agent"), dict):
            kept["agent"] = stored["agent"]
        if stored.get("others_can_wake") is True:
            kept["others_can_wake"] = True
        tools = stored.get("preapprove_tools")
        if isinstance(tools, list) and tools:
            kept["preapprove_tools"] = tools
        await self._request("PUT", "/manifest", self._manifest_body(kept))

    # ── Events (MCP Events, natively or bridged) ───────────────────────────

    def define_event(self, name: str, **opts: Any) -> WakeEvent:
        """Defines one of the app's event types: what a subscriber picks (args),
        what each occurrence carries (payload), and the line an agent reads first
        (text). Returns the event, whose emit sends occurrences. Define every
        event before attach::

            message = wake.define_event(
                "chat.message",
                description="A new message in a channel",
                args=ChatArgs, payload=ChatMessage,
                text=lambda m, a: f"{m.sender}: {m.text}",
            )
            await message.emit({"channel": "eng"}, {"from": "ada", "text": "shipped"})
        """
        from .events import WakeEvent

        if name in self._events:
            raise ValueError(f"wakeshift: event {name} is defined twice")
        ev = WakeEvent(self, name, **opts)
        self._events[name] = ev
        self._published = False  # the manifest changed: sent again when next needed
        self._manifest_version += 1
        return ev

    def attach(
        self,
        server: Any,
        *,
        identify: Callable[[Any], Identity | None] | None = None,
        tools: Sequence[str] | None = None,
        events: EventsOptions | None = None,
    ) -> Attached:
        """Attaches Wakeshift to an MCPServer, before you run it: the one call
        that connects the two. Today it serves the defined events: MCP Events
        (events/list, events/subscribe, events/unsubscribe) for clients that
        speak it, such as ChatGPT, and the subscribe_events tool for everyone
        else; plus unsubscribe_events and list_subscriptions for every client.
        It returns the handle your own tools register with
        (``@attached.tool()``), so they can subscribe the calling conversation
        (``call.subscribe``). ``events`` configures the event features
        (EventsOptions: native, an override of native detection).

        ``tools`` names which of its own tools attach adds (BUILTIN_TOOLS:
        subscribe_events, unsubscribe_events, list_subscriptions); None, the
        default, adds all three, and ``[]`` none. Leave out
        unsubscribe_events or list_subscriptions only if your own tools let a
        person see and stop their subscriptions.

        identify tells who is behind a request (it gets the request context) as
        an Identity, from the app's own sign-in. Without it, requests have no
        user: native subscriptions, personal events and list_subscriptions
        need one::

            wake.attach(server, identify=lambda ctx: Identity(user=my_user(ctx)))"""
        from .events import attach_events

        return attach_events(self, server, list(self._events.values()), identify=identify, tools=tools, events_options=events)

    async def _ensure_events(self) -> None:
        """Sends the manifest with the defined events, once (again after a define_event)."""
        if self._published:
            return
        async with self._manifest_send_lock():
            if self._published:
                return  # sent while this one waited
            version = self._manifest_version
            await self._send_manifest()
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
        await self._ensure_events()
        body: dict[str, Any] = {"name": name, "arguments": arguments, "payload": payload, "text": text}
        if id:
            body["id"] = id
        if isinstance(to, str):
            body["to"] = to
        elif to:
            # One plain user goes as a string (the single-user path), else a list.
            if len(to) == 1 and to[0].subscription is None and to[0].native is None:
                body["to"] = to[0].user
            else:
                body["to"] = [_recipient_wire(r) for r in to]
        out = await self._request("POST", "/events/emit", body)
        return Emitted(
            matched=int(out["matched"]),
            duplicate=bool(out.get("duplicate")),
            unmatched=_recipients_of(out.get("unmatched")),
            ended=_recipients_of(out.get("ended")),
        )

    async def _request_app(self, method: str, path: str, body: Any = None) -> dict[str, Any]:
        """A call to the app API, after the manifest with the defined events is published."""
        await self._ensure_events()
        return await self._request(method, path, body)

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
        WakeshiftError no_grant once revoked, unknown_workspace once forgotten."""
        await self._request("POST", f"/workspaces/{quote(workspace, safe='')}/agents/{quote(agent, safe='')}/token")

    # ── HTTP ───────────────────────────────────────────────────────────────

    async def _send(self, method: str, url: str, **kw: Any) -> tuple[httpx.Response, str]:
        # A request the load balancer refused during a rollout is sent again (retry.py).
        try:
            if self._http is not None:
                return await self._send_with(self._http, method, url, **kw)
            async with httpx.AsyncClient(timeout=self._timeout) as h:
                return await self._send_with(h, method, url, **kw)
        except httpx.TimeoutException as err:
            raise self._unreachable(f"no answer within {self._timeout:g}s") from err
        except httpx.HTTPError as err:
            raise self._unreachable(str(err) or type(err).__name__) from err

    async def _send_with(self, h: httpx.AsyncClient, method: str, url: str, **kw: Any) -> tuple[httpx.Response, str]:
        attempt = 0
        while True:
            resp = await h.request(method, url, timeout=self._timeout, **kw)
            text = resp.text
            if attempt >= len(self._retry_pauses) or not refused(method, resp):
                return resp, text
            await asyncio.sleep(self._retry_pauses[attempt])
            attempt += 1

    async def _request(self, method: str, path: str, body: Any = None) -> dict[str, Any]:
        headers = {"Authorization": "Bearer " + self._assertion()}
        kw: dict[str, Any] = {"headers": headers}
        if body is not None:
            headers["Content-Type"] = "application/json"
            kw["content"] = json.dumps(body)
        resp, text = await self._send(method, self.core + "/api/apps/v1" + path, **kw)
        try:
            data = json.loads(text) if text else None
        except ValueError:
            data = None
        if resp.status_code >= 300:
            # Wakeshift's code, message and reason where it sent them as strings, else the
            # body's start, else the status's text, as every SDK reads it (errors.json).
            e = data.get("error", {}) if isinstance(data, dict) else {}
            if not isinstance(e, dict):
                e = {}

            def string(k: str) -> str:
                v = e.get(k)
                return v if isinstance(v, str) else ""

            raise WakeshiftError(
                resp.status_code,
                string("code"),
                string("message") or text[:200] or resp.reason_phrase,
                reason=string("reason") or None,
            )
        # A 2xx that isn't Wakeshift's JSON: a proxy's page, or a wrong core.
        if not isinstance(data, dict):
            raise WakeshiftError(
                resp.status_code,
                "bad_response",
                f"Wakeshift at {self.core} answered {resp.status_code} without a JSON object. Check that WAKESHIFT_CORE, "
                "or the core option, is the address of the console your key came from",
            )
        return data

    def _unreachable(self, why: str) -> WakeshiftError:
        # Name the core it went to: a wrong or missing WAKESHIFT_CORE is the usual cause.
        return WakeshiftError(
            0,
            "unreachable",
            f"can't reach Wakeshift at {self.core} ({why}). Check that WAKESHIFT_CORE, or the core option, is the address "
            "of the console your key came from",
        )
