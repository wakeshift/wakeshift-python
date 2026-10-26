"""The Wakeshift client for an app (a port of the Node SDK's Wakeshift)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Literal, Optional
from urllib.parse import quote, urlencode

import httpx
from mcp_types import CallToolResult, TextContent
from pydantic import Field

from .errors import WakeshiftError
from .key import AppKey, parse_key, sign_assertion
from .retry import RETRY_PAUSES, refused

if TYPE_CHECKING:
    from .events import Identity, WakeEvent

#: Wakeshift's production core, the default for every key but a wst_test_ one. Override with ``core`` or WAKESHIFT_CORE.
DEFAULT_CORE = "https://app.wakeshift.ai"

#: The tool argument an agent passes its session key in (a result gives it one).
SESSION_ARG = "session_key"
#: The tool argument an agent says which program it runs in with.
HARNESS_ARG = "harness"
#: The tool argument an agent passes its own ID for the conversation in, when its program gives it one.
CONVERSATION_ARG = "conversation_id"

# What the agent is told each argument is (the core's own words:
# internal/harnesses, whose tests check these match).
SESSION_DESCRIPTION = (
    "The session key an earlier result gave you, if it did. It's a secret: keep it to this conversation; don't share or log it"
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

# Annotated types for a tool function's parameters, so the agent can pass
# Wakeshift's part of the call (the Python form of the Node SDK's wake.args):
#
#   @server.tool()
#   async def watch(repo: str, ctx: Context, session_key: SessionKey = None,
#                   harness: Harness = None, conversation_id: ConversationId = None): ...
SessionKey = Annotated[Optional[str], Field(description=SESSION_DESCRIPTION)]  # noqa: UP045 (evaluated at runtime)
Harness = Annotated[Optional[str], Field(description=HARNESS_DESCRIPTION)]  # noqa: UP045
ConversationId = Annotated[Optional[str], Field(description=CONVERSATION_DESCRIPTION)]  # noqa: UP045

#: The JSON Schema properties of Wakeshift's tool arguments (for tools whose schema you write yourself).
WAKE_ARGS_SCHEMA: dict[str, dict[str, str]] = {
    SESSION_ARG: {"type": "string", "description": SESSION_DESCRIPTION},
    HARNESS_ARG: {"type": "string", "description": HARNESS_DESCRIPTION},
    CONVERSATION_ARG: {"type": "string", "description": CONVERSATION_DESCRIPTION},
}

#: What the SDK says, once, when Wakeshift recognised no harness in a tool call.
UNKNOWN_CLIENT_WARNING = (
    "wakeshift: Wakeshift couldn't tell which agent made a tool call (no MCP client name, nothing it knows in the "
    "call's _meta or User-Agent, and no harness argument), so it treats the agent as one it can't wake (its messages "
    "are held for its next call to the tool). The name comes from the client's initialize request, which a stateless "
    "server never sees; Claude Code, Codex, Antigravity, Copilot CLI, opencode and Cursor are recognised without it. "
    "Add the harness argument (wakeshift.Harness) to the tool and pass args to wake.call, so the agent can say; or pass "
    "the name yourself: wake.call(ctx, client=...) or wake.call_of(client_name=...)."
)

Via = Literal["stream", "poll", "hooks", "webhook"]


@dataclass
class Call:
    """Who called one of the app's tools, as Wakeshift needs it. Make one with
    ``wake.call(ctx, ...)`` inside a tool, or ``wake.call_of(...)``."""

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

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"client": self.client}
        for k in ("user", "meta", "session", "user_agent", "conversation", "harness"):
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
class Addition:
    """What Wakeshift adds to a tool result: text for the agent, _meta for its harness."""

    text: str | None = None
    meta: dict[str, Any] | None = None
    #: What Wakeshift recognised the caller's harness as (not added to the result).
    agent: Recognised | None = None

    @classmethod
    def from_json(cls, v: Mapping[str, Any] | None) -> Addition:
        v = v or {}
        return cls(text=v.get("text") or None, meta=v.get("meta") or None)


def apply_addition(a: Addition, base: str | CallToolResult) -> CallToolResult:
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


@dataclass
class Subscription:
    id: str
    topic: str
    once: bool
    expires_at: datetime
    #: The conversation's session key: a secret that acts as the conversation. Don't show, store or log it.
    session: str = field(repr=False)
    addition: Addition = field(default_factory=Addition)
    agent: Recognised = field(default_factory=Recognised)
    filter: dict[str, Any] | None = None

    def result(self, text: str | CallToolResult) -> CallToolResult:
        """The tool result to return: your text (or result) followed by Wakeshift's, with its _meta."""
        return apply_addition(self.addition, text)


@dataclass(frozen=True)
class Published:
    #: Conversations it woke (or will).
    matched: int
    #: An earlier publish had this event's id.
    duplicate: bool = False


@dataclass(frozen=True)
class TopicStatus:
    topic: str
    subscriptions: int
    connected: int
    via: dict[str, int]


@dataclass(frozen=True)
class SubscriptionStatus:
    id: str
    topic: str
    status: str
    connected: bool
    via: str | None = None
    expires_at: datetime | None = None


@dataclass(frozen=True)
class PublishUrl:
    """A publish URL: topic for a topic's (wake.publish_url), event for an
    event's (event.publish_url)."""

    id: str
    topic: str = ""
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


def request_info(ctx: Any) -> tuple[str, dict[str, Any], Mapping[str, str]]:
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


def client_capabilities(ctx: Any) -> dict[str, Any]:
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


class Wakeshift:
    """The Wakeshift client for an app: define events and emit them, subscribe
    the conversations that call your tools to topics, and publish to them."""

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
        c = core or os.environ.get("WAKESHIFT_CORE") or ("" if self._key.mode == "test" else DEFAULT_CORE)
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
        self._warned: set[str] = set()
        self._events: dict[str, WakeEvent] = {}
        self._manifest_base: dict[str, Any] = {}
        self._published = False

    # ── The app ────────────────────────────────────────────────────────────

    @property
    def app_id(self) -> str:
        """The app's ID. An older key (wst_test_…, wst_live_…) names it; for
        a current key, which doesn't, call await wake.app() first."""
        if self._app_id is None:
            raise RuntimeError("wakeshift: call await wake.app() first: this key doesn't name its app")
        return self._app_id

    @property
    def mode(self) -> str | None:
        """The mode of an older key (test or live); None for a current one.
        Deprecated: the mode means nothing now."""
        return self._key.mode

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

    def warn(self, message: str) -> None:
        """Reports a problem through on_warning, every time."""
        self._warn_fn(message)

    def _warn_once(self, kind: str, message: str) -> None:
        if kind in self._warned:
            return
        self._warned.add(kind)
        self._warn_fn(message)

    def _recognised(self, agent: Mapping[str, Any] | None) -> Recognised:
        if agent is not None and not agent.get("harness") and not agent.get("claimed"):
            self._warn_once("unknown_client", UNKNOWN_CLIENT_WARNING)
        agent = agent or {}
        return Recognised(harness=agent.get("harness"), via=agent.get("via"), claimed=agent.get("claimed"))

    # ── Calls ──────────────────────────────────────────────────────────────

    def call(
        self,
        ctx: Any,
        *,
        user: str | None = None,
        args: Mapping[str, Any] | None = None,
        client: str | None = None,
        session: str | None = None,
        harness: str | None = None,
        conversation: str | None = None,
    ) -> Call:
        """Packs the tool call from an MCP tool's Context: its _meta and
        User-Agent, the client's name, the user, and session_key, harness and
        conversation_id from args (the tool's arguments)."""
        name, meta, headers = request_info(ctx)
        return self.call_of(
            client_name=name if client is None else client,
            meta=meta,
            user=user,
            session=session if session is not None else _arg(args, SESSION_ARG),
            user_agent=headers.get("user-agent", "") if headers else "",
            harness=harness if harness is not None else _arg(args, HARNESS_ARG),
            conversation=conversation if conversation is not None else _arg(args, CONVERSATION_ARG),
        )

    def call_of(
        self,
        *,
        client_name: str,
        meta: Mapping[str, Any] | None = None,
        user: str | None = None,
        session: str | None = None,
        user_agent: str | None = None,
        harness: str | None = None,
        conversation: str | None = None,
    ) -> Call:
        """Packs a tool call without an MCP framework."""
        return Call(
            client=client_name or "",
            user=user or None,
            meta=dict(meta) if meta else None,
            session=session or None,
            user_agent=user_agent or None,
            harness=(harness or "").strip() or None,
            conversation=(conversation or "").strip() or None,
        )

    # ── Topics ─────────────────────────────────────────────────────────────

    async def subscribe(
        self,
        call: Call,
        topic: str,
        *,
        filter: Mapping[str, Any] | None = None,
        once: bool = False,
        replay: int | str | None = None,
        ttl: int | str | None = None,
    ) -> Subscription:
        """Subscribes the calling conversation to topic (again: refreshes it).
        Check the caller may see the topic first: Wakeshift only refuses another
        user's u/<user>/… topics."""
        body: dict[str, Any] = {"call": call.to_json(), "topic": topic, "once": bool(once)}
        if filter:
            body["filter"] = dict(filter)
        if replay is not None:
            body["replay"] = replay
        if ttl is not None:
            body["ttl"] = ttl
        out = await self._request("POST", "/topics/subscribe", body)
        s = out["subscription"]
        return Subscription(
            id=s["id"],
            topic=s["topic"],
            filter=s.get("filter"),
            once=bool(s.get("once")),
            expires_at=_parse_time(s["expires_at"]),
            session=out.get("session", ""),
            addition=Addition.from_json(out.get("addition")),
            agent=self._recognised(out.get("agent")),
        )

    async def unsubscribe(self, call: Call, topic: str | None = None) -> int:
        """Ends the conversation's subscription to topic, or to all the app's topics. Returns how many ended."""
        body: dict[str, Any] = {"call": call.to_json()}
        if topic:
            body["topic"] = topic
        return int((await self._request("POST", "/topics/unsubscribe", body))["unsubscribed"])

    async def publish(
        self, topics: str | list[str], *, text: str, data: Mapping[str, Any] | None = None, id: str | None = None
    ) -> Published:
        """Wakes every conversation subscribed to one of topics whose filter the event passes (each once)."""
        event: dict[str, Any] = {"text": text}
        if data is not None:
            event["data"] = dict(data)
        if id:
            event["id"] = id
        out = await self._request(
            "POST", "/topics/publish", {"topics": [topics] if isinstance(topics, str) else list(topics), "event": event}
        )
        return Published(matched=int(out["matched"]), duplicate=bool(out.get("duplicate")))

    async def send(self, subscription_id: str, *, text: str, data: Mapping[str, Any] | None = None, id: str | None = None) -> str:
        """Wakes exactly one subscription's conversation, whatever its filter: delivered, queued or closed."""
        event: dict[str, Any] = {"text": text}
        if data is not None:
            event["data"] = dict(data)
        if id:
            event["id"] = id
        return (await self._request("POST", f"/subscriptions/{quote(subscription_id, safe='')}/send", {"event": event}))["status"]

    async def topic_status(self, topic: str) -> TopicStatus:
        """How many conversations are subscribed to topic, and how many are connected now, and how. Only reads."""
        out = await self._request("GET", "/topics/status?" + urlencode({"topic": topic}))
        via = {"stream": 0, "poll": 0, "hooks": 0, "webhook": 0}
        via.update(out.get("via") or {})
        return TopicStatus(topic=out["topic"], subscriptions=int(out["subscriptions"]), connected=int(out["connected"]), via=via)

    async def subscription_status(self, subscription_id: str) -> SubscriptionStatus:
        """Whether a subscription is active and its conversation connected now, and how. Only reads."""
        out = await self._request("GET", f"/subscriptions/{quote(subscription_id, safe='')}")
        return SubscriptionStatus(
            id=out["id"],
            topic=out["topic"],
            status=out["status"],
            connected=bool(out["connected"]),
            via=out.get("via"),
            expires_at=_parse_time(out["expires_at"]) if out.get("expires_at") else None,
        )

    async def publish_url(self, topic: str) -> PublishUrl:
        """A capability URL that publishes to topic: a POST's body becomes an event."""
        out = await self._request("POST", "/topics/publish-urls", {"topic": topic})
        return PublishUrl(id=out["id"], topic=out["topic"], url=out["url"])

    async def revoke_publish_url(self, id: str) -> None:
        """Stops a publish URL working, a topic's or an event's (410 from then on)."""
        await self._request("DELETE", f"/events/publish-urls/{quote(id, safe='')}")

    async def addition(self, call: Call) -> Addition:
        """What Wakeshift would add to any other tool result for this conversation (creates nothing)."""
        out = await self._request("POST", "/topics/annotate", {"call": call.to_json()})
        a = Addition.from_json(out)
        a.agent = self._recognised(out.get("agent"))
        return a

    async def annotate(self, call: Call, result: CallToolResult) -> CallToolResult:
        """Adds what Wakeshift has for the calling conversation to a tool result of yours (a copy)."""
        return apply_addition(await self.addition(call), result)

    async def close_topic(self, topic: str) -> int:
        """Ends every subscription to topic; each conversation is told. Returns how many ended."""
        return int((await self._request("POST", "/topics/close", {"topic": topic}))["closed"])

    async def end_subscription(self, subscription_id: str) -> bool:
        """Ends one subscription by its id, telling its conversation. False if it had already ended."""
        return bool((await self._request("POST", f"/subscriptions/{quote(subscription_id, safe='')}/end", {}))["ended"])

    async def unsubscribe_user(self, user: str, topic: str | None = None) -> int:
        """Ends one user's subscriptions to topic, or to all the app's topics. Returns how many ended."""
        body: dict[str, Any] = {"user": user}
        if topic:
            body["topic"] = topic
        return int((await self._request("POST", "/topics/unsubscribe", body))["unsubscribed"])

    async def forget_user(self, user: str) -> bool:
        """Deletes everything Wakeshift keeps for one of the app's users. False if there was nothing to forget."""
        return bool((await self._request("POST", "/users/forget", {"user": user}))["forgotten"])

    async def publish_manifest(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        """Tells Wakeshift what the app may do, replacing what it said before.
        The events you defined (define_event) are added unless manifest has
        events of its own. Returns the manifest as Wakeshift keeps it."""
        body = dict(manifest)
        if "events" not in body and self._events:
            body["events"] = [e.manifest for e in self._events.values()]
        out = await self._request("PUT", "/manifest", body)
        self._manifest_base = dict(manifest)
        self._published = True
        return out

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
        self._published = False  # the manifest changed
        return ev

    def attach(
        self,
        server: Any,
        *,
        identify: Callable[[Any], Identity | None] | None = None,
    ) -> None:
        """Attaches Wakeshift to an MCPServer, before you run it: the one call
        that connects the two. Today it serves the defined events: MCP Events
        (events/list, events/subscribe, events/unsubscribe) for clients that
        speak it, such as ChatGPT, and the subscribe_events tool for everyone
        else; plus unsubscribe_events and list_subscriptions for every client.

        identify tells who is behind a request (it gets the request context) as
        an Identity; default: the guest GuestAuth signed in::

            wake.attach(server, identify=lambda ctx: Identity(user=my_user(ctx)))"""
        from .events import attach_events

        attach_events(self, server, list(self._events.values()), identify=identify)

    async def ensure_events(self) -> None:
        """Publishes the manifest with the defined events, once (again after a define_event)."""
        if not self._published:
            await self.publish_manifest(self._manifest_base)

    async def emit_event(
        self, *, name: str, arguments: Any, payload: Any, text: str, id: str | None = None, to: str | None = None
    ) -> Published:
        await self.ensure_events()
        body: dict[str, Any] = {"name": name, "arguments": arguments, "payload": payload, "text": text}
        if id:
            body["id"] = id
        if to:
            body["to"] = to
        out = await self._request("POST", "/events/emit", body)
        return Published(matched=int(out["matched"]), duplicate=bool(out.get("duplicate")))

    async def request_app(self, method: str, path: str, body: Any = None) -> dict[str, Any]:
        """A call to the app API, after the manifest with the defined events is published."""
        await self.ensure_events()
        return await self._request(method, path, body)

    # ── Guests' workspaces (GuestAuth) ─────────────────────────────────────

    async def workspace(self, user: str, name: str) -> str:
        """The workspace Wakeshift keeps for one of the app's users, creating it on first use."""
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

    def secret_seed(self) -> bytes:
        """The app key's seed: what GuestAuth keys its signed values from. Never send or log it."""
        return self._key.seed

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
        if resp.status_code >= 400:
            e = data.get("error", {}) if isinstance(data, dict) else {}
            if not isinstance(e, dict):
                e = {}
            reason = e.get("reason") if isinstance(e.get("reason"), str) else None
            raise WakeshiftError(
                resp.status_code, e.get("code", ""), e.get("message") or text[:200] or resp.reason_phrase, reason=reason
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
