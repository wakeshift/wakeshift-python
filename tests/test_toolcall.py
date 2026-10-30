"""Wrapped tools (@attached.tool()) against a fake core that records what
the SDK asks it: one block per call, rollback decided from the final result,
the scope guard, native callers, the session argument and typed output (the
Go SDK's toolcall_test.go, in Python)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from mcp.server.mcpserver import Context, MCPServer
from mcp.shared.exceptions import MCPError
from pydantic import BaseModel, ConfigDict

from wakeshift import (
    BUILTIN_TOOLS,
    CallEnded,
    Emitted,
    EventsOptions,
    EventStatus,
    EventSubscriptionStatus,
    Identity,
    Recipient,
    Subscription,
    ToolCall,
    WakeOutput,
    Wakeshift,
    WakeshiftError,
    is_code,
)
from wakeshift.client import SESSION_DESCRIPTION
from wakeshift.toolcall import CONVERSATION_META, conversation_key, merge_block, with_wakeshift_field

from .helpers import app_key, golden_file, served

TRAILER = "(For Wakeshift's hook on this computer, not for you: wakeshift-beacon: aaaaaaaaaa.bbbbbbbbbb.cccccccccc)"
WOKEN = "Wakeshift: this conversation is woken by Monitor."


class WakeCore:
    """Answers the app API's calls the wrapper makes, and records them."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.seen: set[str] = set()
        self.subs = 0
        self.addition: dict[str, Any] | None = None
        self.fail: set[str] = set()
        self.native: list[dict[str, Any]] = []
        #: Seconds each subscribe takes.
        self.delay = 0.0

    def paths(self) -> list[str]:
        return [p for p, _ in self.calls if p not in ("/manifest", "/manifest/merge")]

    def bodies(self, path: str) -> list[Any]:
        return [b for p, b in self.calls if p == path]

    def reset(self) -> None:
        self.calls.clear()

    async def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api/apps/v1")
        if self.delay and path == "/event-subscriptions":
            await asyncio.sleep(self.delay)
        body = json.loads(request.content) if request.content else {}
        self.calls.append((path, body))
        if path in self.fail:
            return httpx.Response(503, json={"error": {"code": "unavailable", "message": "down"}})
        call = (body or {}).get("call") or {}
        if path in ("/manifest", "/manifest/merge"):
            return httpx.Response(200, json=body)
        if path == "/event-subscriptions":
            key = f"{body['name']}{json.dumps(body['arguments'], sort_keys=True)}"
            created = key not in self.seen
            self.seen.add(key)
            self.subs += 1
            return httpx.Response(
                200,
                json={
                    "event_subscription": {"id": f"sub-{self.subs}", "user": call.get("user"), "created": created},
                    "session": "wsc_s1",
                    "agent": {"harness": "claude-code", "via": "user_agent"},
                    "pending": body.get("pending") or "pa_first",
                },
            )
        if path == "/calls/session":
            return httpx.Response(
                200, json={"session": "wsc_resolved", "pending": "pa_session", "agent": {"harness": "codex", "via": "meta"}}
            )
        if path == "/calls/addition":
            if self.addition is not None:
                return httpx.Response(200, json=self.addition)
            return httpx.Response(
                200, json={"text": WOKEN, "trailer": TRAILER, "meta": {"wakeshift/connector": "tok", "other": "x"}}
            )
        if path in ("/event-subscriptions/unsubscribe", "/event-subscriptions/end"):
            return httpx.Response(200, json={"ended": True})
        if path == "/event-subscriptions/list":
            return httpx.Response(200, json={"subscriptions": self.native})
        if path == "/events/status":
            if "subscriptions" not in body:
                return httpx.Response(
                    200, json={"subscriptions": 3, "connected": 2, "via": {"stream": 1, "poll": 1, "hooks": 0, "webhook": 0}}
                )
            return httpx.Response(
                200,
                json={
                    "subscriptions": [
                        {"id": r["id"], "user": r["user"], "live": r["id"] == "s1", "connected": r["id"] == "s1"}
                        | ({"via": "stream"} if r["id"] == "s1" else {})
                        for r in body["subscriptions"]
                    ]
                },
            )
        if path == "/events/publish-urls":
            return httpx.Response(200, json={"id": "p1", "url": "https://x"})
        if path == "/events/emit":
            return httpx.Response(
                200,
                json={
                    "matched": 2,
                    "duplicate": False,
                    "unmatched": ["u3", {"native": "u4"}],
                    "ended": [{"user": "u2", "subscription": "s2"}],
                },
            )
        return httpx.Response(404, json={"error": {"code": "not_found", "message": path}})


class TableArgs(BaseModel):
    table: str


class TurnMsg(BaseModel):
    text: str


class View(WakeOutput):
    table: str


class PlainView(BaseModel):
    table: str


class Wrapped:
    """An app with a personal and a shared event, behind attach on a stateless
    MCPServer whose X-User header is the user."""

    def __init__(self, events: EventsOptions | None = None, identify: Any = "header") -> None:
        self.core = WakeCore()
        self.warnings: list[str] = []
        http = httpx.AsyncClient(transport=httpx.MockTransport(self.core.handler))
        self.wake = Wakeshift(app_key("cards"), core="http://core.test", http=http, on_warning=self.warnings.append)
        self.turn = self.wake.define_event(
            "game.turn", description="Your turn.", args=TableArgs, payload=TurnMsg, text=lambda m, a: m.text, personal=True
        )
        self.shared = self.wake.define_event(
            "table.news", description="News at a table.", args=TableArgs, payload=TurnMsg, text=lambda m, a: m.text
        )
        self.server = MCPServer("cards")
        if identify == "header":

            def identify(ctx: Any) -> Identity:
                return Identity(user=ctx.request.headers.get("x-user") if ctx.request is not None else None)

        self.attached = self.wake.attach(self.server, identify=identify, events=events)

    def served(self) -> Any:
        return served(self.server)


async def call_tool(
    rpc: Any,
    tool: str,
    args: dict[str, Any],
    *,
    user: str = "ada",
    ua: str = "claude-code/2.1.280",
    meta: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"name": tool, "arguments": args}
    if meta is not None:
        params["_meta"] = meta
    return await rpc("tools/call", params, headers={"X-User": user, "User-Agent": ua, **(headers or {})})


def texts(res: dict[str, Any]) -> list[str]:
    assert "result" in res, res
    return [c.get("text", "") for c in res["result"]["content"]]


# ── One block per call ────────────────────────────────────────────────────


async def test_two_subscribes_one_block() -> None:
    w = Wrapped()
    got: list[Subscription] = []
    before: list[Any] = []

    @w.attached.tool()
    async def join(table: str, call: ToolCall) -> str:
        """Take a seat."""
        before.append((call.recognized, call.session, call.user, call.tool, call.native))
        a = await call.subscribe(w.turn, {"table": table}, reason=f"Playing at {table}", ttl=3600)
        b = await call.subscribe(w.shared, {"table": table}, label="poker")
        got.extend([a, b])
        return f"Seated at {table}."

    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "join", {"table": "oak"})
    assert not res["result"].get("isError"), res
    assert before == [(None, None, "ada", "join", False)]
    # A string reply stays one text block: the app's, Wakeshift's, the beacon last.
    assert texts(res) == [f"Seated at oak.\n\n{WOKEN}\n\n{TRAILER}"]
    assert res["result"]["_meta"] == {"wakeshift/connector": "tok", "other": "x"}
    assert w.core.paths() == ["/event-subscriptions", "/event-subscriptions", "/calls/addition"]
    subs = w.core.bodies("/event-subscriptions")
    assert subs[0]["defer_addition"] is True and "pending" not in subs[0]
    assert subs[0]["reason"] == "Playing at oak" and subs[0]["ttl"] == 3600
    assert subs[0]["call"]["user"] == "ada" and subs[0]["call"]["tool"] == "join"
    assert subs[0]["call"]["user_agent"] == "claude-code/2.1.280"
    assert subs[1]["pending"] == "pa_first" and subs[1]["label"] == "poker" and subs[1]["call"]["session"] == "wsc_s1"
    add = w.core.bodies("/calls/addition")[0]
    assert add["pending"] == "pa_first" and "native" not in add and add["call"]["session"] == "wsc_s1"
    assert got[0].id == "sub-1" and got[0].user == "ada" and got[0].created and got[0].session == "wsc_s1"
    assert got[0].recognized.harness == "claude-code"


async def test_without_subscribing() -> None:
    w = Wrapped()
    w.core.addition = {"text": "Wakeshift: 2 messages wait."}

    @w.attached.tool()
    async def look(table: str, call: ToolCall) -> str:
        return "A table."

    @w.server.tool()
    async def plain(table: str, session_key: str | None = None) -> str:
        return "Plain."

    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "look", {"table": "oak"})
        assert texts(res) == ["A table."] and w.core.paths() == []
        res = await call_tool(rpc, "look", {"table": "oak", "session_key": "wsc_s1"})
        assert texts(res) == ["A table.\n\nWakeshift: 2 messages wait."]
        add = w.core.bodies("/calls/addition")
        assert len(add) == 1 and "pending" not in add[0]
        # A harness's conversation in _meta names it too.
        w.core.reset()
        await call_tool(rpc, "look", {"table": "oak"}, meta={"x-codex-turn-metadata": {"thread_id": "t1"}})
        assert w.core.paths() == ["/calls/addition"]
        w.core.reset()
        res = await call_tool(rpc, "plain", {"table": "oak", "session_key": "wsc_s1"})
        assert texts(res) == ["Plain."] and w.core.paths() == []


async def test_resolve_session() -> None:
    w = Wrapped()
    seen: list[Any] = []

    @w.attached.tool()
    async def join(call: ToolCall) -> str:
        s = await call.resolve_session()
        seen.append((s, call.session, call.recognized))
        assert await call.resolve_session() == s
        await call.subscribe(w.shared, {"table": "oak"})
        return "ok"

    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {})
    assert seen[0][0] == "wsc_resolved" and seen[0][1] == "wsc_resolved" and seen[0][2].harness == "codex"
    assert w.core.paths() == ["/calls/session", "/event-subscriptions", "/calls/addition"]
    assert w.core.bodies("/event-subscriptions")[0]["pending"] == "pa_session"
    assert w.core.bodies("/calls/addition")[0]["pending"] == "pa_session"


# ── Rollback ──────────────────────────────────────────────────────────────


async def test_rollback_on_each_failure_kind() -> None:
    w = Wrapped()
    w.core.seen.add('table.news{"table": "old"}')  # "old" exists before the calls: renewed, not ended

    async def both(call: ToolCall) -> None:
        await call.subscribe(w.shared, {"table": "old"})
        await call.subscribe(w.shared, {"table": "new"})

    @w.attached.tool()
    async def fails(call: ToolCall) -> str:
        await both(call)
        raise ValueError("the table is full")

    @w.attached.tool()
    async def is_error(call: ToolCall) -> Any:
        from mcp_types import CallToolResult, TextContent

        await both(call)
        return CallToolResult(content=[TextContent(type="text", text="no")], is_error=True)

    @w.attached.tool()
    async def rpc_error(call: ToolCall) -> str:
        await both(call)
        raise MCPError(-32602, "bad table")

    @w.attached.tool()
    async def bad_output(call: ToolCall) -> PlainView:
        await both(call)
        return {"seat": 3}  # type: ignore[return-value]  # no table: the output schema refuses it

    async with w.served() as (_, rpc):
        for tool in ("fails", "is_error", "rpc_error", "bad_output"):
            w.core.reset()
            res = await call_tool(rpc, tool, {})
            failed = "error" in res or res["result"].get("isError")
            assert failed, (tool, res)
            if "result" in res:
                assert not any("Wakeshift:" in t or "wakeshift-beacon" in t for t in texts(res)), (tool, res)
            assert "/calls/addition" not in w.core.paths(), tool
            ends = w.core.bodies("/event-subscriptions/unsubscribe")
            assert len(ends) == 1, (tool, ends)
            assert ends[0]["reason"] == "rolled_back" and ends[0]["name"] == "table.news"
            assert ends[0]["arguments"] == {"table": "new"} and ends[0]["call"]["session"] == "wsc_s1"
            # The call's record: the core spares a subscription another call renewed.
            assert ends[0]["pending"] == "pa_first"
            w.core.seen.discard('table.news{"table": "new"}')


async def test_rollback_on_cancellation() -> None:
    """A canceled request (here, its task canceled mid-tool) still ends what
    it made: the rollback is shielded."""
    w = Wrapped()
    subscribed = asyncio.Event()

    @w.attached.tool()
    async def slow(call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": "oak"})
        subscribed.set()
        await asyncio.sleep(30)
        return "never"

    async with w.served() as (_, rpc):
        task = asyncio.create_task(call_tool(rpc, "slow", {}))
        await asyncio.wait_for(subscribed.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        for _ in range(100):
            if w.core.bodies("/event-subscriptions/unsubscribe"):
                break
            await asyncio.sleep(0.02)
    ends = w.core.bodies("/event-subscriptions/unsubscribe")
    assert len(ends) == 1 and ends[0]["reason"] == "rolled_back"
    assert "/calls/addition" not in w.core.paths()


async def test_block_failure_rolls_back() -> None:
    w = Wrapped()
    w.core.fail.add("/calls/addition")

    @w.attached.tool()
    async def join(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return "Seated."

    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "join", {"table": "oak"})
    assert res["result"]["isError"] is True
    assert "Wakeshift couldn't finish setting up" in texts(res)[0]
    ends = w.core.bodies("/event-subscriptions/unsubscribe")
    assert len(ends) == 1 and ends[0]["reason"] == "rolled_back"
    # The app hears why, through on_warning.
    assert any("the block for join failed" in m for m in w.warnings), w.warnings


async def test_a_failed_block_without_a_subscription_is_a_courtesy() -> None:
    w = Wrapped()
    w.core.fail.add("/calls/addition")

    @w.attached.tool()
    async def look(call: ToolCall) -> str:
        return "A table."

    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "look", {"session_key": "wsc_s1"})
    assert texts(res) == ["A table."] and not res["result"].get("isError")
    assert any("addition" in m for m in w.warnings)


# ── The scope guard ───────────────────────────────────────────────────────


async def test_scope_guard() -> None:
    w = Wrapped()
    with pytest.raises(TypeError):
        ToolCall(
            object(),
            scope=None,
            context=None,
            user=None,
            tool="x",
            native=False,
            native_conversation=None,
            session=None,
            harness=None,
            conversation=None,
        )  # type: ignore[arg-type]
    kept: list[ToolCall] = []

    @w.attached.tool()
    async def join(call: ToolCall) -> str:
        kept.append(call)
        return "ok"

    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {})
    assert kept
    with pytest.raises(CallEnded):
        await kept[0].subscribe(w.turn, {"table": "oak"})
    with pytest.raises(CallEnded):
        await kept[0].resolve_session()
    assert "/event-subscriptions" not in w.core.paths() and "/calls/session" not in w.core.paths()

    # A wrapped tool on a server its attach wasn't given refuses.
    other = MCPServer("other")
    w.attached._server = other  # register the next tool on another server
    ran: list[bool] = []

    @w.attached.tool()
    async def stray(call: ToolCall) -> str:
        ran.append(True)
        return "ran"

    async with served(other) as (_, rpc2):
        res = await call_tool(rpc2, "stray", {})
    assert res["result"]["isError"] is True and not ran


def test_decorator_refusals() -> None:
    w = Wrapped()
    with pytest.raises(TypeError, match="async"):

        @w.attached.tool()
        def sync(call: ToolCall) -> str:  # pyright: ignore[reportUnusedFunction]
            return ""

    with pytest.raises(TypeError, match="ToolCall"):

        @w.attached.tool()
        async def nocall(table: str) -> str:  # pyright: ignore[reportUnusedFunction]
            return ""

    with pytest.raises(TypeError, match="parentheses"):
        w.attached.tool(lambda: None)  # type: ignore[arg-type]


async def test_attach_tools_option() -> None:
    """attach's tools picks which of its own tools it adds: all three by
    default, the ones it names, or none; an unknown name is refused."""
    assert BUILTIN_TOOLS == ("subscribe_events", "unsubscribe_events", "list_subscriptions")

    async def listed(tools: list[str] | None) -> list[str]:
        wake = Wakeshift(app_key("cards"), core="http://core.test")
        wake.define_event("table.news", description="News at a table.", args=TableArgs, payload=TurnMsg, text=lambda m, a: m.text)
        server = MCPServer("cards")
        wake.attach(server, tools=tools)
        return sorted(t.name for t in await server.list_tools())

    assert await listed(None) == ["list_subscriptions", "subscribe_events", "unsubscribe_events"]
    assert await listed(["list_subscriptions", "unsubscribe_events"]) == ["list_subscriptions", "unsubscribe_events"]
    assert await listed([]) == []
    wake = Wakeshift(app_key("cards"), core="http://core.test")
    wake.define_event("table.news", description="News at a table.", args=TableArgs, payload=TurnMsg, text=lambda m, a: m.text)
    with pytest.raises(ValueError, match="'subscribe'"):
        wake.attach(MCPServer("cards"), tools=["subscribe"])


async def test_schema_hides_call_and_adds_wake_args() -> None:
    w = Wrapped()

    @w.attached.tool(description="Take a seat.")
    async def join(table: str, call: ToolCall, seats: int = 1) -> str:
        return ""

    @w.attached.tool()
    async def with_ctx(ctx: Context, call: ToolCall, harness: str | None = None) -> str:
        """Own context and harness."""
        assert ctx is call.context
        return f"h={harness}"

    tools = {t.name: t for t in await w.server.list_tools()}
    props = tools["join"].input_schema["properties"]
    assert set(props) == {"table", "seats", "session_key", "harness", "conversation_id"}
    assert props["session_key"]["description"] == SESSION_DESCRIPTION
    assert tools["join"].input_schema.get("required") == ["table"]
    assert tools["join"].description == "Take a seat."
    assert tools["with_ctx"].description == "Own context and harness."
    assert set(tools["with_ctx"].input_schema["properties"]) == {"session_key", "harness", "conversation_id"}
    # The registered wrapper hides call from its signature and its evaluated annotations.
    import inspect

    fn = w.server._tool_manager.get_tool("join").fn
    assert "call" not in inspect.signature(fn).parameters and "call" not in fn.__annotations__
    assert fn.__annotations__["table"] is str and fn.__annotations__["return"] is str
    assert "wakeshift_context" in fn.__annotations__
    async with w.served() as (_, rpc):
        assert texts(await call_tool(rpc, "with_ctx", {"harness": "codex"})) == ["h=codex"]


async def test_personal_and_other_app_checks() -> None:
    w = Wrapped(identify=None)
    other = Wakeshift(app_key("other"), core="http://core.test")
    foreign = other.define_event("x.y", description="X.", args=TableArgs, payload=TurnMsg, text=lambda m, a: "")
    errs: list[BaseException] = []

    @w.attached.tool()
    async def join(call: ToolCall) -> str:
        for ev in (w.turn, foreign):
            try:
                await call.subscribe(ev, {"table": "oak"})
            except Exception as err:
                errs.append(err)
        return "ok"

    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {})
    assert isinstance(errs[0], WakeshiftError) and errs[0].code == "bad_arguments" and "personal" in str(errs[0])
    assert is_code(errs[1], "unknown_event") and "another app" in str(errs[1])
    assert "/event-subscriptions" not in w.core.paths()


async def test_in_flight_subscribe_is_waited_for() -> None:
    w = Wrapped()
    w.core.delay = 0.1

    @w.attached.tool()
    async def join(call: ToolCall) -> str:
        asyncio.ensure_future(call.subscribe(w.shared, {"table": "oak"}))
        return "ok"

    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "join", {})
    assert w.core.paths() == ["/event-subscriptions", "/calls/addition"], w.core.paths()
    assert texts(res)[0].startswith("ok\n\n" + WOKEN)
    assert any("in flight" in m for m in w.warnings)


# ── Native callers ────────────────────────────────────────────────────────


async def test_native_callers() -> None:
    w = Wrapped(events=EventsOptions(native=lambda ctx: ctx.request_context.request.headers.get("x-house") == "yes"))
    w.core.addition = {"text": 'To be woken by game.turn with {"table":"oak"}, subscribe to it (MCP Events).'}
    w.core.native = [
        {"id": "n1", "event": "game.turn", "arguments": {"table": "oak"}, "destination": "webhook"},
        {"id": "n2", "event": "game.turn", "arguments": {"table": "elm"}, "destination": "webhook"},
        {"id": "c1", "event": "game.turn", "arguments": {"table": "oak"}, "destination": "conversation"},
    ]
    seen: list[tuple[bool, str | None]] = []
    subs: list[Subscription] = []
    ended: list[bool] = []

    @w.attached.tool()
    async def join(table: str, call: ToolCall) -> str:
        seen.append((call.native, call.native_conversation))
        subs.append(await call.subscribe(w.turn, {"table": table}))
        subs.append(await call.subscribe(w.turn, {"table": table}))  # once in the block
        return "Seated."

    @w.attached.tool()
    async def leave(table: str, call: ToolCall) -> str:
        ended.append(await call.unsubscribe(w.turn, {"table": table}))
        return "Left."

    cases: list[tuple[str, str, dict[str, Any] | None, str | None]] = [
        ("ChatGPT's user agent", "openai-mcp/1.0.0", None, None),
        ("ChatGPT's conversation", "some-client/1", {"openai/session": "chat-9"}, "chat-9"),
        (
            "a client that declares MCP Events",
            "some-client/1",
            {"io.modelcontextprotocol/clientCapabilities": {"extensions": {"io.modelcontextprotocol/events": {}}}},
            None,
        ),
        ("ChatGPT's client name", "x/1", {"io.modelcontextprotocol/clientInfo": {"name": "openai-mcp", "version": "1"}}, None),
    ]
    async with w.served() as (_, rpc):
        for name, ua, meta, conv in cases:
            w.core.reset()
            seen.clear()
            subs.clear()
            res = await call_tool(rpc, "join", {"table": "oak"}, ua=ua, meta=meta)
            assert not res["result"].get("isError"), (name, res)
            assert seen == [(True, conv)], name
            assert subs[0].native and subs[0].id == "" and not subs[0].created and subs[0].user == "ada", name
            assert w.core.paths() == ["/calls/addition"], name
            assert w.core.bodies("/calls/addition")[0]["native"] == [{"name": "game.turn", "arguments": {"table": "oak"}}]
            assert "subscribe to it (MCP Events)" in "\n".join(texts(res))
        seen.clear()
        await call_tool(rpc, "join", {"table": "oak"}, headers={"X-House": "yes"})
        assert seen == [(True, None)]
        seen.clear()
        await call_tool(rpc, "join", {"table": "oak"})
        assert seen == [(False, None)]  # Claude Code isn't native
        w.core.reset()
        await call_tool(rpc, "leave", {"table": "oak"}, ua="openai-mcp/1.0.0")
    ends = w.core.bodies("/event-subscriptions/end")
    assert ended == [True] and ends == [{"user": "ada", "id": "n1"}]


async def test_identify_client() -> None:
    """identify's client names the client for a request that doesn't (a
    stateless server never sees initialize): the call sends it, it decides
    native, and the request's own name wins."""
    clients = {"ada": "Claude Code", "gpt": "openai-mcp"}

    def identify(ctx: Any) -> Identity:
        user = ctx.request.headers.get("x-user") if ctx.request is not None else None
        return Identity(user=user, client=clients.get(user or ""))

    w = Wrapped(identify=identify)
    native: list[bool] = []

    @w.attached.tool()
    async def join(table: str, call: ToolCall) -> str:
        native.append(call.native)
        await call.subscribe(w.shared, {"table": table})
        return "Seated."

    def client_of() -> Any:
        subs = w.core.bodies("/event-subscriptions")
        assert len(subs) == 1, subs
        return subs[0]["call"]["client"]

    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {"table": "oak"})
        assert client_of() == "Claude Code" and native == [False]
        w.core.reset()
        await call_tool(
            rpc, "join", {"table": "oak"}, meta={"io.modelcontextprotocol/clientInfo": {"name": "Cursor", "version": "1"}}
        )
        assert client_of() == "Cursor", "the request's own name"
        w.core.reset()
        await call_tool(rpc, "join", {"table": "oak"}, user="house")
        assert client_of() == "", "no name"
        w.core.reset()
        await call_tool(rpc, "join", {"table": "oak"}, user="gpt", ua="x/1")
        assert native[-1] is True and w.core.bodies("/event-subscriptions") == [], "ChatGPT by identify's name"
        w.core.reset()
        await call_tool(rpc, "subscribe_events", {"event": "table.news", "arguments": {"table": "oak"}})
        assert client_of() == "Claude Code", "subscribe_events"


# ── Typed output and the session argument ─────────────────────────────────


async def test_typed_output() -> None:
    w = Wrapped()

    @w.attached.tool()
    async def join(table: str, call: ToolCall) -> View:
        await call.subscribe(w.shared, {"table": table})
        return View(table=table)

    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "join", {"table": "oak"})
        assert res["result"]["structuredContent"] == {"table": "oak", "wakeshift": WOKEN}
        t = texts(res)
        assert len(t) == 3 and t[1] == WOKEN and t[2] == TRAILER
    assert not w.warnings


class StrictView(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: str


async def test_structured_output_carries_the_block() -> None:
    """Any structured result of a wrapped tool carries the block in
    wakeshift, without opting in, and tools/list's output schema names it,
    so a client that checks structuredContent against the schema takes it."""
    w = Wrapped()

    @w.attached.tool()
    async def join_plain(table: str, call: ToolCall) -> PlainView:
        await call.subscribe(w.shared, {"table": table})
        return PlainView(table=table)

    @w.attached.tool()
    async def join_strict(table: str, call: ToolCall) -> StrictView:
        await call.subscribe(w.shared, {"table": table})
        return StrictView(table=table)

    @w.attached.tool()
    async def seats(table: str, call: ToolCall) -> list[str]:
        await call.subscribe(w.shared, {"table": table})
        return ["ada", "bo"]

    @w.server.tool()
    async def unwrapped(table: str) -> PlainView:
        return PlainView(table=table)

    async with w.served() as (_, rpc):
        listed = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
        for name in ("join_plain", "join_strict", "seats"):
            res = await call_tool(rpc, name, {"table": "oak"})
            assert res["result"]["structuredContent"]["wakeshift"] == WOKEN, name
            assert texts(res)[-2:] == [WOKEN, TRAILER], name
            assert listed[name]["outputSchema"]["properties"]["wakeshift"]["type"] == "string", name
        assert res["result"]["structuredContent"] == {"result": ["ada", "bo"], "wakeshift": WOKEN}
        assert listed["join_strict"]["outputSchema"]["additionalProperties"] is False
        assert "wakeshift" not in listed["join_strict"]["outputSchema"].get("required", [])
        # A tool that isn't wrapped gets no block, and its schema is its own.
        assert "wakeshift" not in listed["unwrapped"]["outputSchema"]["properties"]
    # The registered schema itself is left alone.
    assert "wakeshift" not in w.server._tool_manager.get_tool("join_plain").output_schema["properties"]
    assert not w.warnings


async def test_str_tool_replies_with_text() -> None:
    """A wrapped tool returning str has no output schema and replies with
    text only, the block in it, so a host that shows structuredContent
    instead of the text can't miss the block. structured_output=True keeps
    MCPServer's {"result": …}, which carries the block too."""
    w = Wrapped()

    @w.attached.tool()
    async def watch(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return f"Watching {table}."

    @w.attached.tool(structured_output=True)
    async def watch_structured(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return f"Watching {table}."

    async with w.served() as (_, rpc):
        listed = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
        assert listed["watch"].get("outputSchema") is None
        res = await call_tool(rpc, "watch", {"table": "oak"})
        assert res["result"].get("structuredContent") is None
        assert texts(res) == [f"Watching oak.\n\n{WOKEN}\n\n{TRAILER}"]
        assert "wakeshift" in listed["watch_structured"]["outputSchema"]["properties"]
        res = await call_tool(rpc, "watch_structured", {"table": "oak"})
        assert not res["result"].get("isError"), res
        assert res["result"]["structuredContent"] == {"result": "Watching oak.", "wakeshift": WOKEN}
    # Neither call was refused or rolled back.
    assert w.core.bodies("/event-subscriptions/unsubscribe") == []
    assert not w.warnings


async def test_no_session_alias() -> None:
    """session_key is the one name for the session argument: session, its
    old name, isn't taken as it."""
    w = Wrapped()
    got: list[str | None] = []

    @w.attached.tool()
    async def join(table: str, call: ToolCall) -> str:
        got.append(call.session)
        return "ok"

    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {"table": "oak", "session": "claude-code#2"})
    assert got == [None]


# ── The shared fixtures ───────────────────────────────────────────────────


def test_wrapped_block_golden() -> None:
    f = golden_file("wrapped-block.json")
    for c in f["cases"]:
        add = c.get("addition", f["addition"])
        string = isinstance(c["result"], str)
        result = {"content": [{"type": "text", "text": c["result"]}]} if string else c["result"]
        got = merge_block(result, add.get("text", ""), add.get("trailer", ""), add.get("meta"), string=string)
        assert got == c["want"], c["name"]
    for c in f["output_schemas"]:
        before = json.dumps(c["schema"], sort_keys=True)
        assert with_wakeshift_field(c["schema"]) == c["want"], c["name"]
        assert json.dumps(c["schema"], sort_keys=True) == before, c["name"]


def _recipient(v: Any) -> Recipient:
    if isinstance(v, str):
        return Recipient(user=v)
    if "native" in v:
        return Recipient(native=v["native"])
    return Recipient(user=v["user"], subscription=v["subscription"])


def _plain(r: Recipient) -> Any:
    if r.native is not None:
        return {"native": r.native}
    if r.subscription is not None:
        return {"user": r.user, "subscription": r.subscription}
    return r.user


async def test_emit_recipients_golden() -> None:
    f = golden_file("emit-recipients.json")
    w = Wrapped()
    for c in f["cases"]:
        w.core.reset()
        await w.turn.emit({"table": "oak"}, {"text": "go"}, to=[_recipient(r) for r in c["to"]])
        assert w.core.bodies("/events/emit")[0]["to"] == c["wire"], c["name"]
    for c in f["responses"]:
        resp = c["response"]
        http = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda r, resp=resp: httpx.Response(200, json=resp if r.url.path.endswith("/emit") else {})
            )
        )
        wake = Wakeshift(app_key("cards"), core="http://core.test", http=http)
        ev = wake.define_event(
            "game.turn", description="T.", args=TableArgs, payload=TurnMsg, text=lambda m, a: m.text, personal=True
        )
        out = await ev.emit({"table": "oak"}, {"text": "go"}, to="u1")
        assert isinstance(out, Emitted)
        got = {
            "matched": out.matched,
            "duplicate": out.duplicate,
            "unmatched": [_plain(r) for r in out.unmatched],
            "ended": [_plain(r) for r in out.ended],
        }
        assert got == c["emitted"], c["name"]
    for c in f["refused"]:
        w.core.reset()
        with pytest.raises(WakeshiftError) as e:
            await w.turn.emit({"table": "oak"}, {"text": "go"}, to=[_recipient(r) for r in c["to"]])
        assert e.value.code == f["refused_code"], c["name"]
        assert w.core.bodies("/events/emit") == [], c["name"]
    # A personal event's status and publish URL name one user, trimmed the same way.
    one = f["one_user"]
    w.core.reset()
    await w.turn.status({"table": "oak"}, to=one["to"])
    await w.turn.publish_url({"table": "oak"}, to=one["to"])
    assert w.core.bodies("/events/status")[0]["to"] == one["wire"]
    assert w.core.bodies("/events/publish-urls")[0]["to"] == one["wire"]
    w.core.reset()
    for to in one["refused"]:
        with pytest.raises(WakeshiftError) as e:
            await w.turn.status({"table": "oak"}, to=to)
        assert e.value.code == one["refused_code"], repr(to)
        with pytest.raises(WakeshiftError) as e:
            await w.turn.publish_url({"table": "oak"}, to=to)
        assert e.value.code == one["refused_code"], repr(to)
    so = f["status_of"]
    await w.turn.status_of([Recipient(user=s["user"], subscription=s["subscription"]) for s in so["subscriptions"]])
    assert w.core.bodies("/events/status")[0]["subscriptions"] == so["wire"]
    w.core.reset()
    for subs in so["refused"]:
        with pytest.raises(WakeshiftError) as e:
            await w.turn.status_of([Recipient(user=s["user"], subscription=s["subscription"]) for s in subs])
        assert e.value.code == so["refused_code"], repr(subs)
    assert w.core.calls == []


async def test_emit_forms() -> None:
    w = Wrapped()
    out = await w.turn.emit(
        {"table": "oak"},
        {"text": "go"},
        to=["u1", "u3", Recipient(user="u2", subscription="s2"), Recipient(native="u4")],
        id="m1:mention",
    )
    body = w.core.bodies("/events/emit")[0]
    assert body["to"] == ["u1", "u3", {"user": "u2", "subscription": "s2"}, {"native": "u4"}] and body["id"] == "m1:mention"
    assert out.unmatched == [Recipient(user="u3"), Recipient(native="u4")] and out.ended == [
        Recipient(user="u2", subscription="s2")
    ]
    w.core.reset()
    await w.turn.emit({"table": "oak"}, {"text": "go"}, to=Recipient(user="u2", subscription="s2"))
    assert w.core.bodies("/events/emit")[0]["to"] == [{"user": "u2", "subscription": "s2"}]
    with pytest.raises(WakeshiftError, match="isn't personal"):
        await w.shared.emit({"table": "oak"}, {"text": "go"}, to=Recipient(native="u1"))
    with pytest.raises(WakeshiftError, match="personal"):
        await w.turn.publish_url({"table": "oak"})


async def test_status() -> None:
    w = Wrapped()
    got = await w.turn.status({"table": "oak"}, to=" u1 ")
    assert got == EventStatus(subscriptions=3, connected=2, via={"stream": 1, "poll": 1, "hooks": 0, "webhook": 0})
    assert w.core.bodies("/events/status")[0] == {"name": "game.turn", "arguments": {"table": "oak"}, "to": "u1"}
    w.core.reset()
    subs = await w.turn.status_of([Recipient(user="u1", subscription="s1"), Recipient(user="u2", subscription="s2")])
    assert subs == [
        EventSubscriptionStatus(id="s1", user="u1", live=True, connected=True, via="stream"),
        EventSubscriptionStatus(id="s2", user="u2", live=False, connected=False),
    ]
    assert w.core.bodies("/events/status")[0] == {
        "name": "game.turn",
        "subscriptions": [{"user": "u1", "id": "s1"}, {"user": "u2", "id": "s2"}],
    }
    w.core.reset()
    assert await w.turn.status_of([]) == []
    with pytest.raises(WakeshiftError, match="personal"):
        await w.turn.status({"table": "oak"})
    with pytest.raises(WakeshiftError, match="isn't personal"):
        await w.shared.status({"table": "oak"}, to="u1")
    with pytest.raises(WakeshiftError, match="user and subscription"):
        await w.turn.status_of([Recipient(user="", subscription="s1")])
    with pytest.raises(WakeshiftError, match="at most 1000"):
        await w.turn.status_of([Recipient(user="u", subscription="s")] * 1001)
    assert w.core.bodies("/events/status") == []


def test_conversation_meta_golden() -> None:
    assert list(CONVERSATION_META) == golden_file("conversation-meta.json")["keys"]
    assert conversation_key({"x-codex-turn-metadata": {"thread_id": "t1"}}) == "t1"
    assert conversation_key({"antigravity.google/conversation_id": "c9"}) == "c9"
    assert conversation_key({"vscode": {"conversationId": "v1"}}) == "v1"
    assert conversation_key({"other": "x"}) == ""


async def test_client_checks_golden() -> None:
    """The mistakes every SDK refuses before asking Wakeshift, each a
    WakeshiftError with the status and code Wakeshift answers the same
    mistake with (errors.json's client_checks, which the Go and Node SDKs
    test too)."""
    cases = golden_file("errors.json")["client_checks"]["cases"]
    assert cases
    w = Wrapped()
    anon = Wrapped(identify=None)
    other = Wakeshift(app_key("other"), core="http://core.test")
    # Another app's event, by the same name as one of this app's.
    foreign = other.define_event(
        "game.turn", description="X.", args=TableArgs, payload=TurnMsg, text=lambda m, a: "", personal=True
    )
    pending: list[Any] = []
    thrown: list[BaseException] = []

    for app in (w, anon):

        @app.attached.tool()
        async def check(call: ToolCall) -> str:
            try:
                await pending[0](call)
            except Exception as err:
                thrown.append(err)
            return "ok"

    async def tool(app: Wrapped, f: Any) -> None:
        pending[:] = [f]
        thrown.clear()
        async with app.served() as (_, rpc):
            await call_tool(rpc, "check", {})
        if thrown:
            raise thrown[0]

    oak, msg = {"table": "oak"}, {"text": "go"}
    ops: dict[str, Any] = {
        "emit_personal_without_recipient": lambda: w.turn.emit(oak, msg),
        "emit_shared_with_recipient": lambda: w.shared.emit(oak, msg, to="ada"),
        "emit_recipient_without_user": lambda: w.turn.emit(oak, msg, to=[" "]),
        "emit_recipient_without_subscription": lambda: w.turn.emit(oak, msg, to=Recipient(user="ada", subscription=" ")),
        "emit_bad_arguments": lambda: w.turn.emit({"table": 7}, msg),
        "emit_bad_payload": lambda: w.turn.emit(oak, {"text": 7}),
        "publish_url_personal_without_to": lambda: w.turn.publish_url(oak),
        "publish_url_shared_with_to": lambda: w.shared.publish_url(oak, to="ada"),
        "publish_url_two_users": lambda: w.turn.publish_url(oak, to=["ada", "bo"]),  # type: ignore[arg-type]
        "status_personal_without_to": lambda: w.turn.status(oak),
        "status_shared_with_to": lambda: w.shared.status(oak, to="ada"),
        "status_two_users": lambda: w.turn.status(oak, to=["ada", "bo"]),  # type: ignore[arg-type]
        "status_of_too_many": lambda: w.turn.status_of([Recipient(user="ada", subscription="s")] * 1001),
        "status_of_without_user": lambda: w.turn.status_of([Recipient(subscription="s1")]),
        "subscribe_another_apps_event": lambda: tool(w, lambda call: call.subscribe(foreign, oak)),
        "unsubscribe_another_apps_event": lambda: tool(w, lambda call: call.unsubscribe(foreign, oak)),
        "subscribe_personal_without_user": lambda: tool(anon, lambda call: call.subscribe(anon.turn, oak)),
        "subscribe_bad_arguments": lambda: tool(w, lambda call: call.subscribe(w.turn, {"table": 7})),
    }
    for c in cases:
        if (c.get("skip") or {}).get("python"):
            assert c["op"] not in ops, f"{c['name']}: skipped for Python, but checked"
            continue
        assert c["op"] in ops, f"{c['name']}: no op {c['op']} here"
        w.core.reset()
        anon.core.reset()
        with pytest.raises(WakeshiftError) as e:
            await ops[c["op"]]()
        err = e.value
        assert (err.status, err.code, is_code(err, c["code"])) == (c["status"], c["code"], True), c["name"]
        assert c["says"] in err.detail and not err.detail.startswith("wakeshift:"), (c["name"], err.detail)
        asked = [p for p in w.core.paths() + anon.core.paths() if p != "/calls/addition"]
        assert asked == [], (c["name"], asked)
