"""attach on FastMCP 4 (docs/fastmcp.md): what's FastMCP's own, over the wire
and through FastMCP's own client. The rest of the suite runs against FastMCP
with WAKESHIFT_TEST_SERVER=fastmcp (conftest.py)."""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx2
import pytest

fastmcp = pytest.importorskip("fastmcp")

from fastmcp import Client, Context, FastMCP  # noqa: E402
from fastmcp.client.transports import StreamableHttpTransport  # noqa: E402
from fastmcp.dependencies import CurrentContext, Depends  # noqa: E402
from fastmcp.server.auth.providers.jwt import StaticTokenVerifier  # noqa: E402
from fastmcp.server.middleware import Middleware  # noqa: E402
from fastmcp.server.transforms import ToolTransform  # noqa: E402
from fastmcp.tools.tool_transform import ToolTransformConfig  # noqa: E402
from mcp_types import ToolAnnotations  # noqa: E402
from pydantic import BaseModel  # noqa: E402

from wakeshift import Identity, ToolCall, WakeOutput, Wakeshift  # noqa: E402
from wakeshift._hosts import FastMCPHost, host_of  # noqa: E402
from wakeshift.fastmcp import access_token, identify_by_claim  # noqa: E402
from wakeshift.spec import EXTENSION_KEY  # noqa: E402
from wakeshift.tasks import TASKS_EXTENSION, WAIT_TOOL  # noqa: E402

from .helpers import Rpc, app_key, golden_file  # noqa: E402
from .test_toolcall import TRAILER, WOKEN, WakeCore  # noqa: E402

TOKENS = {
    "tok-ada": {"client_id": "cli", "scopes": [], "sub": "ada"},
    "tok-bo": {"client_id": "cli", "scopes": [], "sub": "bo", "client_name": "bo-agent"},
}

MODERN = "2026-07-28"
UA = {"User-Agent": "claude-code/2.1.314"}


def modern_meta(**caps: Any) -> dict[str, Any]:
    """A 2026-07-28 request's _meta: its version, client and capabilities."""
    return {
        "io.modelcontextprotocol/protocolVersion": MODERN,
        "io.modelcontextprotocol/clientInfo": {"name": "claude-code", "version": "2.1.314"},
        "io.modelcontextprotocol/clientCapabilities": caps,
    }


def modern_headers(method: str, name: str | None = None) -> dict[str, str]:
    h = {"MCP-Protocol-Version": MODERN, "Mcp-Method": method}
    if name:
        h["Mcp-Name"] = name
    return h


class TableArgs(BaseModel):
    table: str


class TurnMsg(BaseModel):
    text: str


class Seat(WakeOutput):
    table: str
    seat: int


class Plain(BaseModel):
    table: str


def lucky() -> int:
    return 7


class App:
    """A FastMCP app with bearer tokens, a personal event and wrapped tools."""

    def __init__(self, identify: Any = None, **server_kw: Any) -> None:
        self.core = WakeCore()
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(self.core.handler))
        self.warnings: list[str] = []
        self.wake = Wakeshift(app_key("cards"), core="http://core.test", http=http, on_warning=self.warnings.append)
        self.turn = self.wake.define_event(
            "game.turn", description="Your turn.", args=TableArgs, payload=TurnMsg, text=lambda m, a: m.text, personal=True
        )
        self.mcp = FastMCP("cards", auth=StaticTokenVerifier(tokens=TOKENS), **server_kw)
        self.seen: list[Any] = []
        self.contexts: list[Any] = []
        outer = self

        class Observe(Middleware):
            async def on_call_tool(self, context: Any, call_next: Any) -> Any:
                out = await call_next(context)
                outer.seen.append(out)
                return out

        self.mcp.add_middleware(Observe())
        self.attached = self.wake.attach(
            self.mcp, identify=identify or identify_by_claim("sub"), tools={"wait": {"enabled": True}}
        )

        @self.attached.tool(subscribes=[self.turn])
        async def join(table: str, call: ToolCall) -> str:
            """Take a seat."""
            await call.subscribe(self.turn, {"table": table})
            return f"Seated at {table}."

        @self.attached.tool(subscribes=[self.turn])
        async def sit(table: str, call: ToolCall) -> Seat:
            """Take a numbered seat."""
            await call.subscribe(self.turn, {"table": table})
            return Seat(table=table, seat=3)

        @self.attached.tool(subscribes=[self.turn])
        async def boom(table: str, call: ToolCall) -> str:
            await call.subscribe(self.turn, {"table": table})
            raise ValueError("the table is full")

        @self.attached.tool(subscribes=[self.turn])
        async def bad_output(table: str, call: ToolCall) -> Plain:
            await call.subscribe(self.turn, {"table": table})
            return {"seat": 3}  # type: ignore[return-value]  # no table: the output schema refuses it

        @self.attached.tool(
            name="peek",
            subscribes=[self.turn],
            description="Look at a table.",
            tags={"cards"},
            annotations=ToolAnnotations(read_only_hint=True),
            meta={"area": "lobby"},
            timeout=30,
        )
        async def peek_impl(table: str, ctx: Context, call: ToolCall, n: int = Depends(lucky)) -> str:  # noqa: B008
            outer.contexts.append(ctx)
            await call.subscribe(self.turn, {"table": table})
            return f"{table}: {n} seats on {ctx.fastmcp.name}, request {ctx.request_id}."

        @self.attached.tool(subscribes=[self.turn])
        async def current(table: str, call: ToolCall, ctx: Context = CurrentContext()) -> str:  # noqa: B008
            outer.contexts.append(ctx)
            await call.subscribe(self.turn, {"table": table})
            return f"user {call.user} at {table}"

        @self.mcp.tool
        async def plain(table: str) -> Plain:
            """Not wrapped: no block."""
            return Plain(table=table)

    def app(self, stateless: bool = True, json_response: bool = True) -> Any:
        return self.mcp.http_app(path="/mcp", stateless_http=stateless, json_response=json_response)


class Served:
    def __init__(self, app: Any) -> None:
        self.app = app
        self.clients: list[httpx2.AsyncClient] = []

    def rpc(self, token: str | None = "tok-ada") -> Rpc:
        client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=self.app), base_url="http://127.0.0.1:8000")
        self.clients.append(client)
        return Rpc(client, "/mcp", headers={"Authorization": f"Bearer {token}"} if token else {})

    async def close(self) -> None:
        for c in self.clients:
            await c.aclose()


@asynccontextmanager
async def serve(app: Any) -> AsyncIterator[Served]:
    """app (a FastMCP http_app) served in-process."""
    async with app.router.lifespan_context(app):
        s = Served(app)
        try:
            yield s
        finally:
            await s.close()


def texts(res: dict[str, Any]) -> list[str]:
    assert "result" in res, res
    return [c.get("text", "") for c in res["result"]["content"]]


async def call_tool(rpc: Rpc, name: str, args: dict[str, Any], **kw: Any) -> dict[str, Any]:
    return await rpc("tools/call", {"name": name, "arguments": args}, headers={**UA, **kw.pop("headers", {})}, **kw)


# ── Identity ─────────────────────────────────────────────────────────────


async def test_identity_comes_from_fastmcp_auth_per_call() -> None:
    a = App()
    async with serve(a.app()) as s:
        bo, ada = s.rpc("tok-bo"), s.rpc("tok-ada")
        res = await call_tool(bo, "join", {"table": "oak"})
        assert texts(res) == [f"Seated at oak.\n\n{WOKEN}\n\n{TRAILER}"]
        # The next call, on the same server, is the next caller's.
        await call_tool(ada, "join", {"table": "elm"})
        subs = a.core.bodies("/event-subscriptions")
        assert [b["call"]["user"] for b in subs] == ["bo", "ada"]
        assert subs[0]["call"]["tool"] == "join" and subs[0]["call"]["user_agent"] == "claude-code/2.1.314"
        # Native events/subscribe and the wait tool: the token's user too.
        a.core.reset()
        await bo(
            "events/subscribe",
            {"name": "game.turn", "arguments": {"table": "oak"}, "delivery": {"mode": "webhook", "url": "https://x.test/h"}},
        )
        assert a.core.bodies("/events/subscribe")[0]["call"]["user"] == "bo"
        await bo(
            "tools/call",
            {"name": WAIT_TOOL, "arguments": {"session_key": "wsc_s1"}, "_meta": modern_meta(extensions={TASKS_EXTENSION: {}})},
            headers=modern_headers("tools/call", WAIT_TOOL),
        )
        wait = a.core.bodies("/calls/task")[0]["call"]
        assert wait["user"] == "bo" and wait["session"] == "wsc_s1" and wait["tasks"] is True
        # Without a token FastMCP refuses the request before attach sees it.
        assert (await s.rpc(None)("tools/list", {})) == {"status": 401}


async def test_identify_by_claim_names_the_client_too() -> None:
    a = App(identify=identify_by_claim("sub", client_claim="client_name"))
    async with serve(a.app()) as s:
        await call_tool(s.rpc("tok-bo"), "join", {"table": "oak"}, headers={"User-Agent": "x"})
        call = a.core.bodies("/event-subscriptions")[0]["call"]
        assert call["user"] == "bo" and call["client"] == "bo-agent"


def test_identify_by_claim_without_a_request() -> None:
    assert access_token() is None
    assert identify_by_claim("sub")(None) is None
    assert Identity() == Identity(user=None)


# ── The wire: 2026-07-28, capabilities, lists ────────────────────────────


async def test_wire_schemas_and_capabilities() -> None:
    a = App()
    async with serve(a.app()) as s:
        rpc = s.rpc()
        init = await rpc(
            "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "c", "version": "1"}}
        )
        caps = init["result"]["capabilities"]
        assert caps["events"] == {"listChanged": False}
        assert caps["extensions"][EXTENSION_KEY] == {"listChanged": False}
        meta = modern_meta(extensions={TASKS_EXTENSION: {}})
        d = await rpc("server/discover", {"_meta": meta}, headers=modern_headers("server/discover"))
        r = d["result"]
        assert r["resultType"] == "complete" and MODERN in r["supportedVersions"]
        assert isinstance(r["ttlMs"], int) and r["cacheScope"] in ("public", "private")
        assert r["capabilities"]["events"] == {"listChanged": False}
        assert set(r["capabilities"]["extensions"]) >= {EXTENSION_KEY, TASKS_EXTENSION}
        listed = await rpc("tools/list", {"_meta": meta}, headers=modern_headers("tools/list"))
        lr = listed["result"]
        assert lr["resultType"] == "complete" and isinstance(lr["ttlMs"], int)
        # The list differs by what each request declares: this client's only.
        assert lr["cacheScope"] == "private"
        tools = {t["name"]: t for t in lr["tools"]}
        assert WAIT_TOOL in tools
        props = tools["join"]["inputSchema"]["properties"]
        assert set(props) == {"table", "session_key", "harness", "conversation_id"}
        # A str tool replies with text: no output schema.
        assert "outputSchema" not in tools["join"]
        assert "wakeshift" in tools["sit"]["outputSchema"]["properties"]
        assert "wakeshift" not in tools["plain"]["outputSchema"]["properties"]
        assert tools["subscribe_events"]["inputSchema"]["properties"]["event"]["enum"] == ["game.turn"]
        # attach's tools and the wait tool carry every SDK's annotations (tool-annotations.json).
        annotations = golden_file("tool-annotations.json")
        for name in ("subscribe_events", "unsubscribe_events", "list_subscriptions", WAIT_TOOL):
            assert tools[name]["annotations"] == annotations[name], name
        # The decorator's options reach FastMCP; Context and Depends parameters are hidden.
        peek = tools["peek"]
        assert peek["annotations"]["readOnlyHint"] is True
        # It declares game.turn: the sentence follows its own description.
        assert peek["description"].startswith("Look at a table. This tool is how ")
        assert " notifies you in this conversation of this event: Your turn. " in peek["description"]
        assert peek["_meta"]["area"] == "lobby"
        assert set(peek["inputSchema"]["properties"]) == {"table", "session_key", "harness", "conversation_id"}
        assert set(tools["current"]["inputSchema"]["properties"]) == {"table", "session_key", "harness", "conversation_id"}
        # A client that declares MCP Events isn't offered subscribe_events, or the wait tool without tasks.
        res = await rpc(
            "tools/list", {"_meta": modern_meta(extensions={EXTENSION_KEY: {}})}, headers=modern_headers("tools/list")
        )
        names = [t["name"] for t in res["result"]["tools"]]
        assert "subscribe_events" not in names and WAIT_TOOL not in names and "join" in names


async def test_a_public_cache_scope_becomes_private_on_tools_list_only() -> None:
    a = App(cache_ttl=60, cache_scope="public")  # seconds
    async with serve(a.app()) as s:
        rpc = s.rpc()
        meta = modern_meta()
        lr = (await rpc("tools/list", {"_meta": meta}, headers=modern_headers("tools/list")))["result"]
        assert lr["ttlMs"] == 60_000 and lr["cacheScope"] == "private"
        d = (await rpc("server/discover", {"_meta": meta}, headers=modern_headers("server/discover")))["result"]
        assert d["cacheScope"] == "public"


# ── The block, rollback, middleware, Context ──────────────────────────────


@pytest.mark.filterwarnings("ignore:Pydantic serializer warnings")  # bad_output's, on purpose
async def test_block_rollback_and_fastmcp_middleware() -> None:
    a = App()
    async with serve(a.app()) as s:
        rpc = s.rpc()
        r = (await call_tool(rpc, "sit", {"table": "oak"}))["result"]
        assert r["structuredContent"] == {"table": "oak", "seat": 3, "wakeshift": WOKEN}
        assert [c["text"] for c in r["content"]][-2:] == [WOKEN, TRAILER]
        # FastMCP's own middleware sees the tool's result, before the block.
        assert a.seen and "wakeshift-beacon" not in str(a.seen[-1].content)
        # A str tool: text only, the block in it.
        r = (await call_tool(rpc, "join", {"table": "oak"}))["result"]
        assert "structuredContent" not in r and r["content"][-1]["text"].endswith(TRAILER)
        # A tool that isn't wrapped gets nothing.
        a.core.reset()
        r = (await call_tool(rpc, "plain", {"table": "oak"}))["result"]
        assert r["structuredContent"] == {"table": "oak"} and a.core.paths() == []
        for tool in ("boom", "bad_output"):
            a.core.reset()
            res = await call_tool(rpc, tool, {"table": f"elm-{tool}"})
            assert res["result"]["isError"] is True, (tool, res)
            assert not any("wakeshift-beacon" in t for t in texts(res)), tool
            ends = a.core.bodies("/event-subscriptions/unsubscribe")
            assert len(ends) == 1 and ends[0]["reason"] == "rolled_back", tool
            assert "/calls/addition" not in a.core.paths(), tool
        assert "output schema" in texts(res)[0]


async def test_context_and_dependencies_are_filled() -> None:
    a = App()
    async with serve(a.app()) as s:
        rpc = s.rpc("tok-bo")
        res = await call_tool(rpc, "peek", {"table": "oak"})
        assert texts(res)[0].startswith("oak: 7 seats on cards, request ")
        res = await call_tool(rpc, "current", {"table": "oak"})
        assert texts(res)[0].startswith("user bo at oak")
        assert len(a.contexts) == 2 and all(isinstance(c, Context) for c in a.contexts)
        subs = a.core.bodies("/event-subscriptions")
        assert [b["call"]["tool"] for b in subs] == ["peek", "current"]
    assert not a.warnings


async def test_a_renamed_tool_keeps_its_block_and_schema() -> None:
    a = App()
    a.mcp.add_transform(ToolTransform({"sit": ToolTransformConfig(name="take_seat")}))
    async with serve(a.app()) as s:
        rpc = s.rpc()
        tools = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
        assert "sit" not in tools and "wakeshift" in tools["take_seat"]["outputSchema"]["properties"]
        r = (await call_tool(rpc, "take_seat", {"table": "oak"}))["result"]
        assert r["structuredContent"]["wakeshift"] == WOKEN
        assert a.core.bodies("/event-subscriptions")[0]["call"]["tool"] == "take_seat"


async def test_a_background_task_tool_is_refused() -> None:
    a = App()
    with pytest.raises(TypeError, match="background task"):

        @a.attached.tool(task=True)
        async def later(call: ToolCall) -> str:
            return "x"


# ── Composition: mount ────────────────────────────────────────────────────


async def test_mounted_children_serve_through_an_attached_parent() -> None:
    """The server you run is the one to attach: a child mounted on it serves
    its own tools as before."""
    a = App()
    child = FastMCP("kid")

    @child.tool
    async def wave() -> str:
        return "hi"

    a.mcp.mount(child, namespace="kid")
    async with serve(a.app()) as s:
        rpc = s.rpc()
        names = {t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"]}
        assert {"kid_wave", "join", "subscribe_events"} <= names
        assert texts(await call_tool(rpc, "kid_wave", {})) == ["hi"]
        assert texts(await call_tool(rpc, "join", {"table": "oak"}))[0].endswith(TRAILER)
    assert not a.warnings


async def test_an_attached_server_mounted_elsewhere_refuses_wrapped_tools() -> None:
    a = App()
    parent = FastMCP("lobby", auth=StaticTokenVerifier(tokens=TOKENS))
    parent.mount(a.mcp, namespace="cards")
    app = parent.http_app(path="/mcp", stateless_http=True, json_response=True)
    async with serve(app) as s:
        rpc = s.rpc()
        await rpc("tools/list", {})
        assert len(a.warnings) == 1 and "mounted" in a.warnings[0] and "attach the server you run" in a.warnings[0]
        res = await call_tool(rpc, "cards_join", {"table": "oak"})
        assert res["result"]["isError"] is True and "attach the server you run" in texts(res)[0]
        assert "/event-subscriptions" not in a.core.paths()


# ── Transports and FastMCP's own client ───────────────────────────────────


async def test_fastmcps_own_client_over_http() -> None:
    """FastMCP's Client, over stateful Streamable HTTP (SSE responses), gets
    the block in a wrapped tool's reply."""
    a = App()
    app = a.app(stateless=False, json_response=False)
    async with app.router.lifespan_context(app):
        transport = StreamableHttpTransport(
            "http://127.0.0.1:8000/mcp",
            headers={"Authorization": "Bearer tok-ada"},
            httpx_client_factory=lambda **kw: httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), **kw),
        )
        async with Client(transport) as c:
            out = await c.call_tool("join", {"table": "oak"})
            text = "\n".join(getattr(b, "text", "") for b in out.content)
            assert text.endswith(TRAILER) and WOKEN in text
            out = await c.call_tool("sit", {"table": "oak"})
            assert out.structured_content["wakeshift"] == WOKEN
            assert a.core.bodies("/event-subscriptions")[0]["call"]["user"] == "ada"


async def test_in_memory_client() -> None:
    """FastMCP's in-memory transport (as stdio, no HTTP) runs attach too."""
    a = App(identify=lambda _ctx: Identity(user="local"))
    async with Client(a.mcp) as c:
        out = await c.call_tool("join", {"table": "oak"}, meta={"wakeshift/session": "wsc_s1"})
        names = {t.name for t in await c.list_tools()}
        assert {"join", "subscribe_events"} <= names
    sub = a.core.bodies("/event-subscriptions")[0]["call"]
    assert sub["user"] == "local" and "user_agent" not in sub
    assert out.content[-1].text.endswith(TRAILER)


# ── Which servers attach takes ────────────────────────────────────────────


def test_older_fastmcp_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:

    class FastMCP:  # a FastMCP 3 server, as attach sees it
        pass

    FastMCP.__module__ = "fastmcp.server.server"
    monkeypatch.setattr(sys.modules["fastmcp"], "__version__", "3.4.11")
    with pytest.raises(TypeError, match=r"FastMCP 4 .*3\.4\.11.*mcp 1\.x"):
        host_of(FastMCP())
    monkeypatch.setattr(sys.modules["fastmcp"], "__version__", "5.0.0")
    with pytest.raises(TypeError, match="5.0.0"):
        host_of(FastMCP())


def test_other_servers_are_refused() -> None:
    with pytest.raises(TypeError, match="MCPServer .* or a FastMCP 4 server"):
        host_of(object())
    assert isinstance(host_of(FastMCP("x")), FastMCPHost)
