"""attach on the official SDK's low-level Server (mcp.server.lowlevel): its own
tools served by wrapping the app's tools/list and tools/call handlers, and an
on_call_tool handler subscribing with attached.call(ctx)."""

from __future__ import annotations

from typing import Any

import httpx2
import mcp_types as types
import pytest
from mcp.server.lowlevel import Server
from mcp.server.mcpserver import Context as MCPContext
from mcp.server.mcpserver import MCPServer as OfficialMCPServer
from pydantic import BaseModel

from wakeshift import Identity, ToolCall, Wakeshift, wrapped_tool
from wakeshift._hosts import LowLevelHost, host_of
from wakeshift.spec import EXTENSION_KEY
from wakeshift.tasks import TASKS_EXTENSION, WAIT_TOOL

from .helpers import app_key, served
from .test_toolcall import TRAILER, WOKEN, WakeCore

UA = {"User-Agent": "claude-code/2.1.314"}


class TableArgs(BaseModel):
    table: str


class TurnMsg(BaseModel):
    text: str


SEAT_SCHEMA = {"type": "object", "properties": {"table": {"type": "string"}, "seat": {"type": "integer"}}, "required": ["table"]}


class App:
    """A low-level Server whose handlers are written as the MCP SDK's docs
    write them, attached; identify reads a header."""

    def __init__(self, *, handlers: bool = True, tools: Any = None) -> None:
        self.core = WakeCore()
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(self.core.handler))
        self.warnings: list[str] = []
        self.wake = Wakeshift(app_key("cards"), core="http://core.test", http=http, on_warning=self.warnings.append)
        self.turn = self.wake.define_event(
            "game.turn", description="Your turn.", args=TableArgs, payload=TurnMsg, text=lambda m, a: m.text, personal=True
        )
        self.calls: list[ToolCall] = []
        self.listed = [
            wrapped_tool(
                types.Tool(
                    name="join",
                    description="Take a seat",
                    input_schema={"type": "object", "properties": {"table": {"type": "string"}}, "required": ["table"]},
                )
            ),
            wrapped_tool(types.Tool(name="sit", input_schema={"type": "object"}, output_schema=SEAT_SCHEMA)),
            types.Tool(name="plain", input_schema={"type": "object"}),
        ]
        outer = self

        async def list_tools(ctx: Any, params: Any) -> types.ListToolsResult:
            if params is not None and params.cursor == "p2":
                return types.ListToolsResult(tools=[types.Tool(name="later", input_schema={"type": "object"})])
            return types.ListToolsResult(tools=outer.listed, next_cursor="p2")

        async def call_tool(ctx: Any, params: types.CallToolRequestParams) -> types.CallToolResult:
            args = params.arguments or {}
            table = str(args.get("table", "oak"))
            text = lambda t: types.CallToolResult(content=[types.TextContent(type="text", text=t)])  # noqa: E731
            if params.name == "plain":
                return text("plain")
            call = outer.attached.call(ctx)
            assert outer.attached.call(ctx) is call  # the same call, asked twice
            outer.calls.append(call)
            await call.subscribe(outer.turn, {"table": table})
            if params.name == "join":
                return text(f"Seated at {table}.")
            if params.name == "sit":
                return types.CallToolResult(
                    content=[types.TextContent(type="text", text="3")], structured_content={"table": table, "seat": 3}
                )
            if params.name == "boom":
                raise ValueError("the table is full")
            if params.name == "refused":
                return types.CallToolResult(content=[types.TextContent(type="text", text="no")], is_error=True)
            raise ValueError(f"unknown tool {params.name}")

        kw: dict[str, Any] = {"on_list_tools": list_tools, "on_call_tool": call_tool} if handlers else {}
        self.server = Server("cards", version="1.0.0", **kw)
        self.attached = self.wake.attach(
            self.server,
            identify=lambda ctx: Identity(user=ctx.request.headers.get("x-user") if ctx.request is not None else None),
            tools=tools,
        )


async def call_tool(rpc: Any, name: str, args: dict[str, Any], user: str = "ada", **extra: Any) -> dict[str, Any]:
    return await rpc("tools/call", {"name": name, "arguments": args, **extra}, headers={**UA, "X-User": user})


def texts(res: dict[str, Any]) -> list[str]:
    assert "result" in res, res
    return [c.get("text", "") for c in res["result"]["content"]]


async def test_tools_list_has_the_apps_tools_and_attachs() -> None:
    a = App()
    async with served(a.server) as (_, rpc):
        res = (await rpc("tools/list", {}))["result"]
        tools = {t["name"]: t for t in res["tools"]}
        assert list(tools) == ["join", "sit", "plain", "subscribe_events", "unsubscribe_events", "list_subscriptions"]
        assert res["nextCursor"] == "p2"
        assert set(tools["join"]["inputSchema"]["properties"]) == {"table", "session_key", "harness", "conversation_id"}
        assert tools["join"]["inputSchema"]["required"] == ["table"]
        assert "session_key" not in tools["plain"]["inputSchema"].get("properties", {})
        assert "wakeshift" in tools["sit"]["outputSchema"]["properties"]
        assert tools["subscribe_events"]["inputSchema"]["properties"]["event"]["enum"] == ["game.turn"]
        assert tools["list_subscriptions"]["annotations"]["readOnlyHint"] is True
        # attach's tools are on the first page only.
        later = (await rpc("tools/list", {"cursor": "p2"}))["result"]["tools"]
        assert [t["name"] for t in later] == ["later"]


async def test_attached_call_subscribes_and_gets_the_block() -> None:
    a = App()
    async with served(a.server) as (_, rpc):
        res = await call_tool(rpc, "join", {"table": "oak", "session_key": "wsc_s1"})
        assert texts(res) == ["Seated at oak.", WOKEN, TRAILER]
        sub = a.core.bodies("/event-subscriptions")[0]
        assert sub["arguments"] == {"table": "oak"}
        assert sub["call"]["user"] == "ada" and sub["call"]["tool"] == "join" and sub["call"]["session"] == "wsc_s1"
        assert a.calls[-1].user == "ada" and a.calls[-1].tool == "join"
        # Structured output carries the block in wakeshift too.
        r = (await call_tool(rpc, "sit", {"table": "elm"}, user="bo"))["result"]
        assert r["structuredContent"] == {"table": "elm", "seat": 3, "wakeshift": WOKEN}
        assert a.core.bodies("/event-subscriptions")[1]["call"]["user"] == "bo"
        # A tool that doesn't call attached.call gets nothing.
        a.core.reset()
        assert texts(await call_tool(rpc, "plain", {})) == ["plain"]
        assert a.core.paths() == []
    assert not a.warnings


async def test_a_failed_call_rolls_back() -> None:
    a = App()
    async with served(a.server) as (_, rpc):
        for tool in ("boom", "refused"):
            a.core.reset()
            res = await call_tool(rpc, tool, {"table": f"t-{tool}"})
            failed = "error" in res or res["result"].get("isError")
            assert failed, (tool, res)
            ends = a.core.bodies("/event-subscriptions/unsubscribe")
            assert len(ends) == 1 and ends[0]["reason"] == "rolled_back", tool
            assert "/calls/addition" not in a.core.paths(), tool


async def test_attachs_own_tools_and_native_events() -> None:
    a = App()
    a.core.native = []
    async with served(a.server) as (_, rpc):
        res = await call_tool(rpc, "subscribe_events", {"event": "game.turn", "arguments": {"table": "oak"}, "extra": 1})
        assert texts(res)[0].startswith('Subscribed to game.turn {"table":"oak"}'), res
        assert a.core.bodies("/event-subscriptions")[0]["call"]["tool"] == "subscribe_events"
        assert "no subscriptions" in texts(await call_tool(rpc, "list_subscriptions", {}))[0]
        listed = (await rpc("events/list", {}))["result"]
        assert [e["name"] for e in listed["events"]] == ["game.turn"]


async def test_a_server_without_tool_handlers_gets_attachs_tools() -> None:
    a = App(handlers=False)
    async with served(a.server) as (_, rpc):
        init = await rpc(
            "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "c", "version": "1"}}
        )
        caps = init["result"]["capabilities"]
        assert "tools" in caps and caps["events"] == {"listChanged": False}
        assert caps["extensions"][EXTENSION_KEY] == {"listChanged": False}
        names = [t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"]]
        assert names == ["subscribe_events", "unsubscribe_events", "list_subscriptions"]
        res = await call_tool(rpc, "nope", {})
        assert res["error"]["message"] == "Unknown tool: nope"


async def test_no_own_tools_leaves_the_handlers_alone() -> None:
    a = App(handlers=False, tools=[])
    async with served(a.server) as (_, rpc):
        res = await rpc("tools/list", {})
        assert "error" in res  # no tools/list handler, as without attach


async def test_protocol_2026_07_28() -> None:
    a = App()
    meta = {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "claude-code", "version": "2.1.314"},
        "io.modelcontextprotocol/clientCapabilities": {"extensions": {TASKS_EXTENSION: {}}},
    }
    h = {"MCP-Protocol-Version": "2026-07-28"}
    async with served(a.server) as (_, rpc):
        d = (await rpc("server/discover", {"_meta": meta}, headers={**h, "Mcp-Method": "server/discover"}))["result"]
        assert d["capabilities"]["events"] == {"listChanged": False}
        assert set(d["capabilities"]["extensions"]) >= {EXTENSION_KEY, TASKS_EXTENSION}
        lr = (await rpc("tools/list", {"_meta": meta}, headers={**h, "Mcp-Method": "tools/list"}))["result"]
        assert lr["resultType"] == "complete" and lr["cacheScope"] == "private"
        assert WAIT_TOOL in [t["name"] for t in lr["tools"]]
        res = await rpc(
            "tools/call",
            {"name": "join", "arguments": {"table": "oak"}, "_meta": meta},
            headers={**h, **UA, "X-User": "ada", "Mcp-Method": "tools/call", "Mcp-Name": "join"},
        )
        assert res["result"]["resultType"] == "complete" and texts(res)[0] == "Seated at oak."
        assert a.core.bodies("/event-subscriptions")[0]["call"]["tasks"] is True


async def test_attached_call_outside_a_tools_call_is_refused() -> None:
    a = App()
    with pytest.raises(RuntimeError, match="only in a tools/call"):
        a.attached.call(None)
    assert a.warnings and "on_call_tool handler" in a.warnings[0]


def test_attached_tool_is_refused_on_a_low_level_server() -> None:
    a = App()
    with pytest.raises(TypeError, match="wrapped_tool"):

        @a.attached.tool()
        async def join(call: ToolCall) -> str:
            return ""


def test_wrapped_tool_in_wire_form() -> None:
    t = wrapped_tool(
        {
            "name": "x",
            "inputSchema": {"type": "object", "properties": {"session_key": {"type": "string", "description": "mine"}}},
            "outputSchema": SEAT_SCHEMA,
        }
    )
    assert t["inputSchema"]["properties"]["session_key"] == {"type": "string", "description": "mine"}
    assert set(t["inputSchema"]["properties"]) == {"session_key", "harness", "conversation_id"}
    assert "wakeshift" in t["outputSchema"]["properties"] and "wakeshift" not in SEAT_SCHEMA["properties"]
    assert wrapped_tool({"name": "y"})["inputSchema"]["type"] == "object"


def test_fastmcps_low_level_server_is_refused() -> None:
    assert isinstance(host_of(Server("x")), LowLevelHost)
    fastmcp = pytest.importorskip("fastmcp")
    with pytest.raises(TypeError, match="FastMCP server itself"):
        host_of(fastmcp.FastMCP("x")._mcp_server)


async def test_attached_call_from_a_plain_mcpserver_tool() -> None:
    """attached.call works in a tool attach didn't register on MCPServer too."""
    core = WakeCore()
    wake = Wakeshift(
        app_key("cards"), core="http://core.test", http=httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    )
    turn = wake.define_event("game.turn", description="Your turn.", args=TableArgs, payload=TurnMsg, text=lambda m, a: m.text)
    server = OfficialMCPServer("cards")
    attached = wake.attach(server)

    @server.tool(structured_output=False)
    async def join(table: str, ctx: MCPContext) -> str:
        await attached.call(ctx).subscribe(turn, {"table": table})
        return f"Seated at {table}."

    async with served(server) as (_, rpc):
        res = await rpc("tools/call", {"name": "join", "arguments": {"table": "oak"}}, headers=UA)
        assert texts(res)[-1].endswith(TRAILER)
    assert core.bodies("/event-subscriptions")[0]["call"]["tool"] == "join"
