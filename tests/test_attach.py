"""attach at the protocol layer (design §2.3, §2.4, PR 5b): Wakeshift's
arguments taken out of every tool call before the host or vendor middleware
sees them, wrap on MCPServer, FastMCP and the low-level Server, current_call
from inside a tool's call chain or from its context, one attach per server."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import threading
from typing import Any

import httpx2
import mcp_types as types
import pytest
from mcp.server.lowlevel import Server

# The official SDK's own MCPServer, whichever server the rest of the suite runs on
# (conftest.py): FastMCP's middleware is tested in test_fastmcp.py.
from mcp.server.mcpserver import Context as OfficialContext
from mcp.server.mcpserver import MCPServer as OfficialMCPServer
from pydantic import BaseModel

from wakeshift import ToolCall, UsageError, Wakeshift

from .helpers import FakeCore, Logged, app_key, served

WAKE = {"session_key": "wsc_canary", "harness": "claude-code", "conversation_id": "conv-1"}


class Table(BaseModel):
    table: str


class Msg(BaseModel):
    text: str


def client() -> tuple[Wakeshift, FakeCore]:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (200, {"event_subscription": {"id": "es_1", "user": None, "created": True}, "session": "wsc_s1"}),
    )
    core.on("POST", "/calls/addition", lambda b: (200, {"text": "Wakeshift: woken."}))
    wake = Wakeshift(
        app_key("cards"), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    )
    return wake, core


def recorder(seen: list[Any]) -> Any:
    async def middleware(ctx: Any, call_next: Any) -> Any:
        if ctx.method == "tools/call":
            seen.append(dict((ctx.params or {}).get("arguments") or {}))
        return await call_next(ctx)

    return middleware


async def test_vendor_middleware_never_sees_wakeshifts_arguments() -> None:
    wake, core = client()
    news = wake.define_event("table.news", description="News", args=Table, payload=Msg)
    before: list[Any] = []
    after: list[Any] = []
    server = OfficialMCPServer("cards", middleware=[recorder(before)])
    attached = wake.attach(server, identify=lambda r: None)
    server.middleware.append(recorder(after))

    @attached.tool(subscribes=[news])
    async def watch(table: str, call: ToolCall) -> str:
        await call.subscribe(news, {"table": table})
        return "watching"

    @server.tool()
    async def plain(table: str) -> str:
        return "plain"

    async with served(server) as (_, rpc):
        for name in ("watch", "plain"):
            r = await rpc("tools/call", {"name": name, "arguments": {"table": "oak", **WAKE}})
            assert not r["result"].get("isError"), r
    # Middleware added before attach and after it alike, for a tool that
    # subscribes and one that doesn't.
    assert before == after == [{"table": "oak"}, {"table": "oak"}]
    sub = core.bodies("/event-subscriptions")[0]["call"]
    assert sub["session"] == "wsc_canary" and sub["harness"] == "claude-code" and sub["conversation"] == "conv-1"


async def test_a_tool_that_names_a_wake_argument_itself_keeps_it() -> None:
    wake, _ = client()
    wake.define_event("table.news", description="News", args=Table, payload=Msg)
    server = OfficialMCPServer("cards")
    wake.attach(server)
    got: list[Any] = []

    @server.tool()
    async def legacy(harness: str) -> str:
        got.append(harness)
        return "ok"

    async with served(server) as (_, rpc):
        await rpc("tools/call", {"name": "legacy", "arguments": {"harness": "mine", "session_key": "wsc_x"}})
    assert got == ["mine"]


async def test_the_low_level_handler_of_a_tool_that_doesnt_subscribe_never_sees_them() -> None:
    wake, _ = client()
    wake.define_event("table.news", description="News", args=Table, payload=Msg)
    seen: list[Any] = []

    async def list_tools(ctx: Any, params: Any) -> types.ListToolsResult:
        return types.ListToolsResult(tools=[types.Tool(name="plain", input_schema={"type": "object"})])

    async def call_tool(ctx: Any, params: types.CallToolRequestParams) -> types.CallToolResult:
        seen.append(dict(params.arguments or {}))
        return types.CallToolResult(content=[types.TextContent(type="text", text="ok")])

    server = Server("cards", on_list_tools=list_tools, on_call_tool=call_tool)
    wake.attach(server)
    async with served(server) as (_, rpc):
        await rpc("tools/call", {"name": "plain", "arguments": {"x": 1, **WAKE}})
    assert seen == [{"x": 1}]


async def test_current_call_from_a_context_that_lost_the_async_context() -> None:
    wake, core = client()
    news = wake.define_event("table.news", description="News", args=Table, payload=Msg)
    server = OfficialMCPServer("cards")
    wake.attach(server, wrap={"watch": [news]})

    @server.tool(structured_output=False)
    async def watch(table: str, ctx: OfficialContext) -> str:
        async def elsewhere() -> str:
            with pytest.raises(UsageError):
                wake.current_call()  # no async context here
            sub = await wake.current_call(ctx).subscribe(news, {"table": table})
            return sub.id

        # A task run in an empty context, as a vendor's worker pool might.
        return await asyncio.get_running_loop().create_task(elsewhere(), context=contextvars.Context())

    async with served(server) as (_, rpc):
        r = await rpc("tools/call", {"name": "watch", "arguments": {"table": "oak"}}, headers={"User-Agent": "claude-code/2"})
    assert r["result"]["content"][0]["text"].startswith("es_1"), r
    assert core.bodies("/event-subscriptions")[0]["call"]["tool"] == "watch"


async def test_a_sync_tool_under_wrap_finds_its_call() -> None:
    wake, _ = client()
    news = wake.define_event("table.news", description="News", args=Table, payload=Msg)
    server = OfficialMCPServer("cards")
    wake.attach(server, wrap={"peek": [news]})

    @server.tool(structured_output=False)
    def peek() -> str:
        return wake.current_call().tool

    async with served(server) as (_, rpc):
        r = await rpc("tools/call", {"name": "peek", "arguments": {}})
    assert r["result"]["content"][0]["text"].startswith("peek")


async def test_a_sync_tool_runs_in_a_worker_thread_with_its_call() -> None:
    wake, _ = client()
    wake.define_event("table.news", description="News", args=Table, payload=Msg)
    server = OfficialMCPServer("cards")
    attached = wake.attach(server)
    loop_thread = threading.get_ident()
    seen: list[Any] = []

    @attached.tool(structured_output=False)
    def peek(table: str, call: ToolCall) -> str:
        seen.append((threading.get_ident() != loop_thread, wake.current_call() is call))
        return f"{call.tool} {table}"

    async with served(server) as (_, rpc):
        tools = await rpc("tools/list")
        r = await rpc("tools/call", {"name": "peek", "arguments": {"table": "oak", **WAKE}})
    schema = next(t for t in tools["result"]["tools"] if t["name"] == "peek")["inputSchema"]
    assert "table" in schema["properties"] and "call" not in schema["properties"]
    assert r["result"]["content"][0]["text"].startswith("peek oak"), r
    assert seen == [(True, True)]


def test_a_sync_tool_that_subscribes_is_refused() -> None:
    wake, _ = client()
    news = wake.define_event("table.news", description="News", args=Table, payload=Msg)
    attached = wake.attach(OfficialMCPServer("cards"))
    with pytest.raises(TypeError, match="async def"):

        @attached.tool(subscribes=[news])
        def watch(table: str, call: ToolCall) -> str:
            return "no"


async def test_a_sync_tool_without_a_key_runs_as_written() -> None:
    wake = Wakeshift(None)
    server = OfficialMCPServer("cards")
    attached = wake.attach(server)

    @attached.tool(structured_output=False)
    def peek(table: str, call: ToolCall) -> str:
        return f"{call.tool or 'off'} {table}"

    async with served(server) as (_, rpc):
        r = await rpc("tools/call", {"name": "peek", "arguments": {"table": "oak"}})
    assert r["result"]["content"][0]["text"] == "peek oak", r


async def test_subscribing_off_the_loop_is_no_call() -> None:
    wake, core = client()
    news = wake.define_event("table.news", description="News", args=Table, payload=Msg)
    server = OfficialMCPServer("cards")
    wake.attach(server, wrap={"watch": [news]})
    codes: list[str] = []

    @server.tool(structured_output=False)
    def watch(table: str) -> str:
        call = wake.current_call()
        try:
            asyncio.run(call.subscribe(news, {"table": table}))
        except UsageError as err:
            codes.append(err.code)
        return "ok"

    async with served(server) as (_, rpc):
        await rpc("tools/call", {"name": "watch", "arguments": {"table": "oak", **WAKE}})
    assert codes == ["no_call"]
    assert core.bodies("/event-subscriptions") == []


@pytest.mark.parametrize("kind", ["mcpserver", "lowlevel"])
async def test_wrap_as_a_function_names_tools_at_run_time(kind: str) -> None:
    wake, core = client()
    news = wake.define_event("table.news", description="News at a table", args=Table, payload=Msg, public=True)
    asked: list[str] = []

    def wrap(tool: str) -> list[Any] | None:
        asked.append(tool)
        return [news] if tool.startswith("watch_") else None

    async def watch(table: str) -> str:
        sub = await wake.current_call().subscribe(news, {"table": table})
        return f"watching {sub.id}"

    if kind == "mcpserver":
        server: Any = OfficialMCPServer("cards")
        wake.attach(server, wrap=wrap)
        # Added after attach, named at run time.
        server.add_tool(watch, name="watch_oak", description="Watch a table.", structured_output=False)

        @server.tool(structured_output=False)
        async def peek(table: str) -> str:
            return f"peek {table}"

    else:
        listed = [
            types.Tool(
                name=n,
                description=d,
                input_schema={"type": "object", "properties": {"table": {"type": "string"}}, "required": ["table"]},
            )
            for n, d in (("watch_oak", "Watch a table."), ("peek", None))
        ]

        async def list_tools(ctx: Any, params: Any) -> types.ListToolsResult:
            return types.ListToolsResult(tools=listed)

        async def call_tool(ctx: Any, params: Any) -> types.CallToolResult:
            args = params.arguments or {}
            text = await watch(args["table"]) if params.name == "watch_oak" else f"peek {args['table']}"
            return types.CallToolResult(content=[types.TextContent(type="text", text=text)])

        server = Server("cards", on_list_tools=list_tools, on_call_tool=call_tool)
        wake.attach(server, wrap=wrap)

    async with served(server) as (_, rpc):
        tools = {t["name"]: t for t in (await rpc("tools/list"))["result"]["tools"]}
        watched = await rpc(
            "tools/call", {"name": "watch_oak", "arguments": {"table": "oak", **WAKE}}, headers={"User-Agent": "claude-code/2"}
        )
        peeked = await rpc("tools/call", {"name": "peek", "arguments": {"table": "oak", **WAKE}})
    assert set(WAKE) <= set(tools["watch_oak"]["inputSchema"]["properties"])
    assert "News at a table" in tools["watch_oak"]["description"]
    assert set(tools["peek"]["inputSchema"]["properties"]) == {"table"}
    assert not tools["peek"].get("description")
    assert "Wakeshift: woken." in watched["result"]["content"][-1]["text"], watched
    assert core.bodies("/event-subscriptions")[0]["call"]["tool"] == "watch_oak"
    assert peeked["result"]["content"][0]["text"] == "peek oak"
    assert {"watch_oak", "peek"} <= set(asked)
    assert not set(asked) & {"subscribe_events", "unsubscribe_events", "list_subscriptions", "wakeshift_wait"}


async def test_wrap_as_a_function_returning_bad_events_serves_the_tool_unwrapped() -> None:
    wake, core = client()
    other, _ = client()
    stray = other.define_event("table.news", description="News", args=Table, payload=Msg)
    answers: dict[str, Any] = {"watch_a": [stray], "watch_b": ["table.news"], "watch_c": "table.news"}
    server = OfficialMCPServer("cards")
    wake.attach(server, wrap=lambda tool: answers.get(tool))
    logged = Logged(logging.ERROR)
    ran: list[str] = []

    for n in answers:

        async def tool(table: str, n: str = n) -> str:
            ran.append(n)
            return f"{n} {table}"

        server.add_tool(tool, name=n, structured_output=False)

    async with served(server) as (_, rpc):
        for _ in range(2):
            tools = {t["name"]: t for t in (await rpc("tools/list"))["result"]["tools"]}
        calls = [(await rpc("tools/call", {"name": n, "arguments": {"table": "oak"}}))["result"] for n in answers]
    for n in answers:
        assert "session_key" not in tools[n]["inputSchema"]["properties"]
    assert [c["content"][0]["text"] for c in calls] == ["watch_a oak", "watch_b oak", "watch_c oak"]
    assert all(len(c["content"]) == 1 for c in calls) and ran == list(answers)
    assert core.bodies("/event-subscriptions") == []
    # Once per tool, at error, with the tool.
    assert logged.codes() == ["wrap_bad_event"] * 3
    assert [r.wakeshift_tool for r in logged.records] == list(answers)  # type: ignore[attr-defined]


async def test_current_call_in_a_tool_wrap_doesnt_name_is_no_call() -> None:
    wake, _ = client()
    wake.define_event("table.news", description="News", args=Table, payload=Msg)
    server = OfficialMCPServer("cards")
    wake.attach(server)
    codes: list[str] = []

    @server.tool()
    async def plain() -> str:
        try:
            wake.current_call()
        except UsageError as err:
            codes.append(err.code)
        return "ok"

    async with served(server) as (_, rpc):
        await rpc("tools/call", {"name": "plain", "arguments": {}})
    assert codes == ["no_call"]


def test_one_attach_per_server() -> None:
    wake, _ = client()
    server = OfficialMCPServer("cards")
    wake.attach(server)
    with pytest.raises(UsageError, match="attach") as e:
        wake.attach(server)
    assert e.value.code == "config"
    other, _ = client()
    with pytest.raises(UsageError, match="attach"):
        other.attach(server)


def test_wrap_takes_this_apps_events_by_tool_name() -> None:
    wake, _ = client()
    news = wake.define_event("table.news", description="News", args=Table, payload=Msg)
    with pytest.raises(UsageError, match="wrap") as e:
        wake.attach(OfficialMCPServer("a"), wrap={"start_crawl": ["table.news"]})  # type: ignore[list-item]
    assert e.value.code == "config"
    with pytest.raises(UsageError, match="wrap"):
        wake.attach(OfficialMCPServer("b"), wrap={"start_crawl": news})  # type: ignore[dict-item]
    with pytest.raises(UsageError, match="wrap"):
        wake.attach(OfficialMCPServer("c"), wrap={"subscribe_events": [news]})
