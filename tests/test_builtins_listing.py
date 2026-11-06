"""attach's own tools go through the app's own tools/list pipeline: a vendor
middleware that amends every listed tool (Magic Hour's securitySchemes for
ChatGPT) amends the built-ins too, on MCPServer, the low-level Server and
FastMCP 4 (test_fastmcp23.py covers FastMCP 2/3)."""

from __future__ import annotations

from typing import Any

import mcp_types as types
import pytest
from mcp.server.lowlevel import Server
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import Identity, ToolCall

from .conftest import FASTMCP
from .helpers import Logged, fake_wake, served

SCHEMES = [{"type": "oauth2", "scopes": ["read"]}]


class Job(BaseModel):
    job: str


def stamp(tools: list[Any]) -> list[Any]:
    out = []
    for t in tools:
        if isinstance(t, dict):
            out.append({**t, "_meta": {**(t.get("_meta") or {}), "securitySchemes": SCHEMES}})
        else:
            out.append(t.model_copy(update={"meta": {**(t.meta or {}), "securitySchemes": SCHEMES}}))
    return out


async def stamping(ctx: Any, call_next: Any) -> Any:
    out = await call_next(ctx)
    if getattr(ctx, "method", "") == "tools/list":
        if isinstance(out, dict):
            return {**out, "tools": stamp(out["tools"])}
        return out.model_copy(update={"tools": stamp(out.tools)})
    return out


def every_tool_stamped(tools: list[dict[str, Any]]) -> None:
    assert {t["name"] for t in tools} >= {"subscribe_events", "unsubscribe_events", "list_subscriptions"}
    for t in tools:
        assert (t.get("_meta") or {}).get("securitySchemes") == SCHEMES, t["name"]


@pytest.mark.skipif(FASTMCP, reason="MCPServer's low-level middleware; FastMCP's on_list_tools is tested below")
@pytest.mark.parametrize("before", [True, False], ids=["middleware before attach", "middleware after attach"])
async def test_mcpserver_middleware_sees_the_builtins(before: bool) -> None:
    wake, _ = fake_wake()
    ev = wake.define_event("crawl.done", description="A crawl finished", args=Job, payload=Job, public=True)
    server = MCPServer("crawler")
    if before:
        server.middleware.append(stamping)
    attached = wake.attach(server, identify=lambda r: Identity(user="u1"))
    if not before:
        server.middleware.append(stamping)

    @attached.tool(subscribes=[ev])
    async def watch(job: str, call: ToolCall) -> str:
        return "ok"

    logged = Logged()
    async with served(server) as (_, rpc):
        tools = (await rpc("tools/list", {}))["result"]["tools"]
    every_tool_stamped(tools)
    assert [n for n in (t["name"] for t in tools) if n == "list_subscriptions"] == ["list_subscriptions"]
    assert logged == []


async def test_lowlevel_server_middleware_sees_the_builtins_and_a_same_named_tool_loses() -> None:
    wake, _ = fake_wake()
    ev = wake.define_event("crawl.done", description="A crawl finished", args=Job, payload=Job, public=True)

    async def list_tools(ctx: Any, params: Any) -> types.ListToolsResult:
        return types.ListToolsResult(
            tools=[
                types.Tool(name="start", input_schema={"type": "object"}),
                types.Tool(name="list_subscriptions", description="mine", input_schema={"type": "object"}),
            ]
        )

    server = Server("crawler", on_list_tools=list_tools)
    wake.attach(server, identify=lambda r: Identity(user="u1"), wrap={"start": [ev]})
    server.middleware.append(stamping)
    logged = Logged()
    async with served(server) as (_, rpc):
        tools = (await rpc("tools/list", {}))["result"]["tools"]
    every_tool_stamped(tools)
    [lst] = [t for t in tools if t["name"] == "list_subscriptions"]
    assert lst["description"] != "mine"
    assert logged.codes() == ["builtin_name_conflict"]


async def test_fastmcp4_on_list_tools_sees_the_builtins() -> None:
    fastmcp = pytest.importorskip("fastmcp")
    from fastmcp.server.middleware import Middleware

    class Stamp(Middleware):
        async def on_list_tools(self, context: Any, call_next: Any) -> Any:
            return stamp(list(await call_next(context)))

    wake, core = fake_wake()
    ev = wake.define_event("crawl.done", description="A crawl finished", args=Job, payload=Job, public=True)
    mcp = fastmcp.FastMCP("crawler")
    attached = wake.attach(mcp, identify=lambda r: Identity(user="u1"))
    mcp.add_middleware(Stamp())

    @attached.tool(subscribes=[ev])
    async def watch(job: str, call: ToolCall) -> str:
        return "ok"

    app = mcp.http_app(path="/mcp", stateless_http=True, json_response=True)
    import httpx2

    from .helpers import Rpc

    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000") as c:
            rpc = Rpc(c, "/mcp")
            tools = (await rpc("tools/list", {}))["result"]["tools"]
            # A built-in in FastMCP's registry is still answered by attach.
            res = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {}})
    every_tool_stamped(tools)
    assert not res["result"].get("isError"), res
    assert core.bodies("/event-subscriptions/list") == [{"user": "u1"}]
