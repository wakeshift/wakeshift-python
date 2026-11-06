"""subscribing-tools-list.json: the tools/list a server serves for tools that
subscribe, a tool with subscribes=[], a tool that doesn't subscribe and the
built-ins (design §2.4, §2.12, §3). Every scenario runs through
@attached.tool and through wrap, on MCPServer (FastMCP with
WAKESHIFT_TEST_SERVER=fastmcp), and through wrap on the low-level Server;
each gives the same tools/list."""

from __future__ import annotations

import logging
from typing import Any

import mcp_types as types
import pytest
from mcp.server.lowlevel import Server
from mcp.server.mcpserver import MCPServer

from wakeshift import ToolCall

from .helpers import Logged, served
from .tools_list_fixture import FIXTURE, check, client, define, listed, register, wrap_of


@pytest.mark.parametrize("via_wrap", [False, True], ids=["attached.tool", "wrap"])
@pytest.mark.parametrize("scenario", FIXTURE["scenarios"], ids=lambda s: s["name"])
async def test_tools_list(scenario: dict[str, Any], via_wrap: bool) -> None:
    wake = client(scenario["enabled"])
    events = define(wake, scenario["events"])
    server = MCPServer("crawler")
    attached = wake.attach(
        server,
        identify=lambda r: None,
        builtin_tools=scenario["builtin_tools"],
        wrap=wrap_of(events, scenario["tools"]) if via_wrap else None,
    )
    register(server, attached, events, scenario["tools"], via_wrap=via_wrap)
    check(await listed(server), scenario["tools_list"], scenario["name"])


@pytest.mark.parametrize("scenario", FIXTURE["scenarios"], ids=lambda s: s["name"])
async def test_tools_list_on_a_low_level_server(scenario: dict[str, Any]) -> None:
    wake = client(scenario["enabled"])
    events = define(wake, scenario["events"])
    vt = FIXTURE["vendor_tools"]
    defined = [
        types.Tool(
            name=n,
            description=vt[n]["description"],
            input_schema=vt[n]["inputSchema"],
            output_schema=vt[n].get("outputSchema"),
        )
        for n in scenario["tools"]
    ]

    async def list_tools(ctx: Any, params: Any) -> types.ListToolsResult:
        return types.ListToolsResult(tools=defined)

    server = Server("crawler", on_list_tools=list_tools)
    wake.attach(server, identify=lambda r: None, builtin_tools=scenario["builtin_tools"], wrap=wrap_of(events, scenario["tools"]))
    check(await listed(server), scenario["tools_list"], scenario["name"])


MARKDOWN = """Start a crawl.

  Example:

  ```json
  {"url": "https://example.com",   "depth": 2}
  ```
"""


@pytest.mark.parametrize("via_wrap", [False, True], ids=["attached.tool", "wrap"])
async def test_a_markdown_description_keeps_its_whitespace(via_wrap: bool) -> None:
    """A multi-line description stays as written (only trailing whitespace
    dropped), with a blank line before the subscribes sentence; a one-line
    one gets a space."""
    wake = client(True)
    events = define(wake, ["crawl.finished"])
    crawl = events["crawl.finished"]
    server = MCPServer("crawler")
    attached = wake.attach(server, wrap={"start_crawl": [crawl], "check": [crawl]} if via_wrap else None)
    if via_wrap:
        server.tool(name="start_crawl", description=MARKDOWN, structured_output=False)(_start)
        server.tool(name="check", description="Check a crawl.  ", structured_output=False)(_check)
    else:

        @attached.tool("start_crawl", MARKDOWN, subscribes=[crawl], structured_output=False)
        async def start(url: str, call: ToolCall) -> str:
            return "started"

        @attached.tool("check", "Check a crawl.  ", subscribes=[crawl], structured_output=False)
        async def check(job: str, call: ToolCall) -> str:
            return "running"

    tools = {t["name"]: t["description"] for t in await listed(server)}
    sentence = tools["check"].removeprefix("Check a crawl. ")
    assert sentence.startswith("This tool is how ") and "  " not in tools["check"]
    assert tools["start_crawl"] == MARKDOWN.rstrip() + "\n\n" + sentence


async def _start(url: str) -> str:
    return "started"


async def _check(job: str) -> str:
    return "running"


async def test_a_description_over_2048_characters_with_the_sentence_is_logged() -> None:
    wake = client(True)
    crawl = define(wake, ["crawl.finished"])["crawl.finished"]
    server = MCPServer("crawler")
    attached = wake.attach(server)
    long = "Crawl a site. " + "x" * 1900  # under 2048 alone, over with the sentence

    @attached.tool("start_crawl", long, subscribes=[crawl], structured_output=False)
    async def start(url: str, call: ToolCall) -> str:
        return "started"

    @attached.tool("check", "Check a crawl.", subscribes=[crawl], structured_output=False)
    async def check(job: str, call: ToolCall) -> str:
        return "running"

    @attached.tool("note", "x" * 3000, structured_output=False)  # subscribes to nothing: no sentence
    async def note(job: str, call: ToolCall) -> str:
        return "noted"

    logged = Logged(logging.WARNING)
    async with served(server) as (_, rpc):
        for _ in range(2):
            tools = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
    assert len(tools["start_crawl"]["description"]) > 2048
    assert logged.codes() == ["description_too_long"]
    assert logged.records[0].wakeshift_tool == "start_crawl"  # type: ignore[attr-defined]
    assert "Claude Code keeps only its first 2048 characters" in logged[0]
