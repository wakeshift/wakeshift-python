"""The Go, Node and Python SDKs must say the same thing: the event tools'
wording, and what events/list, tools/list and the manifest give for the same
event types, checked against the golden files in sdk/typescript/test (which the
Go SDK writes: WAKESHIFT_UPDATE_GOLDEN=1 go test ./pkg/appkit)."""

from __future__ import annotations

from typing import Any

from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel, Field

from wakeshift import EVENT_TOOL_TEXT, harness_agent

from .helpers import fake_wake, golden_file, served, sorted_json


class ChatArgs(BaseModel):
    channel: str = Field(description="The channel, lowercase")
    sender: str | None = Field(None, alias="from", description="Only messages from this person")


class ChatMessage(BaseModel):
    sender: str = Field(alias="from", description="Who wrote it")
    text: str = Field(description="The message")


class TurnArgs(BaseModel):
    table: str = Field(description="The table you're seated at")


class Turn(BaseModel):
    street: str = Field(description="The betting round")
    hand: list[str] | None = Field(None, description="Your hole cards")


# Defined out of alphabetical order: arguments and required fields are listed
# in name order, as the other SDKs list them.
class RunArgs(BaseModel):
    repo: str = Field(description="The repository")
    pr: int = Field(description="The pull request")
    branch: str | None = Field(None, description="Only runs on this branch")


class Run(BaseModel):
    status: str = Field(description="How it ended")
    branch: str = Field(description="The branch it ran on")


def input_shape(s: dict[str, Any]) -> dict[str, Any]:
    """What the golden file keeps of a tool's input schema: each property's
    type, description and enum, and what's required (the rest is each MCP SDK's
    own rendering; the Go and Node tests do the same)."""
    props = {}
    for k, p in (s.get("properties") or {}).items():
        out = {"type": p.get("type"), "description": p.get("description")}
        if "enum" in p:
            out["enum"] = p["enum"]
        props[k] = out
    shape: dict[str, Any] = {"properties": props}
    if "required" in s:
        shape["required"] = s["required"]
    return shape


def test_event_tool_wording_is_the_other_sdks() -> None:
    assert sorted_json(dict(EVENT_TOOL_TEXT)) == golden_file("event-tools.json")


def test_agent_ids_match_the_core() -> None:
    for name, want in golden_file("agent-ids.json").items():
        assert harness_agent(name) == want, name


async def test_events_list_tools_and_manifest_match_the_other_sdks() -> None:
    wake, _ = fake_wake()
    chat = wake.define_event(
        "chat.message",
        title="Chat message",
        description="A new message in a channel.",
        args=ChatArgs,
        payload=ChatMessage,
        text=lambda m, a: f"{m.sender}: {m.text}",
    )
    turn = wake.define_event(
        "game.turn",
        description="It's your move at a table.",
        args=TurnArgs,
        payload=Turn,
        text=lambda t, a: f"Your move at {a.table}",
        personal=True,
    )
    run = wake.define_event(
        "ci.run",
        description="A CI run on a pull request finished.",
        args=RunArgs,
        payload=Run,
        text=lambda r, a: f"CI {r.status}",
    )
    server = MCPServer("sample")
    wake.attach(server)
    async with served(server) as (_, rpc):
        init = await rpc(
            "initialize",
            {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "claude-code", "version": "1"}},
        )
        assert "error" not in init, init
        events = (await rpc("events/list", {}))["result"]["events"]
        tools = (await rpc("tools/list", {}))["result"]["tools"]
    got = {
        "events": events,
        "manifest": [chat.manifest, turn.manifest, run.manifest],
        "tools": sorted(
            (
                {"name": t["name"], "description": t["description"], "inputSchema": input_shape(t["inputSchema"])}
                | ({"annotations": t["annotations"]} if t.get("annotations") else {})
                for t in tools
            ),
            key=lambda t: t["name"],
        ),
    }
    assert sorted_json(got) == golden_file("events-list.json")
