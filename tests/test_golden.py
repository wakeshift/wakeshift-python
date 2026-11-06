"""The Go, TypeScript and Python SDKs must say the same thing: the built-in
tools' wording, and what events/list, tools/list and the manifest give for
the same event types, checked against the golden files in sdk/typescript/test."""

from __future__ import annotations

from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel, Field

from wakeshift._tasks import TOOL_ANNOTATIONS
from wakeshift._text import TEXT

from .helpers import fake_wake, golden_file, input_shape, served, sorted_json


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


class EnvArgs(BaseModel):
    env: str = Field(description="The environment")


class Deploy(BaseModel):
    status: str = Field(description="How it ended")


def test_event_tool_wording_is_the_other_sdks() -> None:
    want = {k: v for k, v in golden_file("event-tools.json").items() if not k.startswith("_")}
    assert TEXT == want


def test_tool_annotations_are_the_other_sdks() -> None:
    assert sorted_json(TOOL_ANNOTATIONS) == golden_file("tool-annotations.json")


async def test_events_list_tools_and_manifest_match_the_other_sdks() -> None:
    wake, _ = fake_wake()
    chat = wake.define_event(
        "chat.message",
        title="Chat message",
        description="A new message in a channel.",
        args=ChatArgs,
        payload=ChatMessage,
        text=lambda m, a: f"{m.sender}: {m.text}",
        public=True,
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
    deploy = wake.define_event(
        "deploy.finished",
        description="A deploy finished.",
        args=EnvArgs,
        payload=Deploy,
        authorize=lambda s, a: True,
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
    want = golden_file("events-list.json")
    # events/list names only the events a native client may subscribe to; the
    # manifest says who may subscribe to each (C1's subscribe).
    assert events == want["events"]
    assert sorted_json([chat.manifest, turn.manifest, run.manifest, deploy.manifest]) == sorted_json(want["manifest"])
    got = {
        "tools": sorted(
            (
                {"name": t["name"], "description": t["description"], "inputSchema": input_shape(t["inputSchema"])}
                | ({"title": t["title"]} if t.get("title") else {})
                | ({"annotations": t["annotations"]} if t.get("annotations") else {})
                for t in tools
            ),
            key=lambda t: t["name"],
        ),
    }
    assert sorted_json(got["tools"]) == sorted_json(want["tools"])
    # subscribe_events' and unsubscribe_events' arguments: exactly the shared schema.
    for t in tools:
        if t["name"] in ("subscribe_events", "unsubscribe_events"):
            assert t["inputSchema"]["properties"]["arguments"] == want["builtin_arguments_schema"], t["name"]


async def test_events_list_with_subscribe_vouchers_matches_the_other_sdks() -> None:
    """events-list.json vouchers: to a native request while the core reports
    subscribe_vouchers, events/list also lists the tools-only sample (ci.run),
    with a required wakeshift_voucher naming the app."""
    from .helpers import FakeCore

    want = golden_file("events-list.json")
    core = FakeCore(default_ok=True)
    core.on("POST", "/manifest/merge", lambda b: (200, {**b, "features": ["subscribe_vouchers"]}))
    core.on("GET", "/app", lambda b: (200, {"id": "sample", "name": want["vouchers"]["app"]}))
    wake, _ = fake_wake(core)
    wake.define_event(
        "chat.message",
        title="Chat message",
        description="A new message in a channel.",
        args=ChatArgs,
        payload=ChatMessage,
        public=True,
    )
    wake.define_event("game.turn", description="It's your move at a table.", args=TurnArgs, payload=Turn, personal=True)
    wake.define_event("ci.run", description="A CI run on a pull request finished.", args=RunArgs, payload=Run)
    wake.define_event(
        "deploy.finished", description="A deploy finished.", args=EnvArgs, payload=Deploy, authorize=lambda s, a: True
    )
    server = MCPServer("sample")
    wake.attach(server)
    lists = {}
    for client in ("openai-mcp", "claude-code"):
        async with served(server, stateless=False) as (_, rpc):
            await rpc(
                "initialize",
                {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": client, "version": "1"}},
            )
            lists[client] = (await rpc("events/list", {}))["result"]["events"]
    assert sorted_json(lists["openai-mcp"]) == sorted_json(want["vouchers"]["events_native"])
    assert lists["claude-code"] == want["events"]
