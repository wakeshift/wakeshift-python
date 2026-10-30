"""The README's examples run: each python block, in order, in one namespace."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import httpx
import pytest

from .helpers import FakeCore, app_key, served

README = Path(__file__).resolve().parents[1] / "README.md"
WAKE_ARGS = {"session_key", "harness", "conversation_id"}


async def test_the_readmes_examples_run(monkeypatch: pytest.MonkeyPatch) -> None:
    blocks = re.findall(r"```python\n(.*?)```", README.read_text(), re.S)
    assert len(blocks) == 2
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("chat"))
    monkeypatch.setenv("WAKESHIFT_CORE", "http://core.test")
    ns: dict[str, Any] = {"__name__": "readme"}
    for b in blocks:
        exec(compile(b, str(README), "exec"), ns)

    server = ns["server"]
    tools = {t.name for t in await server.list_tools()}
    assert tools == {"subscribe_events", "unsubscribe_events", "list_subscriptions", "join", "watch", "sit", "leave"}
    # Every wrapped tool gets Wakeshift's arguments and hides call.
    assert set(server._tool_manager.get_tool("join").parameters["properties"]) == {"table", *WAKE_ARGS}
    assert set(server._tool_manager.get_tool("watch").parameters["properties"]) == {"channel", *WAKE_ARGS}
    assert set(server._tool_manager.get_tool("leave").parameters["properties"]) == {"table", *WAKE_ARGS}
    # sit's output (WakeOutput) carries the block's text too; watch, which
    # returns str, replies with text only (no output schema).
    out_schema = server._tool_manager.get_tool("sit").output_schema
    assert out_schema is not None and "wakeshift" in out_schema["properties"]
    assert server._tool_manager.get_tool("watch").output_schema is None

    # The handlers emit what the README says.
    core = FakeCore(default_ok=True)
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 1}))
    core.on("POST", "/events/status", lambda b: (200, {"subscriptions": 2, "connected": 1, "via": {"stream": 1}}))
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (
            200,
            {"event_subscription": {"id": "es_1", "user": b["call"].get("user"), "created": True}, "session": "wsc_1"},
        ),
    )
    core.on("POST", "/event-subscriptions/unsubscribe", lambda b: (200, {"ended": True}))
    core.on("POST", "/calls/addition", lambda b: (200, {"text": "Wakeshift: woken by Monitor."}))
    ns["wake"]._http = httpx.AsyncClient(transport=httpx.MockTransport(core.handler))
    await ns["on_message"]("eng", "ada", "shipped it")
    await ns["on_turn"]("juniper", "ada", ["As", "Kd"], 2)
    emits = core.bodies("/events/emit")
    assert emits[0]["name"] == "chat.message"
    assert emits[0]["arguments"] == {"channel": "eng"}
    assert emits[0]["text"] == "ada in #eng: shipped it"
    assert emits[1]["to"] == "ada"
    assert emits[1]["text"] == "Your move at juniper: 2 to call"
    # A deal reaches every seated conversation in one emit.
    ns["seats"]["juniper"] = [ns["Recipient"](user="ada", subscription="s1"), ns["Recipient"](user="bo", subscription="s2")]
    await ns["on_deal"]("juniper")
    assert core.bodies("/events/emit")[2]["to"] == [{"user": "ada", "subscription": "s1"}, {"user": "bo", "subscription": "s2"}]
    assert await ns["listening"]("eng") == 1
    assert core.bodies("/events/status")[0] == {"name": "chat.message", "arguments": {"channel": "eng"}}

    # The wrapped tools subscribe and unsubscribe the calling conversation.
    ns["user_of"] = lambda ctx: "cy"
    async with served(server) as (_, rpc):

        async def call(name: str, args: dict[str, Any]) -> dict[str, Any]:
            res = await rpc("tools/call", {"name": name, "arguments": args}, headers={"User-Agent": "claude-code/2.1.280"})
            assert "result" in res and not res["result"].get("isError"), res
            return res["result"]

        watched = await call("watch", {"channel": "eng"})
        assert watched["content"][0]["text"] == "Watching #eng.\n\nWakeshift: woken by Monitor."
        assert "structuredContent" not in watched
        sat = await call("sit", {"table": "oak"})
        assert sat["structuredContent"] == {"table": "oak", "players": 1, "wakeshift": "Wakeshift: woken by Monitor."}
        assert ns["seats"]["oak"] == [ns["Recipient"](user="cy", subscription="es_1")]
        await call("leave", {"table": "oak"})
        assert ns["seats"]["oak"] == []
    subs = core.bodies("/event-subscriptions")
    assert [(s["name"], s["arguments"]) for s in subs] == [("chat.message", {"channel": "eng"}), ("game.turn", {"table": "oak"})]
    assert subs[0]["reason"] == "Watching #eng"
    assert core.bodies("/event-subscriptions/unsubscribe")[0]["arguments"] == {"table": "oak"}
