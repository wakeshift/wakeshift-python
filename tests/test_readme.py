"""The README's examples run: each python block, in order, in one namespace."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import httpx
import pytest

from .helpers import FakeCore, app_key

README = Path(__file__).resolve().parents[1] / "README.md"


async def test_the_readmes_examples_run(monkeypatch: pytest.MonkeyPatch) -> None:
    blocks = re.findall(r"```python\n(.*?)```", README.read_text(), re.S)
    assert len(blocks) == 2
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("chat"))
    monkeypatch.setenv("WAKESHIFT_CORE", "http://core.test")
    ns: dict[str, Any] = {"__name__": "readme"}
    for b in blocks:
        exec(compile(b, str(README), "exec"), ns)

    tools = {t.name for t in await ns["server"].list_tools()}
    assert tools == {"subscribe_events", "unsubscribe_events", "list_subscriptions", "watch_ci"}
    assert {"session_key", "harness", "conversation_id"} <= set(
        ns["server"]._tool_manager.get_tool("watch_ci").parameters["properties"]
    )

    # The handlers emit and publish what the README says.
    core = FakeCore(default_ok=True)
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 1}))
    core.on("POST", "/topics/publish", lambda b: (200, {"matched": 1}))
    ns["wake"]._http = httpx.AsyncClient(transport=httpx.MockTransport(core.handler))
    await ns["on_message"]("eng", "ada", "shipped it")
    await ns["on_turn"]("juniper", "ada", ["As", "Kd"], 2)
    await ns["on_ci"]("Acme", "failure", "https://ci.example/1")
    emits = core.bodies("/events/emit")
    assert emits[0]["text"] == "ada in #eng: shipped it"
    assert emits[1]["to"] == "ada"
    assert emits[1]["text"] == "Your move at juniper: 2 to call"
    assert core.bodies("/topics/publish")[0]["topics"] == ["ci/acme"]
