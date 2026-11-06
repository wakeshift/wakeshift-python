"""The README's example runs: its one python block, the quickstart, against a
fake core. (The rest of the guide is site/docs/python.md, which test_docs.py runs.)"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import httpx2
import pytest

from .helpers import FakeCore, app_key, served

README = Path(__file__).resolve().parents[1] / "README.md"


async def test_the_readmes_examples_run(monkeypatch: pytest.MonkeyPatch) -> None:
    blocks = re.findall(r"```python\n(.*?)```", README.read_text(), re.S)
    assert len(blocks) == 1
    quickstart = (Path(__file__).parent / "typing" / "quickstart.py").read_text().split('"""', 2)[2]
    assert blocks[0].strip() == quickstart.strip(), (
        "the README's quickstart is tests/typing/quickstart.py, which the typing gate checks"
    )
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("crawler"))
    monkeypatch.setenv("WAKESHIFT_CORE", "https://core.test")
    ns: dict[str, Any] = {"__name__": "readme"}
    for b in blocks:
        exec(compile(b, str(README), "exec"), ns)

    core = FakeCore(default_ok=True)
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 1}))
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (
            200,
            {"event_subscription": {"id": "es_1", "user": b["call"].get("user"), "created": True}, "session": "wsc_1"},
        ),
    )
    core.on("POST", "/calls/addition", lambda b: (200, {"text": "Wakeshift: woken by Monitor."}))
    ns["wake"]._http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))

    # The quickstart: the tool subscribes the calling conversation, once.
    async with served(ns["server"]) as (_, rpc):
        tools = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
        assert set(tools) == {"start_crawl", "unsubscribe_events", "list_subscriptions"}, "crawl.finished is tools-only"
        assert set(tools["start_crawl"]["inputSchema"]["properties"]) == {"url", "session_key", "harness", "conversation_id"}
        res = await rpc(
            "tools/call",
            {"name": "start_crawl", "arguments": {"url": "https://x.example"}},
            headers={"User-Agent": "claude-code/2.1"},
        )
        assert (
            res["result"]["content"][0]["text"].startswith("Crawl ")
            and "Wakeshift: woken by Monitor." in res["result"]["content"][0]["text"]
        )
    sub = core.bodies("/event-subscriptions")[0]
    assert sub["name"] == "crawl.finished" and sub["once"] is True and sub["call"]["tool"] == "start_crawl"
    await ns["on_crawl_finished"]("j1", 3)
    assert core.bodies("/events/emit")[0]["payload"] == {"status": "completed", "pages": 3}
