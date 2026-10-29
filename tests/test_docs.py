"""The site's Python examples run: in Wakeshift's own repository, site/docs/python.md's
python blocks run in order, in one namespace, against a fake core; every other python
block in site/docs at least compiles. The README's are tested in test_readme.py. The
published copy of the SDK has no site, so this skips there."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import httpx
import pytest

from .helpers import FakeCore, app_key, served

DOCS = Path(__file__).resolve().parents[3] / "site" / "docs"
BLOCK = re.compile(r"^```python[^\n]*\n(.*?)^```", re.S | re.M)
pytestmark = pytest.mark.skipif(not DOCS.is_dir(), reason="no site docs here")


def blocks(page: Path) -> list[str]:
    return BLOCK.findall(page.read_text())


def test_every_python_block_compiles() -> None:
    pages = sorted(DOCS.glob("*.md"))
    found = 0
    for page in pages:
        for b in blocks(page):
            compile(b, str(page), "exec")
            found += 1
    assert found >= 2


async def test_the_python_pages_examples_run(monkeypatch: pytest.MonkeyPatch) -> None:
    page = DOCS / "python.md"
    code = blocks(page)
    assert code, "python.md has no python blocks"
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("chat"))
    monkeypatch.setenv("WAKESHIFT_CORE", "http://core.test")
    # What the examples take from around them: your own sign-in.
    ns: dict[str, Any] = {"__name__": "docs", "user_of": lambda ctx: "ada"}
    for b in code:
        exec(compile(b, str(page), "exec"), ns)

    server = ns["server"]
    tools = {t.name for t in await server.list_tools()}
    assert {"subscribe_events", "unsubscribe_events", "list_subscriptions", "join"} <= tools

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
    ns["wake"]._http = httpx.AsyncClient(transport=httpx.MockTransport(core.handler))

    await ns["on_message"]("eng", "ada", "shipped it")
    assert core.bodies("/events/emit")[0]["arguments"] == {"channel": "eng"}
    async with served(server) as (_, rpc):
        res = await rpc(
            "tools/call", {"name": "join", "arguments": {"channel": "eng"}}, headers={"User-Agent": "claude-code/2.1.280"}
        )
        assert "result" in res and not res["result"].get("isError"), res
    assert core.bodies("/event-subscriptions")[0]["arguments"] == {"channel": "eng"}
