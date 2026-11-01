"""The site's Python examples run: in Wakeshift's own repository, site/docs/python.md's
python blocks run in order, in one namespace, against a fake core (its FastMCP block in
a namespace of its own); every other python block in site/docs at least compiles. The README's are tested in test_readme.py. The
published copy of the SDK has no site, so this skips there."""

from __future__ import annotations

import asyncio
import html
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx2
import pytest

from .helpers import FakeCore, Rpc, app_key, initialize, served

DOCS = Path(__file__).resolve().parents[3] / "site" / "docs"
BLOCK = re.compile(r"^```python([^\n]*)\n(.*?)^```", re.S | re.M)
pytestmark = pytest.mark.skipif(not DOCS.is_dir(), reason="no site docs here")


#: The python.md blocks that run in a namespace of their own, by the file
#: their info string names.
OWN = ("fastmcp_server.py", "lowlevel_server.py")


def blocks(page: Path, name: str | None = None) -> list[str]:
    """page's python blocks: all of them; those whose info string names
    name; or, with name "", those that run together (not OWN)."""
    out = []
    for info, code in BLOCK.findall(page.read_text()):
        own = next((n for n in OWN if n in info), "")
        if name is None or own == name or (name and name in info):
            out.append(code)
    return out


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
    code = blocks(page, "")
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
    ns["wake"]._http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))

    await ns["on_message"]("eng", "ada", "shipped it")
    assert core.bodies("/events/emit")[0]["arguments"] == {"channel": "eng"}
    async with served(server) as (_, rpc):
        res = await rpc(
            "tools/call", {"name": "join", "arguments": {"channel": "eng"}}, headers={"User-Agent": "claude-code/2.1.280"}
        )
        assert "result" in res and not res["result"].get("isError"), res
    assert core.bodies("/event-subscriptions")[0]["arguments"] == {"channel": "eng"}


async def test_the_python_pages_fastmcp_example_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("fastmcp")
    from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

    page = DOCS / "python.md"
    code = blocks(page, "fastmcp_server.py")
    assert len(code) == 1, "python.md has one FastMCP block"
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("ci"))
    monkeypatch.setenv("WAKESHIFT_CORE", "http://core.test")
    # What the example takes from around it: your FastMCP auth.
    verifier = StaticTokenVerifier(tokens={"tok": {"client_id": "c", "scopes": [], "sub": "ada"}})
    ns: dict[str, Any] = {"__name__": "docs_fastmcp", "verifier": verifier}
    exec(compile(code[0], str(page), "exec"), ns)

    core = FakeCore(default_ok=True)
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
    assert ns["app"] is not None
    # The example's server, answering in JSON rather than SSE for the test's client.
    app = ns["mcp"].http_app(path="/mcp", json_response=True)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000") as client:
            rpc = Rpc(client, "/mcp", headers={"Authorization": "Bearer tok", "User-Agent": "claude-code/2.1.314"})
            await initialize(rpc, "claude-code")
            listed = {t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"]}
            assert {"subscribe_events", "unsubscribe_events", "list_subscriptions", "watch"} <= listed
            res = await rpc("tools/call", {"name": "watch", "arguments": {"repo": "acme"}})
            assert res["result"]["content"][-1]["text"] == "Watching acme.\n\nWakeshift: woken by Monitor.", res
    sub = core.bodies("/event-subscriptions")[0]
    assert sub["arguments"] == {"repo": "acme"} and sub["call"]["user"] == "ada"


async def test_the_python_pages_low_level_example_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    page = DOCS / "python.md"
    code = blocks(page, "lowlevel_server.py")
    assert len(code) == 1, "python.md has one low-level Server block"
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("rooms"))
    monkeypatch.setenv("WAKESHIFT_CORE", "http://core.test")
    ns: dict[str, Any] = {"__name__": "docs_lowlevel", "user_of": lambda ctx: "ada"}
    exec(compile(code[0], str(page), "exec"), ns)

    core = FakeCore(default_ok=True)
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
    async with served(ns["server"]) as (_, rpc):
        listed = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
        assert {"subscribe_events", "unsubscribe_events", "list_subscriptions", "watch"} <= set(listed)
        assert "session_key" in listed["watch"]["inputSchema"]["properties"]
        res = await rpc(
            "tools/call", {"name": "watch", "arguments": {"room": "lobby"}}, headers={"User-Agent": "claude-code/2.1.314"}
        )
        assert [c["text"] for c in res["result"]["content"]] == ["Watching lobby.", "Wakeshift: woken by Monitor."], res
    sub = core.bodies("/event-subscriptions")[0]
    assert sub["arguments"] == {"room": "lobby"} and sub["call"]["user"] == "ada"


LANDING = DOCS.parent / "templates" / "landing.html"


def landing_sample() -> str:
    """The landing page's Python sample as the reader sees it: the <pre> in its tab,
    without highlighting."""
    m = re.search(r'<div class="panel"[^>]*id="c-py"[^>]*>\s*<pre>(.*?)</pre>', LANDING.read_text(), re.S)
    assert m, "landing.html has no Python sample"
    lines = m.group(1).split('<span class="l">')[1:]
    return "\n".join(html.unescape(re.sub(r"<[^>]+>", "", line)) for line in lines) + "\n"


def test_the_landing_pages_python_sample_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    """The landing page's server.py runs as shown, against a fake core: it attaches
    the subscribe tools and emits ci.finished. (site/landing_examples_test.go checks
    the Go sample, and the TypeScript SDK's check-examples the TypeScript one.)"""
    code = landing_sample()
    head, last = code.rstrip("\n").rsplit("\n", 1)
    assert last.startswith("asyncio.run("), last
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("ci-bot"))
    monkeypatch.setenv("WAKESHIFT_CORE", "http://core.test")
    ns: dict[str, Any] = {"__name__": "landing"}
    exec(compile(head, str(LANDING), "exec"), ns)

    tools = {t.name for t in asyncio.run(ns["server"].list_tools())}
    assert "subscribe_events" in tools

    core = FakeCore(default_ok=True)
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 1}))
    ns["wake"]._http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    exec(compile(last, str(LANDING), "exec"), ns)
    emitted = core.bodies("/events/emit")
    assert len(emitted) == 1 and emitted[0]["arguments"] == {"owner": "acme", "repo": "api"}, emitted
    assert "failure" in json.dumps(emitted[0])


def test_the_landing_pages_python_sample_type_checks(tmp_path: Path) -> None:
    """pyright, as CI runs it on the SDK, finds nothing in the landing page's sample."""
    f = tmp_path / "server.py"
    f.write_text(landing_sample())
    res = subprocess.run(
        [sys.executable, "-m", "pyright", "--pythonpath", sys.executable, str(f)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parents[1],
    )
    assert res.returncode == 0, res.stdout + res.stderr
