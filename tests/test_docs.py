"""The site's Python examples run: in Wakeshift's own repository, site/docs/python.md's
python blocks run in order, in one namespace, against a fake core (its FastMCP block in
a namespace of its own), and so do site/docs/low-level.md's low-level Server block and
AGENTS.md's (site/llms/agents.md); the quickstart's Python tab is the typing gate's
quickstart; every other python block in site/docs at least compiles. The README's are
tested in test_readme.py. The published copy of the SDK has no site, so this skips there."""

from __future__ import annotations

import asyncio
import html
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx2
import pytest
from mcp.server.auth.provider import AccessToken

from .helpers import FakeCore, Rpc, app_key, initialize, served

DOCS = Path(__file__).resolve().parents[3] / "site" / "docs"
BLOCK = re.compile(r"^```python([^\n]*)\n(.*?)^```", re.S | re.M)
pytestmark = pytest.mark.skipif(not DOCS.is_dir(), reason="no site docs here")


#: The blocks that run in a namespace of their own, by the file their info
#: string names.
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


#: /AGENTS.md, the instructions for coding agents the site serves.
AGENTS = DOCS.parent / "llms" / "agents.md"


def subscribed(b: Any) -> tuple[int, Any]:
    """The fake core's answer to a subscribe: created, for the caller's user."""
    return 200, {"event_subscription": {"id": "es_1", "user": b["call"].get("user"), "created": True}, "session": "wsc_1"}


def fake_core() -> FakeCore:
    core = FakeCore(default_ok=True)
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 1}))
    core.on("POST", "/event-subscriptions", subscribed)
    core.on("POST", "/calls/addition", lambda b: (200, {"text": "Wakeshift: woken by Monitor."}))
    return core


def signed_in(subject: str) -> Any:
    """An ASGI wrapper that does what a server's auth does: puts the verified
    token in the request's scope."""

    def wrap(app: Any) -> Any:
        async def asgi(scope: Any, receive: Any, send: Any) -> None:
            if scope["type"] == "http":
                token = AccessToken(token="t", client_id="c", scopes=[], subject=subject)
                scope = {**scope, "user": SimpleNamespace(access_token=token)}
            await app(scope, receive, send)

        return asgi

    return wrap


def test_every_python_block_compiles() -> None:
    pages = [*sorted(DOCS.glob("*.md")), AGENTS]
    found = 0
    for page in pages:
        for b in blocks(page):
            compile(b, str(page), "exec")
            found += 1
    assert found >= 2


def test_the_quickstarts_python_tab_is_the_typing_gates() -> None:
    page = DOCS / "quickstart.md"
    tab = re.findall(r"^```python tab:Python\n(.*?)^```", page.read_text(), re.S | re.M)
    quickstart = (Path(__file__).parent / "typing" / "quickstart.py").read_text().split('"""', 2)[2]
    assert tab and tab[0].strip() == quickstart.strip(), (
        "the quickstart's Python tab is tests/typing/quickstart.py, which the typing gate checks"
    )


#: The core's rule for a routing argument value (internal/apps/eventtypes.go, topicValueRE).
ARG_VALUE = re.compile(r"[a-z0-9._-]{1,100}")


def checks_arguments(b: Any) -> tuple[int, Any]:
    """The fake core's subscribe, refusing argument values the core refuses."""
    for name, value in (b.get("arguments") or {}).items():
        if isinstance(value, str) and not ARG_VALUE.fullmatch(value):
            msg = f"argument {name!r} is required and must be lowercase a-z 0-9 . _ - (at most 100 characters)"
            return 400, {"error": {"code": "bad_arguments", "message": msg}}
    return subscribed(b)


async def test_the_quickstarts_python_tab_runs_with_a_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """The quickstart's Python tab and step 3's stand-in crawl run as one module, with the
    key set, against a fake core that checks arguments as the core does: start_crawl
    subscribes once, with arguments the core accepts."""
    tabs = re.findall(r"^```python tab:Python\n(.*?)^```", (DOCS / "quickstart.md").read_text(), re.S | re.M)
    assert len(tabs) == 2, "the quickstart has two Python blocks: the code, and step 3"
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("crawler"))
    monkeypatch.setenv("WAKESHIFT_CORE", "https://core.test")
    ns: dict[str, Any] = {"__name__": "quickstart"}
    exec(compile(tabs[0] + "\n" + tabs[1], "quickstart.md", "exec"), ns)

    core = fake_core()
    core.on("POST", "/event-subscriptions", checks_arguments)
    ns["wake"]._http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    async with served(ns["server"]) as (_, rpc):
        res = await rpc(
            "tools/call",
            {"name": "start_crawl", "arguments": {"url": "https://example.com"}},
            headers={"User-Agent": "claude-code/2.1.314"},
        )
        assert "result" in res and not res["result"].get("isError"), res
    subs = core.bodies("/event-subscriptions")
    assert len(subs) == 1 and subs[0]["name"] == "crawl.finished" and subs[0]["once"] is True, subs
    assert all(ARG_VALUE.fullmatch(str(v)) for v in subs[0]["arguments"].values()), subs[0]["arguments"]


async def test_the_agents_files_python_example_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    """AGENTS.md's server.py runs as shown, against a fake core: its start_crawl tool
    subscribes the calling conversation once, crawl.finished is open only to the
    app's tools, and on_crawl_finished emits with an id."""
    code = blocks(AGENTS)
    assert len(code) == 1, "AGENTS.md has one python block"
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("crawler"))
    monkeypatch.setenv("WAKESHIFT_CORE", "https://core.test")
    ns: dict[str, Any] = {"__name__": "agents_md"}
    exec(compile(code[0], str(AGENTS), "exec"), ns)

    core = fake_core()
    core.on("POST", "/event-subscriptions", checks_arguments)
    ns["wake"]._http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    assert ns["app"] is not None
    async with served(ns["server"]) as (_, rpc):
        listed = {t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"]}
        assert listed == {"start_crawl", "unsubscribe_events", "list_subscriptions"}, "crawl.finished is tools-only"
        res = await rpc(
            "tools/call",
            {"name": "start_crawl", "arguments": {"url": "https://example.com"}},
            headers={"User-Agent": "claude-code/2.1.314"},
        )
        assert "result" in res and not res["result"].get("isError"), res
    sub = core.bodies("/event-subscriptions")[0]
    assert sub["name"] == "crawl.finished" and sub["once"] is True and set(sub["arguments"]) == {"job"}

    await ns["on_crawl_finished"]("j1", 42)
    emitted = core.bodies("/events/emit")[0]
    assert emitted["arguments"] == {"job": "j1"} and emitted["id"] == "crawl-j1", emitted
    assert emitted["payload"] == {"status": "completed", "pages": 42}


async def test_the_python_pages_examples_run(monkeypatch: pytest.MonkeyPatch) -> None:
    page = DOCS / "python.md"
    code = blocks(page, "")
    assert code, "python.md has no python blocks"
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("chat"))
    monkeypatch.setenv("WAKESHIFT_CORE", "https://core.test")
    # What the examples take from around them: your own sign-in and access checks.
    ns: dict[str, Any] = {
        "__name__": "docs",
        "user_of": lambda token: token.subject,
        "can_read": lambda user, channel: user == "ada",
    }
    for b in code:
        exec(compile(b, str(page), "exec"), ns)

    server = ns["server"]
    core = fake_core()
    ns["wake"]._http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    async with served(server) as (_, rpc):
        tools = {t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"]}
    assert {"subscribe_events", "unsubscribe_events", "list_subscriptions", "join"} <= tools

    await ns["on_message"]("eng", "ada", "shipped it")
    assert core.bodies("/events/emit")[0]["arguments"] == {"channel": "eng"}
    async with served(server, wrap=signed_in("ada")) as (_, rpc):
        res = await rpc(
            "tools/call", {"name": "join", "arguments": {"channel": "eng"}}, headers={"User-Agent": "claude-code/2.1.280"}
        )
        assert "result" in res and not res["result"].get("isError"), res
    subs = core.bodies("/event-subscriptions")
    assert [s["name"] for s in subs] == ["chat.message", "chat.mention"]
    assert subs[0]["arguments"] == {"channel": "eng"} and subs[0]["call"]["user"] == "ada"
    assert ns["members"]["eng"][0].user == "ada"

    await ns["on_mention"]("eng", "bo", ["ada"], "m1")
    mention = core.bodies("/events/emit")[1]
    assert mention["to"] == "ada" or mention["to"] == ["ada"], mention
    assert await ns["emit_message"]("eng", "bo", "hi", "d1") == 202
    core.on("POST", "/events/publish-urls", lambda b: (200, {"id": "pu_1", "url": "https://core.test/p/wsp_x"}))
    assert await ns["webhook_url_for"]("eng") == "https://core.test/p/wsp_x"


async def test_the_python_pages_fastmcp_example_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("fastmcp")
    from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

    page = DOCS / "python.md"
    code = blocks(page, "fastmcp_server.py")
    assert len(code) == 1, "python.md has one FastMCP block"
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("ci"))
    monkeypatch.setenv("WAKESHIFT_CORE", "https://core.test")
    # What the example takes from around it: your FastMCP auth and access check.
    verifier = StaticTokenVerifier(tokens={"tok": {"client_id": "c", "scopes": [], "sub": "ada"}})
    ns: dict[str, Any] = {
        "__name__": "docs_fastmcp",
        "verifier": verifier,
        "can_see_repo": lambda user, repo: user == "ada",
    }
    exec(compile(code[0], str(page), "exec"), ns)

    core = fake_core()
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


async def test_the_low_level_pages_python_example_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    page = DOCS / "low-level.md"
    code = blocks(page, "lowlevel_server.py")
    assert len(code) == 1, "low-level.md has one low-level Server block"
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("crawler"))
    monkeypatch.setenv("WAKESHIFT_CORE", "https://core.test")
    # What the example takes from around it: your own sign-in.
    ns: dict[str, Any] = {"__name__": "docs_lowlevel", "user_of": lambda token: token.subject}
    exec(compile(code[0], str(page), "exec"), ns)

    core = fake_core()
    ns["wake"]._http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    async with served(ns["server"], wrap=signed_in("ada")) as (_, rpc):
        listed = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
        assert {"unsubscribe_events", "list_subscriptions", "start_crawl"} <= set(listed)
        assert "subscribe_events" not in listed, "crawl.finished is tools-only"
        assert "session_key" in listed["start_crawl"]["inputSchema"]["properties"]
        res = await rpc(
            "tools/call",
            {"name": "start_crawl", "arguments": {"url": "https://example.com"}},
            headers={"User-Agent": "claude-code/2.1.314"},
        )
        texts = [c["text"] for c in res["result"]["content"]]
        assert texts[0].startswith("Crawl ") and texts[-1] == "Wakeshift: woken by Monitor.", res
    sub = core.bodies("/event-subscriptions")[0]
    assert sub["once"] is True and sub["call"]["user"] == "ada"


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
    the subscribe tools and emits crawl.finished. (site/landing_examples_test.go checks
    the Go sample, and the TypeScript SDK's check-examples the TypeScript one.)"""
    code = landing_sample()
    head, last = code.rstrip("\n").rsplit("\n", 1)
    assert last.startswith("asyncio.run("), last
    monkeypatch.setenv("WAKESHIFT_KEY", app_key("crawler"))
    monkeypatch.setenv("WAKESHIFT_CORE", "https://core.test")
    ns: dict[str, Any] = {"__name__": "landing"}
    exec(compile(head, str(LANDING), "exec"), ns)

    async def listed() -> set[str]:
        async with served(ns["server"]) as (_, rpc):
            return {t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"]}

    assert "subscribe_events" in asyncio.run(listed()), "ci.finished is open to every agent"

    core = FakeCore(default_ok=True)
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 1}))
    ns["wake"]._http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    exec(compile(last, str(LANDING), "exec"), ns)
    emitted = core.bodies("/events/emit")
    assert len(emitted) == 1 and emitted[0]["arguments"] == {"site": "example.com"}, emitted
    assert emitted[0]["payload"] == {"pages": 1834}, emitted


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
