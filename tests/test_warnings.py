"""warnings.json: which situations log, at which level and with which code
(design §2.11). Each situation runs on a fresh client; its lines at info
level and above, from construction on, must be exactly the fixture's, in
order, each with the fields it gives. The message contains the code's fix
verbatim and never the vendor's own error message."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx2
import pytest
from mcp.server.mcpserver import MCPServer as OfficialMCPServer
from pydantic import BaseModel

from wakeshift import APIError, Identity, ToolCall, WakeEvent, Wakeshift
from wakeshift._log import CODES

from .helpers import FakeCore, Logged, app_key, golden_file, initialize, served

FIXTURE = golden_file("warnings.json")
QUICKSTART = Path(__file__).parent / "typing" / "quickstart.py"
LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}

#: Situations a later commit of this release implements.
PENDING: set[str] = set()


def test_the_codes_are_the_shared_list() -> None:
    py = [c for c in FIXTURE["codes"] if "python" in c.get("languages", ["python"])]
    assert sorted(CODES) == sorted(c["code"] for c in py)
    for c in py:
        assert CODES[c["code"]].fix == c.get("fix_python", c["fix"]), c["code"]
        assert CODES[c["code"]].level == LEVELS[c["level"]], c["code"]
        kind = {"once_per_client": "once", "rate_limited": "rate", "each": "each"}[c["policy"]["kind"]]
        assert CODES[c["code"]].policy == kind, c["code"]
        assert list(CODES[c["code"]].per) == c["policy"].get("per", []), c["code"]


class Job(BaseModel):
    job: str


class Crawl(BaseModel):
    pages: int


class Channel(BaseModel):
    channel: str


class Message(BaseModel):
    text: str


class Env(BaseModel):
    env: str


class Deploy(BaseModel):
    status: str


class Table(BaseModel):
    table: str


class Error(Exception):
    """An error whose type name is the fixture's ("Error")."""


class Scene:
    """A fresh client with a key against a fake core, the sample events, and
    an MCPServer with start_crawl and watch, as warnings.json's situations
    describe them."""

    def __init__(self, *, identify: Callable[[Any], Any] | None = lambda r: Identity(user="u1"), attach: bool = True) -> None:
        self.core = FakeCore(default_ok=True)
        self.logged = Logged(logging.INFO)
        self.clock = [0.0]
        self.wake = Wakeshift(
            app_key("crawler"),
            core="https://core.test",
            http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(self.core.handler)),
        )
        self.wake._log.clock = lambda: self.clock[0]
        self.authorize_error: BaseException | None = None
        w = self.wake
        self.crawl = w.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
        self.chat = w.define_event("chat.message", description="A message", args=Channel, payload=Message, public=True)
        self.deploy = w.define_event(
            "deploy.finished", description="A deploy finished", args=Env, payload=Deploy, authorize=self._authorize
        )
        self.turn = w.define_event("game.turn", description="Your move", args=Table, payload=Message, personal=True)
        self.news = w.define_event("table.news", description="News at a table", args=Table, payload=Message)
        self.server = OfficialMCPServer("crawler")
        self.body: Callable[[ToolCall], Awaitable[Any]] = self._subscribe_news
        if attach:
            self.attached = w.attach(self.server, identify=identify)
            self.register()

    def _authorize(self, s: Any, a: Any) -> bool:
        if self.authorize_error is not None:
            raise self.authorize_error
        return True

    async def _subscribe_news(self, call: ToolCall) -> None:
        await call.subscribe(self.news, {"table": "oak"})

    def register(self) -> None:
        scene = self

        @self.attached.tool(subscribes=[self.crawl])
        async def start_crawl(url: str, call: ToolCall) -> str:
            """Crawl a site."""
            await call.subscribe(scene.crawl, {"job": "j1"}, once=True)
            return "started"

        @self.attached.tool(subscribes=[self.news])
        async def watch(table: str, call: ToolCall) -> str:
            """Watch a table."""
            await scene.body(call)
            return "watching"

    async def call(self, name: str, args: dict[str, Any], **headers: str) -> dict[str, Any]:
        async with served(self.server) as (_, rpc):
            return await rpc("tools/call", {"name": name, "arguments": args}, headers=headers or None)

    def lines(self) -> list[dict[str, Any]]:
        out = []
        for r in self.logged.records:
            line = {"code": getattr(r, "wakeshift_code", "")}
            for k, v in vars(r).items():
                if k.startswith("wakeshift_") and k != "wakeshift_code":
                    line[k.removeprefix("wakeshift_")] = v
            out.append(line)
        return out


def quickstart(monkeypatch: pytest.MonkeyPatch, *, key: bool, identify: bool = False) -> dict[str, Any]:
    monkeypatch.delenv("WAKESHIFT_KEY", raising=False)
    monkeypatch.delenv("WAKESHIFT_CORE", raising=False)
    if key:
        monkeypatch.setenv("WAKESHIFT_KEY", app_key("crawler"))
        monkeypatch.setenv("WAKESHIFT_CORE", "https://core.test")
    code = QUICKSTART.read_text()
    if identify:
        code = code.replace(
            "attached = wake.attach(server)",
            "attached = wake.attach(server, identify=lambda r: __import__('wakeshift').Identity(user='u1'))",
        )
    ns: dict[str, Any] = {"__name__": "quickstart"}
    exec(compile(code, str(QUICKSTART), "exec"), ns)
    return ns


async def run_quickstart(ns: dict[str, Any]) -> None:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (200, {"event_subscription": {"id": "es_1", "created": True}, "session": "wsc_1"}),
    )
    core.on("POST", "/calls/addition", lambda b: (200, {"text": "Wakeshift: woken by Monitor."}))
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 1}))
    ns["wake"]._http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    async with served(ns["server"]) as (_, rpc):
        listed = await rpc("tools/list", {})
        assert "start_crawl" in {t["name"] for t in listed["result"]["tools"]}
        r = await rpc(
            "tools/call",
            {"name": "start_crawl", "arguments": {"url": "https://x.example"}},
            headers={"User-Agent": "claude-code/2.1"},
        )
        assert not r["result"].get("isError"), r
    await ns["on_crawl_finished"]("j1", 3)


async def situation(op: str, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Runs one situation and returns its lines."""
    if op.startswith("quickstart_"):
        logged = Logged(logging.INFO)
        ns = quickstart(monkeypatch, key=op != "quickstart_keyless", identify=op == "quickstart_with_identify")
        await run_quickstart(ns)
        return [{"code": c} for c in logged.codes()]
    if op == "no_key_twice":
        monkeypatch.delenv("WAKESHIFT_KEY", raising=False)
        logged = Logged(logging.INFO)
        Wakeshift()
        Wakeshift()
        return [{"code": c} for c in logged.codes()]
    if op == "no_identify":
        s = Scene(identify=None)
        await s.call("watch", {"table": "oak"})
        return s.lines()
    if op == "attach_no_events":
        logged = Logged(logging.INFO)
        w = Wakeshift(app_key("crawler"), core="https://core.test")
        w.attach(OfficialMCPServer("x"), identify=lambda r: None)
        return [{"code": c} for c in logged.codes()]
    if op == "attach_per_request":
        s = Scene(attach=False)
        for _ in range(3):
            server = OfficialMCPServer("crawler")
            att = s.wake.attach(server)

            @att.tool(subscribes=[s.news])
            async def watch(table: str, call: ToolCall) -> str:
                await call.subscribe(s.news, {"table": table})
                return "watching"

            async with served(server) as (_, rpc):
                await rpc("tools/call", {"name": "watch", "arguments": {"table": "oak"}})
        return s.lines()
    if op == "text_trimmed":
        s = Scene()
        s.core.on("POST", "/events/emit", lambda b: (200, {"matched": 1, "text_trimmed": True}))
        await s.chat.emit({"channel": "eng"}, {"text": "x" * 2000})
        await s.chat.emit({"channel": "eng"}, {"text": "x" * 2000})
        await s.turn.emit({"table": "oak"}, {"text": "x" * 2000}, to="u1")
        return s.lines()
    if op in ("rollback_failed", "rollback_failed_after_window"):
        s = Scene()
        s.core.on(
            "POST",
            "/event-subscriptions",
            lambda b: (200, {"event_subscription": {"id": "es_1", "created": True}, "session": "wsc_1"}),
        )
        s.core.on("POST", "/event-subscriptions/unsubscribe", lambda b: (500, {"error": {"code": "internal", "message": "no"}}))

        async def fail(call: ToolCall) -> None:
            await call.subscribe(s.news, {"table": "oak"})
            raise Error("boom-canary")

        s.body = fail
        for i in range(3):
            s.clock[0] = 20.0 * i
            await s.call("watch", {"table": "oak"}, **{"User-Agent": "claude-code/2.1"})
        if op == "rollback_failed_after_window":
            s.clock[0] = 20.0 * 2 + 300
            await s.call("watch", {"table": "oak"}, **{"User-Agent": "claude-code/2.1"})
        return s.lines()
    if op == "addition_failed":
        s = Scene()
        s.core.on(
            "POST",
            "/event-subscriptions",
            lambda b: (200, {"event_subscription": {"id": "es_1", "created": True}, "session": "wsc_1", "pending": "pa_1"}),
        )
        s.core.on("POST", "/event-subscriptions/unsubscribe", lambda b: (200, {"ended": True}))
        s.core.on("POST", "/calls/addition", lambda b: (503, {"error": {"code": "unavailable", "message": "down"}}))

        async def watching(call: ToolCall) -> str:
            await call.subscribe(s.news, {"table": "oak"})
            return "Watching."

        s.body = watching
        for i in range(3):
            s.clock[0] = 20.0 * i
            r = await s.call("watch", {"table": "oak"}, **{"User-Agent": "claude-code/2.1"})
            assert not r["result"].get("isError") and [c["text"] for c in r["result"]["content"]] == ["watching"], r
        return s.lines()
    if op == "authorize_throws":
        s = Scene()
        s.authorize_error = Error("boom-canary")
        async with served(s.server) as (_, rpc):
            for _ in range(2):
                r = await rpc(
                    "events/poll",
                    {"name": "deploy.finished", "arguments": {"env": "prod"}},
                    headers={"User-Agent": "openai-mcp/1.0"},
                )
                assert "error" in r
        assert not any("boom-canary" in m for m in s.logged)
        return s.lines()
    if op == "key_refused":
        s = Scene()
        s.core.on("POST", "/events/emit", lambda b: (401, {"error": {"code": "unauthenticated", "message": "bad key"}}))
        for i in range(3):
            s.clock[0] = 10.0 * i
            with pytest.raises(APIError) as e:
                await s.chat.emit({"channel": "eng"}, {"text": "hi"})
            assert e.value.code == "unauthenticated"
        return s.lines()
    if op == "subscribe_in_flight":
        s = Scene()
        s.core.on(
            "POST",
            "/event-subscriptions",
            lambda b: (200, {"event_subscription": {"id": "es_1", "created": True}, "session": "wsc_1"}),
        )
        tasks: list[asyncio.Task[Any]] = []

        async def forget(call: ToolCall) -> None:
            tasks.append(asyncio.ensure_future(call.subscribe(s.news, {"table": "oak"})))

        async def slow(r: httpx2.Request) -> httpx2.Response:
            await asyncio.sleep(0.02)  # the subscribe is still in flight when the tool returns
            return s.core.handler(r)

        s.wake._http = httpx2.AsyncClient(transport=httpx2.MockTransport(slow))
        s.body = forget
        await s.call("watch", {"table": "oak"}, **{"User-Agent": "claude-code/2.1"})
        return s.lines()
    if op in ("wrap_unknown_tool", "wrap_arg_conflict", "builtin_name_conflict", "events_capability_kept"):
        s = Scene(attach=False)
        server = OfficialMCPServer("crawler")

        @server.tool()
        async def start_crawl(url: str) -> str:
            return "started"

        @server.tool()
        async def legacy_watch(table: str, session_key: str) -> str:
            return "watching"

        wrap: dict[str, Any] = {}
        if op == "wrap_unknown_tool":
            wrap = {"stat_crawl": [s.crawl]}
        elif op == "wrap_arg_conflict":
            wrap = {"legacy_watch": [s.news]}
        elif op == "builtin_name_conflict":

            @server.tool(name="list_subscriptions")
            async def mine() -> str:
                return "mine"

        else:

            async def events_list(ctx: Any, params: Any) -> dict[str, Any]:
                return {"events": []}

            from mcp_types import RequestParams

            server._lowlevel_server.add_request_handler("events/list", RequestParams, events_list)
        s.wake.attach(server, identify=lambda r: Identity(user="u1"), wrap=wrap)
        async with served(server) as (_, rpc):
            for _ in range(2):
                listed = await rpc("tools/list", {})
                assert "result" in listed, listed
        return s.lines()
    if op == "native_unavailable":
        s = Scene(identify=lambda r: Identity(user=None))
        events = [s.crawl, s.crawl, s.chat]
        server = OfficialMCPServer("crawler")
        att = s.wake.attach(server, identify=lambda r: Identity(user=None))
        picked: list[Any] = []

        @att.tool(subscribes=[s.crawl, s.chat])
        async def start_crawl(url: str, call: ToolCall) -> str:
            ev = events[len(picked)]
            picked.append(ev)
            await call.subscribe(ev, {"job": "j1"} if ev is s.crawl else {"channel": "eng"})
            return "started"

        async with served(server) as (_, rpc):
            for _ in events:
                await rpc(
                    "tools/call", {"name": "start_crawl", "arguments": {"url": "u"}}, headers={"User-Agent": "openai-mcp/1"}
                )
        return s.lines()
    if op in ("event_closed", "event_opened"):
        s = Scene()
        changed = (
            [
                {"event": "crawl.finished", "from": "any", "to": "tools", "ended": 0},
                {"event": "build.finished", "from": "any", "to": "tools", "ended": 3},
            ]
            if op == "event_closed"
            else [{"event": "build.finished", "from": "tools", "to": "any", "ended": 0}]
        )
        s.core.on("PUT", "/manifest", lambda b: (200, {**b, "changed": changed}))
        await s.wake.publish_manifest({"description": "Crawls"})
        return s.lines()
    if op == "undeclared_subscribe_doesnt_log":
        s = Scene()
        caught: list[Any] = []

        async def undeclared(call: ToolCall) -> None:
            try:
                await call.subscribe(s.crawl, {"job": "j1"})
            except Exception as err:  # noqa: BLE001
                caught.append(err)

        s.body = undeclared
        await s.call("watch", {"table": "oak"})
        assert caught and getattr(caught[0], "code", "") == "undeclared_subscribe"
        return s.lines()
    if op in ("identify_throws", "identify_not_called_for_list"):

        def identify(r: Any) -> Any:
            raise Error("boom-canary")

        s = Scene(identify=identify)
        async with served(s.server, stateless=False) as (_, rpc):
            await initialize(rpc, "claude-code")
            await rpc("ping")
            await rpc("tools/list", {})
            if op == "identify_throws":
                r = await rpc("tools/call", {"name": "watch", "arguments": {"table": "oak"}})
                assert r["result"]["content"][0]["text"] == "watching" or r["result"].get("isError")
        assert not any("boom-canary" in m for m in s.logged)
        return s.lines()
    if op == "require_user_no_user":
        s = Scene(attach=False)
        att = s.wake.attach(s.server, identify=lambda r: Identity(user=None), require_user=True)
        refused: list[Any] = []

        @att.tool(subscribes=[s.crawl])
        async def start_crawl(url: str, call: ToolCall) -> str:
            try:
                await call.subscribe(s.crawl, {"job": "j1"})
            except Exception as err:  # noqa: BLE001
                refused.append(getattr(err, "code", ""))
            return "started"

        for _ in range(2):
            await s.call("start_crawl", {"url": "u"}, **{"User-Agent": "claude-code/2.1"})
        assert refused == ["user_required", "user_required"]
        return s.lines()
    if op == "tool_without_subscribes":
        monkeypatch.delenv("WAKESHIFT_KEY", raising=False)
        logged = Logged(logging.INFO)
        for keyed in (False, True):
            if keyed:
                s = Scene(attach=False)
                wake, server = s.wake, s.server
            else:
                wake, server = Wakeshift(), OfficialMCPServer("crawler")
            att = wake.attach(server, identify=lambda r: Identity(user="u1"))

            @att.tool()
            async def plain(url: str) -> str:
                """A plain tool."""
                return "plain " + url

            async with served(server) as (_, rpc):
                listed = await rpc("tools/list", {})
                tool = next(t for t in listed["result"]["tools"] if t["name"] == "plain")
                props = set(tool["inputSchema"]["properties"])
                assert props == ({"url", "session_key"} if keyed else {"url"}), props
                r = await rpc("tools/call", {"name": "plain", "arguments": {"url": "u"}})
                assert r["result"]["content"][0]["text"].startswith("plain u"), r
        return [{"code": c} for c in logged.codes()]
    if op == "monitor_line_long":
        s = Scene()
        s.news._text = lambda p, a: "x" * 300
        s.core.on("POST", "/events/emit", lambda b: (200, {"matched": 1}))
        await s.news.emit({"table": "oak"}, {"text": "hi"})
        await s.news.emit({"table": "oak"}, {"text": "hi"})
        s.chat._text = lambda p, a: "y" * 200
        await s.chat.emit({"channel": "eng"}, {"text": "hi"})
        return s.lines()
    raise AssertionError(f"no op {op}")


def _ops() -> list[dict[str, Any]]:
    return FIXTURE["situations"]["cases"]


@pytest.mark.parametrize("case", _ops(), ids=lambda c: c["op"])
async def test_situation(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    if (case.get("skip") or {}).get("python"):
        pytest.skip(case["skip"]["python"])
    if case["op"] in PENDING:
        pytest.skip("a later commit of this release")
    got = await situation(case["op"], monkeypatch)
    want = case["logs"]
    assert [line["code"] for line in got] == [w["code"] for w in want], (case["name"], got)
    for g, w in zip(got, want, strict=True):
        for k, v in w.items():
            assert g.get(k) == v, (case["name"], k, g)


def test_every_line_carries_its_fix(monkeypatch: pytest.MonkeyPatch) -> None:
    logged = Logged(logging.DEBUG)
    monkeypatch.delenv("WAKESHIFT_KEY", raising=False)
    Wakeshift()
    w = Wakeshift(app_key("crawler"), core="https://core.test")
    w.attach(OfficialMCPServer("x"))
    for r in logged.records:
        code = getattr(r, "wakeshift_code", None)
        if code:
            assert CODES[code].fix in r.getMessage()


def test_unused_events_are_fine() -> None:
    # A WakeEvent is generic; nothing here logs.
    assert WakeEvent is not None
