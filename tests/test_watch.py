"""event.watch and an event's on_subscribe (watch.json, the same in every
SDK): the delays, the option checks, how a watch ends, and when on_subscribe
runs, against a fake core."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx2
import pytest
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import Identity, Subscribed, ToolCall, UsageError, Wakeshift, WatchResult
from wakeshift import _watch as watch_mod

from .helpers import FakeCore, Logged, app_key, golden_file, initialize, served

FIXTURE = golden_file("watch.json")


@pytest.fixture(autouse=True)
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    # A thousandth of real time, and no jitter: every 30 s is 30 ms.
    monkeypatch.setattr(watch_mod, "_SCALE", 0.001)
    monkeypatch.setattr(watch_mod, "_JITTER", 0.0)


class Job(BaseModel):
    job: str


class Crawl(BaseModel):
    status: str


def test_defaults() -> None:
    d = FIXTURE["defaults"]
    assert (watch_mod.DEFAULT_EVERY, watch_mod.DEFAULT_TIMEOUT, watch_mod.DEFAULT_MAX_ERRORS) == (
        d["every_seconds"],
        d["timeout_seconds"],
        d["max_errors"],
    )
    assert (watch_mod.MAX_CONCURRENT_CHECKS, watch_mod.MIN_EVERY) == (d["max_concurrent_checks"], d["min_every_seconds"])


@pytest.mark.parametrize("case", FIXTURE["delays"]["cases"], ids=lambda c: f"{c['every']}s-{c['errors_in_a_row']}")
def test_delays(case: dict[str, Any]) -> None:
    assert watch_mod.delay(case["every"], case["errors_in_a_row"]) == case["want"]


class Core:
    """A fake core whose emits answer from a script."""

    def __init__(self, emits: list[str] | None = None) -> None:
        self.emits = list(emits or [])
        self.fake = FakeCore(default_ok=True)
        self.fake.on("POST", "/events/emit", self.emit)

    def emit(self, body: Any) -> tuple[int, Any]:
        step = self.emits.pop(0) if self.emits else "ok"
        if step == "ok":
            return 200, {"matched": 1}
        if step == "bad_payload":
            return 422, {"error": {"code": "bad_payload", "message": "no"}}
        if step == "id_reused":
            return 409, {"error": {"code": "id_reused", "message": "used"}}
        if step == "rate_limited":
            return 429, {"error": {"code": "rate_limited", "message": "slow"}}
        raise AssertionError(step)

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        if request.url.path.endswith("/events/emit") and self.emits and self.emits[0] == "unreachable":
            self.emits.pop(0)
            self.fake.requests.append(("POST", "/events/emit", json.loads(request.content)))
            raise httpx2.ConnectError("refused")
        return self.fake.handler(request)

    def emitted(self) -> list[Any]:
        return self.fake.bodies("/events/emit")


def client(core: Core, *, enabled: bool = True) -> Wakeshift:
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    return Wakeshift(app_key("crawler") if enabled else "", core="https://core.test", http_client=http, max_retries=0)


class Final(Exception):
    pass


def script(steps: list[str]) -> tuple[Any, list[int]]:
    calls: list[int] = []

    async def check() -> Any:
        calls.append(1)
        step = steps[min(len(calls), len(steps)) - 1] if steps else "pending_forever"
        if step in ("pending", "pending_forever"):
            return None
        if step == "error":
            raise RuntimeError("boom")
        if step == "final_error":
            raise Final("gone")
        return {"status": "done"}

    return check, calls


@pytest.mark.parametrize("case", FIXTURE["option_checks"]["cases"], ids=lambda c: c["name"])
async def test_option_checks(case: dict[str, Any]) -> None:
    for enabled in (True, False):
        wake = client(Core(), enabled=enabled)
        ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
        o = case["options"]
        kw: dict[str, Any] = {k: o[k] for k in ("every", "timeout", "max_errors") if k in o}
        check, calls = script(["payload"])
        with pytest.raises(UsageError) as e:
            ev.watch({"nope": 1} if case.get("bad_args") else {"job": "j1"}, check, **kw)
        assert e.value.code == case["error"]
        if case.get("says"):
            assert case["says"] in str(e.value)
        assert calls == []


def test_watch_needs_a_running_loop() -> None:
    wake = client(Core())
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    with pytest.raises(UsageError) as e:
        ev.watch({"job": "j1"}, lambda: None)
    assert e.value.code == "config"


SIMPLE = [
    c
    for c in FIXTURE["outcomes"]["cases"]
    if not any(k in c for k in ("second_watch", "stop", "subscription_state", "call_fails")) and c.get("enabled", True)
]


@pytest.mark.parametrize("case", SIMPLE, ids=lambda c: c["name"])
async def test_outcomes(case: dict[str, Any]) -> None:
    core = Core(case.get("emit"))
    wake = client(core)
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    logged = Logged(logging.INFO)
    check, calls = script(case.get("checks", []))
    kw: dict[str, Any] = {"is_final": lambda e: isinstance(e, Final)}
    if "max_errors" in case.get("options", {}):
        kw["max_errors"] = case["options"]["max_errors"]
    if "pending_forever" in case.get("checks", []):
        kw["timeout"] = 120  # 120 ms here
    result: WatchResult = await ev.watch({"job": "j1"}, check, **kw)
    want = case["want"]
    assert result.outcome == want["outcome"], result
    if "reason" in want:
        assert result.reason == want["reason"]
    if "emitted" in want:
        assert result.emitted is None
    if "emits" in want:
        assert len(core.emitted()) == want["emits"]
    if "checks" in want:
        assert len(calls) == want["checks"]
    if want.get("same_id"):
        ids = {b["id"] for b in core.emitted()}
        assert len(ids) == 1 and next(iter(ids)).startswith("watch-")
    if "logs" in case:
        got = [r for r in logged.records if r.wakeshift_code in ("watch_failed", "watch_stopped", "watch_timeout")]
        assert [r.wakeshift_code for r in got] == [w["code"] for w in case["logs"]]
        for r, w in zip(got, case["logs"], strict=True):
            if "reason" in w:
                assert r.wakeshift_reason == w["reason"]


async def test_the_id_option_is_every_attempts() -> None:
    core = Core(["unreachable", "ok"])
    wake = client(core)
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    r = await ev.watch({"job": "j1"}, lambda: Crawl(status="done"), id="job-j1-done")
    assert r.outcome == "emitted" and [b["id"] for b in core.emitted()] == ["job-j1-done", "job-j1-done"]


async def test_a_second_watch_is_the_running_one() -> None:
    wake = client(Core())
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    check, calls = script(["pending", "pending", "payload"])
    a = ev.watch({"job": "j1"}, check)
    b = ev.watch(Job(job="j1"), check)
    assert a is b
    assert (await a).outcome == "emitted" and len(calls) == 3
    # Once it's done, a new watch runs.
    c = ev.watch({"job": "j1"}, check)
    assert c is not a
    await c


async def test_aclose_stops_every_watch() -> None:
    core = Core()
    wake = client(core)
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    check, _ = script(["pending_forever"])
    task = ev.watch({"job": "j1"}, check)
    await asyncio.sleep(0.05)
    await wake.aclose()
    assert (await task).outcome == "stopped" and core.emitted() == []


async def test_a_disabled_client_runs_nothing() -> None:
    wake = client(Core(), enabled=False)
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    check, calls = script(["payload"])
    assert (await ev.watch({"job": "j1"}, check)).outcome == "skipped"
    assert calls == []


async def test_a_sync_check_runs_in_a_thread_and_a_personal_watch_needs_to() -> None:
    core = Core()
    wake = client(core)
    ev = wake.define_event("turn", description="Your turn", args=Job, payload=Crawl, personal=True)
    with pytest.raises(UsageError):
        ev.watch({"job": "j1"}, lambda: None)
    r = await ev.watch({"job": "j1"}, lambda: {"status": "go"}, to="u1")
    assert r.outcome == "emitted" and core.emitted()[0]["to"] == "u1"


async def test_at_most_ten_checks_at_once() -> None:
    wake = client(Core())
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    running = 0
    most = 0

    async def check() -> Any:
        nonlocal running, most
        running += 1
        most = max(most, running)
        await asyncio.sleep(0.01)
        running -= 1
        return {"status": "done"}

    results = await asyncio.gather(*(ev.watch({"job": f"j{i}"}, check) for i in range(25)))
    assert all(r.outcome == "emitted" for r in results)
    assert most == watch_mod.MAX_CONCURRENT_CHECKS


# ── In a tool call ────────────────────────────────────────────────────────


class CallCore(Core):
    def __init__(self) -> None:
        super().__init__()
        self.created = True
        self.fake.on(
            "POST",
            "/event-subscriptions",
            lambda b: (
                200,
                {
                    "event_subscription": {"id": "s1", "user": (b.get("call") or {}).get("user"), "created": self.created},
                    "session": "wsc_s1",
                    "pending": "pa_1",
                },
            ),
        )
        self.fake.on("POST", "/calls/addition", lambda b: (200, {"text": "woken"}))
        self.fake.on("POST", "/event-subscriptions/unsubscribe", lambda b: (200, {"ended": True}))
        self.fake.on(
            "POST",
            "/events/subscribe",
            lambda b: (
                200,
                {"id": "n1", "refresh_before": "2026-10-22T10:00:00Z", "cursor": "c", "truncated": False, "created": True},
            ),
        )
        self.polled_created = True
        self.fake.on(
            "POST",
            "/events/poll",
            lambda b: (
                200,
                {
                    "events": [],
                    "cursor": "c",
                    "truncated": False,
                    "has_more": False,
                    "next_poll_ms": 1000,
                    "created": self.polled_created,
                },
            ),
        )


async def call(rpc: Any, tool: str, args: dict[str, Any], ua: str = "claude-code/2.1") -> dict[str, Any]:
    return await rpc("tools/call", {"name": tool, "arguments": args}, headers={"User-Agent": ua})


async def test_a_watch_is_skipped_for_an_unavailable_subscription_and_stopped_by_a_rollback() -> None:
    core = CallCore()
    wake = client(core)
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    server = MCPServer("crawler")
    attached = wake.attach(server, identify=lambda r: Identity(user="u1"))
    tasks: list[Any] = []

    @attached.tool(subscribes=[ev])
    async def start(job: str, fail: bool, call: ToolCall) -> str:
        await call.subscribe(ev, {"job": job})
        check, _ = script(["pending_forever"])
        tasks.append(ev.watch({"job": job}, check))
        if fail:
            raise RuntimeError("the job didn't start")
        return "started"

    async with served(server, stateless=False) as (_, rpc):
        # ChatGPT: a tools-only event is unavailable to it, so nothing runs.
        await initialize(rpc, "openai-mcp")
        await call(rpc, "start", {"job": "a", "fail": False}, ua="openai-mcp/1.0")
        assert (await tasks[0]).outcome == "skipped"
    async with served(server) as (_, rpc):
        await call(rpc, "start", {"job": "b", "fail": True})
        assert (await asyncio.wait_for(tasks[1], 1)).outcome == "stopped"
        await call(rpc, "start", {"job": "c", "fail": False})
        assert not tasks[2].done()
    await wake.aclose()
    assert (await tasks[2]).outcome == "stopped"


# ── on_subscribe ──────────────────────────────────────────────────────────


async def settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def test_on_subscribe_runs_once_per_new_subscription_on_every_path() -> None:
    core = CallCore()
    wake = client(core)
    seen: list[Subscribed[Any]] = []

    async def on_subscribe(s: Subscribed[Any]) -> None:
        seen.append(s)

    shut = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl, on_subscribe=on_subscribe)
    opened = wake.define_event(
        "chat.message", description="A message", args=Job, payload=Crawl, public=True, on_subscribe=seen.append
    )
    server = MCPServer("crawler")
    attached = wake.attach(server, identify=lambda r: Identity(user="u1"))

    @attached.tool(subscribes=[shut, opened])
    async def start(job: str, fail: bool, call: ToolCall) -> str:
        await call.subscribe(shut, {"job": job})
        if fail:
            raise RuntimeError("no")
        return "ok"

    @attached.tool(subscribes=[opened])
    async def watch_chat(call: ToolCall) -> str:
        await call.subscribe(opened, {"job": "room"})
        return "ok"

    async with served(server) as (_, rpc):
        await call(rpc, "start", {"job": "a", "fail": False})
        await settle()
        assert [(s.via, s.tool, s.user, s.args) for s in seen] == [("tool", "start", "u1", Job(job="a"))]
        core.created = False  # renewed: nothing
        await call(rpc, "start", {"job": "a", "fail": False})
        core.created = True
        await call(rpc, "start", {"job": "b", "fail": True})  # rolled back: nothing
        await settle()
        assert len(seen) == 1
        await call(rpc, "subscribe_events", {"event": "chat.message", "arguments": {"job": "x"}})
        core.created = False
        await call(rpc, "subscribe_events", {"event": "chat.message", "arguments": {"job": "x"}})
        await settle()
        assert [(s.via, s.tool) for s in seen[1:]] == [("subscribe_events", None)]
    seen.clear()
    async with served(server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        # A native client's tool call: the block names the event; its own events/subscribe fires it.
        await call(rpc, "watch_chat", {}, ua="openai-mcp/1.0")
        await settle()
        assert seen == []
        delivery = {"mode": "webhook", "url": "https://chatgpt.example/cb", "secret": "whsec_x"}
        await rpc("events/subscribe", {"name": "chat.message", "arguments": {"job": "room"}, "delivery": delivery})
        await rpc("events/poll", {"name": "chat.message", "arguments": {"job": "p"}})
        core.polled_created = False
        await rpc("events/poll", {"name": "chat.message", "arguments": {"job": "p"}, "cursor": "c"})
        await settle()
        assert [(s.via, s.args, s.user) for s in seen] == [("native", Job(job="room"), "u1"), ("native", Job(job="p"), "u1")]


async def test_a_failing_on_subscribe_is_logged_and_the_subscription_stands() -> None:
    core = CallCore()
    wake = client(core)

    def boom(s: Subscribed[Any]) -> None:
        raise ValueError("secret detail")

    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl, on_subscribe=boom)
    server = MCPServer("crawler")
    attached = wake.attach(server, identify=lambda r: Identity(user="u1"))

    @attached.tool(subscribes=[ev])
    async def start(call: ToolCall) -> str:
        await call.subscribe(ev, {"job": "a"})
        return "ok"

    logged = Logged()
    async with served(server) as (_, rpc):
        res = await call(rpc, "start", {})
        await settle()
    assert not res["result"].get("isError")
    assert logged.codes() == ["on_subscribe_failed"] and "secret detail" not in logged[0]
    assert logged.records[0].wakeshift_error_type == "ValueError"
    assert core.fake.bodies("/event-subscriptions/unsubscribe") == []


async def test_on_subscribe_starting_a_watch() -> None:
    """The documented pattern: one on_subscribe covers every path."""
    core = CallCore()
    wake = client(core)
    done: list[Any] = []

    def on_subscribe(s: Subscribed[Job]) -> None:
        done.append(ev.watch(s.args, lambda: Crawl(status="done")))

    ev = wake.define_event(
        "chat.message", description="A message", args=Job, payload=Crawl, public=True, on_subscribe=on_subscribe
    )
    server = MCPServer("crawler")
    wake.attach(server, identify=lambda r: Identity(user="u1"))
    async with served(server) as (_, rpc):
        await call(rpc, "subscribe_events", {"event": "chat.message", "arguments": {"job": "x"}})
        await settle()
    assert (await done[0]).outcome == "emitted"
    assert core.emitted()[0]["arguments"] == {"job": "x"}
