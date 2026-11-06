"""Round-3 gate fixes: on_subscribe's context (a tool call's, and a voucher's),
plain registration without subscribes, config errors logged, quiet HTTP
logs, built-in tools without execution, and the testing module's failures,
vouchers, finish and aclose."""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import json
import logging
import re
from typing import Any

import httpx2
import pytest
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import APIConnectionError, APIError, Identity, Subscribed, ToolCall, UsageError, Wakeshift
from wakeshift.testing import TestClient, tool_call, with_call

from .helpers import FakeCore, Logged, fake_wake, initialize, served

CANARY = "context-canary-7f3a"
VOUCHER = re.compile(r"wsv_[a-z2-7]{32}")
CHATGPT = {"User-Agent": "openai-mcp/1.0"}
CLAUDE = {"User-Agent": "claude-code/2.1"}
REGION: contextvars.ContextVar[str] = contextvars.ContextVar("region", default="none")


class Job(BaseModel):
    job: str


class Done(BaseModel):
    pages: int


def _text(r: dict[str, Any]) -> str:
    assert "result" in r, r
    return "\n".join(c.get("text", "") for c in r["result"]["content"])


def _webhook(n: int) -> dict[str, Any]:
    return {
        "mode": "webhook",
        "url": f"https://agent.example/hooks/{n}",
        "secret": "whsec_" + "dGVzdC1zZWNyZXQtdGVzdC1zZWNyZXQtdGVzdA==",
    }


class Crawler:
    """A TestClient with a tools-only event whose on_subscribe records what it
    got, and a tool that subscribes with the caller's credential as context."""

    def __init__(self, wake: Wakeshift | None = None) -> None:
        self.wake = wake or TestClient(app="Crawler")
        self.seen: list[tuple[Subscribed[Any], str]] = []
        self.finished = self.wake.define_event(
            "crawl.finished", description="A crawl finished", args=Job, payload=Done, on_subscribe=self.on_subscribe
        )
        self.server = MCPServer("crawler")
        self.attached = self.wake.attach(self.server)
        crawler = self

        @self.attached.tool(subscribes=[self.finished])
        async def start_crawl(job: str, credential: str, call: ToolCall) -> str:
            """Start a crawl."""
            # The vendor's own context (a region, say), set only around the subscribe.
            token = REGION.set("eu-" + job)
            try:
                await call.subscribe(crawler.finished, {"job": job}, context={"credential": credential})
            finally:
                REGION.reset(token)
            return f"Crawling {job}."

    async def on_subscribe(self, s: Subscribed[Any]) -> None:
        self.seen.append((s, REGION.get()))


# ── A1: on_subscribe gets the tool call's context ─────────────────────────


async def test_on_subscribe_gets_the_tool_calls_context_and_runs_in_its_contextvars() -> None:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (200, {"event_subscription": {"id": "es_1", "created": True}, "session": "wsc_1", "pending": "pa_1"}),
    )
    core.on("POST", "/calls/addition", lambda b: (200, {"text": "Wakeshift: woken."}))
    wake, _ = fake_wake(core)
    logged = Logged(logging.DEBUG)
    c = Crawler(wake)
    async with served(c.server) as (_, rpc):
        r = await rpc("tools/call", {"name": "start_crawl", "arguments": {"job": "j1", "credential": CANARY}}, headers=CLAUDE)
    assert "Crawling j1." in _text(r)
    await asyncio.sleep(0.05)
    assert len(c.seen) == 1
    s, region = c.seen[0]
    assert (s.via, s.tool, s.context, region) == ("tool", "start_crawl", {"credential": CANARY}, "eu-j1")
    # Never sent to Wakeshift, never logged, never shown.
    assert all(CANARY not in json.dumps(b) for _, _, b in core.requests)
    assert all(CANARY not in m for m in logged)
    assert CANARY not in repr(s)
    await wake.aclose()


async def test_a_voucher_redemption_gets_the_handing_out_calls_context_and_no_other() -> None:
    c = Crawler()
    wake = c.wake
    assert isinstance(wake, TestClient)
    async with served(c.server) as (_, rpc):
        a = _text(
            await rpc("tools/call", {"name": "start_crawl", "arguments": {"job": "j1", "credential": "cred-a"}}, headers=CHATGPT)
        )
        b = _text(
            await rpc("tools/call", {"name": "start_crawl", "arguments": {"job": "j2", "credential": "cred-b"}}, headers=CHATGPT)
        )
        va, vb = VOUCHER.findall(a), VOUCHER.findall(b)
        assert len(va) == len(vb) == 1 and va != vb
        assert (
            f'To be woken by crawl.finished with {{"job":"j1"}}, subscribe to it (MCP Events) with wakeshift_voucher set to {va[0]}'
            in a
        )
        # Only the vouchers' hashes are kept, never the vouchers.
        keys = set(wake._voucher_contexts)
        assert keys == {hashlib.sha256(v.encode()).hexdigest() for v in (va[0], vb[0])}
        assert not any(v in repr(wake._voucher_contexts) for v in (va[0], vb[0]))
        # A redemption with the wrong arguments is refused, and keeps the context.
        bad = await rpc(
            "events/subscribe",
            {"name": "crawl.finished", "arguments": {"job": "j2", "wakeshift_voucher": va[0]}, "delivery": _webhook(1)},
            headers=CHATGPT,
        )
        assert "error" in bad
        assert len(wake._voucher_contexts) == 2
        # j2's voucher, redeemed: j2's call's context, never j1's.
        ok = await rpc(
            "events/subscribe",
            {"name": "crawl.finished", "arguments": {"job": "j2", "wakeshift_voucher": vb[0]}, "delivery": _webhook(2)},
            headers=CHATGPT,
        )
        assert "result" in ok, ok
        await asyncio.sleep(0.05)
    assert [(s.via, s.tool, s.args.job, s.context) for s, _ in c.seen] == [
        ("tool", "start_crawl", "j2", {"credential": "cred-b"})
    ]
    # Redeemed: forgotten. j1's is still kept until it's redeemed or expires.
    assert wake._take_voucher_context(vb[0]) is None
    assert wake._take_voucher_context(va[0]) == {"credential": "cred-a"}
    await wake.aclose()


async def test_the_voucher_memory_is_bounded_and_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    import wakeshift._client as client

    wake = TestClient()
    now = [1000.0]
    monkeypatch.setattr(client.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(client, "VOUCHER_CONTEXTS", 2)
    tokens = [f"wsv_{c * 32}" for c in "abc"]
    for t in tokens:
        wake._remember_voucher(t, t.upper())
    assert wake._take_voucher_context(tokens[0]) is None  # the oldest went first
    assert wake._take_voucher_context(tokens[1]) == tokens[1].upper()
    now[0] += client.VOUCHER_TTL
    assert wake._take_voucher_context(tokens[2]) is None  # expired with its voucher
    assert not wake._voucher_contexts


async def test_subscribe_events_and_native_subscriptions_get_no_context() -> None:
    wake = TestClient()
    got: list[Any] = []
    news = wake.define_event(
        "news", description="News", args=Job, payload=Done, public=True, on_subscribe=lambda s: got.append(s)
    )
    server = MCPServer("n")
    wake.attach(server, identify=lambda r: Identity(user="u1"))
    async with served(server) as (_, rpc):
        await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "news", "arguments": {"job": "j"}}}, headers=CLAUDE
        )
        await rpc("events/subscribe", {"name": "news", "arguments": {"job": "k"}, "delivery": _webhook(3)}, headers=CHATGPT)
        await asyncio.sleep(0.05)
    assert [(s.via, s.context) for s in got] == [("subscribe_events", None), ("native", None)]
    assert news.name == "news"


# ── B1: a tool without subscribes is a plain registration ─────────────────


@pytest.mark.parametrize("keyed", [False, True])
async def test_a_tool_without_subscribes_needs_no_toolcall_and_never_raises(keyed: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WAKESHIFT_KEY", raising=False)
    wake: Wakeshift = TestClient() if keyed else Wakeshift()
    watched = wake.define_event("product.changed", description="A product changed", args=Job, payload=Done)
    server = MCPServer("shop")
    attached = wake.attach(server)
    ended: list[bool] = []

    @attached.tool()
    async def lookup(job: str) -> str:
        """A plain tool."""
        return "found " + job

    @attached.tool()
    async def unwatch(job: str) -> str:
        """Subscribes nothing, but ends a subscription (Channel3's unwatch_product)."""
        ended.append(await wake.current_call().unsubscribe(watched, {"job": job}))
        return "stopped"

    @attached.tool()
    async def unwatch_with_call(job: str, call: ToolCall) -> str:
        """The same, taking its call."""
        ended.append(await call.unsubscribe(watched, {"job": job}))
        return "stopped"

    async with served(server) as (_, rpc):
        tools = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
        for name in ("lookup", "unwatch", "unwatch_with_call"):
            props = set(tools[name]["inputSchema"]["properties"])
            assert props == ({"job", "session_key"} if keyed else {"job"}), (name, props)
        assert _text(await rpc("tools/call", {"name": "lookup", "arguments": {"job": "j1"}})).startswith("found j1")
        args = {"job": "j1", **({"session_key": "wsc_x"} if keyed else {})}
        assert _text(await rpc("tools/call", {"name": "unwatch", "arguments": args})).startswith("stopped")
        assert _text(await rpc("tools/call", {"name": "unwatch_with_call", "arguments": args})).startswith("stopped")
    assert ended == [False, False]
    if keyed:
        assert isinstance(wake, TestClient) and [u.event for u in wake.unsubscribes] == ["product.changed"] * 2


def test_a_config_error_raises_and_is_logged() -> None:
    logged = Logged(logging.ERROR)
    wake = TestClient()
    server = MCPServer("x")
    wake.attach(server)
    with pytest.raises(UsageError) as e:
        wake.attach(server)
    assert e.value.code == "config"
    attached = wake.attach(MCPServer("y"))
    with pytest.raises(UsageError):

        @attached.tool(name="subscribe_events")
        async def taken() -> str:  # pyright: ignore[reportUnusedFunction]
            return ""

    assert logged.codes() == ["config", "config"]
    assert "attached already" in logged[0] and "the same error was raised" in logged[0]


# ── B6: the SDK's own requests log nothing through httpx2 ─────────────────


async def test_the_sdks_requests_log_no_httpx2_lines_and_the_apps_still_do(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="httpx2")
    wake, core = fake_wake()
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 0}))
    ev = wake.define_event("job.done", description="A job finished", args=Job, payload=Done)
    await ev.emit({"job": "j"}, {"pages": 1})
    assert core.bodies("/events/emit") and not [r for r in caplog.records if r.name.startswith("httpx2")]
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(lambda r: httpx2.Response(204))) as mine:
        await mine.get("https://app.example/health")
    assert [r.levelname for r in caplog.records if r.name == "httpx2"] == ["INFO"]
    assert logging.getLogger("httpx2").level == logging.DEBUG  # the app's setting, untouched
    await wake.aclose()


# ── B7: watch_failed says what happened once ──────────────────────────────


async def test_watch_failed_doesnt_repeat_itself() -> None:
    logged = Logged(logging.WARNING)
    wake = TestClient()
    ev = wake.define_event("job.done", description="A job finished", args=Job, payload=Done)

    async def check() -> Done | None:
        raise RuntimeError("gone")

    task = ev.watch({"job": "j"}, check, max_errors=1)
    assert (await task).outcome == "failed"
    assert logged.codes() == ["watch_failed"]
    assert logged[0].count("without emitting") == 1, logged[0]


async def test_a_watch_stopped_by_a_final_error_logs_info_not_a_warning() -> None:
    warned, info = Logged(logging.WARNING), Logged(logging.INFO)
    wake = TestClient()
    ev = wake.define_event("job.done", description="A job finished", args=Job, payload=Done)

    async def check() -> Done | None:
        raise RuntimeError("no such job")

    result = await ev.watch({"job": "j"}, check, is_final=lambda e: True)
    assert (result.outcome, result.reason) == ("failed", "check")
    assert warned.codes() == []
    assert info.codes() == ["watch_stopped"]
    assert "Check the watch's check" not in info[0]


# ── B10: built-in tools carry no execution unless the server's own do ─────


@pytest.mark.parametrize("own_execution", [False, True])
async def test_builtin_tools_carry_execution_only_when_the_servers_own_do(own_execution: bool) -> None:
    wake = TestClient()
    wake.define_event("news", description="News", args=Job, payload=Done, public=True)
    server = MCPServer("n")
    attached = wake.attach(server, identify=lambda r: Identity(user="u1"))
    for _, entry in attached._builtins.values():
        entry["execution"] = {"taskSupport": "forbidden"}

    @server.tool()
    async def own(job: str) -> str:
        return job

    # The built-ins carry execution whichever way the server lists them:
    # entries Wakeshift adds (above), or tools the framework registered (FastMCP).
    original = attached._amend_list

    def with_execution(tools: list[Any]) -> None:
        for t in tools:
            if t.get("name") in attached._builtins:
                t["execution"] = {"taskSupport": "forbidden"}
            elif t.get("name") == "own" and own_execution:
                t["execution"] = {"taskSupport": "optional"}
        original(tools)

    attached._amend_list = with_execution  # type: ignore[method-assign]
    async with served(server) as (_, rpc):
        tools = (await rpc("tools/list", {}, headers=CLAUDE))["result"]["tools"]
    builtins = [t for t in tools if t["name"] in attached._builtins]
    assert builtins and all(("execution" in t) is own_execution for t in builtins)


# ── The testing module ────────────────────────────────────────────────────


@pytest.mark.parametrize("error", ["unreachable", "refused"])
async def test_the_test_client_fails_the_next_subscribe(error: str) -> None:
    logged = Logged(logging.ERROR)
    c = Crawler()
    wake = c.wake
    assert isinstance(wake, TestClient)
    wake.fail_next("subscribe", error)  # type: ignore[arg-type]
    caught: list[BaseException] = []
    server = MCPServer("f")
    att = wake.attach(server)

    @att.tool(subscribes=[c.finished])
    async def careful(job: str, call: ToolCall) -> str:
        try:
            await call.subscribe(c.finished, {"job": job})
        except APIError as err:
            caught.append(err)
            return "couldn't watch"
        return "watching"

    async with served(server) as (_, rpc):
        assert _text(await rpc("tools/call", {"name": "careful", "arguments": {"job": "j1"}}, headers=CLAUDE)) == "couldn't watch"
        assert _text(await rpc("tools/call", {"name": "careful", "arguments": {"job": "j1"}}, headers=CLAUDE)).startswith(
            "watching"
        )
    want = (APIConnectionError, "unreachable") if error == "unreachable" else (APIError, "unauthenticated")
    assert len(caught) == 1 and isinstance(caught[0], want[0]) and caught[0].code == want[1]
    assert logged.codes() == ([] if error == "unreachable" else ["key_refused"])
    with pytest.raises(UsageError):
        wake.fail_next("emit", "refused")  # type: ignore[arg-type]


async def test_the_stand_in_call_fails_the_next_subscribe_and_unsubscribe() -> None:
    c = Crawler()
    call = tool_call(tool="start_crawl", wake=c.wake)
    call.fail_next("subscribe", "unreachable")
    with pytest.raises(APIConnectionError):
        await call.subscribe(c.finished, {"job": "j1"})
    assert (await call.subscribe(c.finished, {"job": "j1"})).state == "created"
    call.fail_next("unsubscribe", "refused")
    with pytest.raises(APIError) as e:
        await call.unsubscribe(c.finished, {"job": "j1"})
    assert e.value.code == "unauthenticated"
    assert await call.unsubscribe(c.finished, {"job": "j1"}) is True


async def test_a_chatgpt_shaped_stand_in_gets_a_voucher_and_redeem_behaves_as_the_core() -> None:
    c = Crawler()
    wake = c.wake
    assert isinstance(wake, TestClient)
    call = tool_call(tool="start_crawl", native=True, wake=wake)
    sub = await call.subscribe(c.finished, {"job": "j1"}, once=True, context={"credential": "cred-1"})
    assert sub.state == "native" and len(call.vouchers) == 1
    v = call.vouchers[0]
    assert (v.event, v.args) == ("crawl.finished", {"job": "j1"}) and VOUCHER.fullmatch(v.voucher)
    first = await wake.redeem(v.voucher, "https://agent.example/hooks/1")
    assert first.created is True
    assert [(s.via, s.tool, s.context) for s, _ in c.seen] == [("tool", "start_crawl", {"credential": "cred-1"})]
    rec = wake.subscriptions[-1]
    assert (rec.via, rec.tool, rec.once, rec.state) == ("tool", "start_crawl", True, "created")
    again = await wake.redeem(v.voucher, "https://agent.example/hooks/1")
    assert (again.id, again.created) == (first.id, False)
    with pytest.raises(APIError) as e:
        await wake.redeem(v.voucher, "https://agent.example/hooks/2")
    assert e.value.code == "bad_voucher"
    with pytest.raises(APIError):
        await wake.redeem("wsv_" + "a" * 32, "https://agent.example/hooks/1")
    assert len(c.seen) == 1


async def test_finish_runs_on_subscribe_with_context_and_async_with_call_runs_it() -> None:
    c = Crawler()
    call = tool_call(tool="start_crawl", user="ada", wake=c.wake)
    REGION.set("us")
    await call.subscribe(c.finished, {"job": "j1"}, context="cred-1")
    await call.subscribe(c.finished, {"job": "j2"}, context="cred-2")
    await call.unsubscribe(c.finished, {"job": "j2"})  # ended again: no on_subscribe
    assert c.seen == []
    await call.finish()
    assert [(s.args.job, s.user, s.via, s.tool, s.context, r) for s, r in c.seen] == [
        ("j1", "ada", "tool", "start_crawl", "cred-1", "us")
    ]
    await call.finish()
    assert len(c.seen) == 1  # once
    other = tool_call(tool="start_crawl", wake=c.wake)
    async with with_call(c.wake, other) as current:
        await c.wake.current_call().subscribe(c.finished, {"job": "j3"}, context="cred-3")
        assert current is other
    assert [s.context for s, _ in c.seen] == ["cred-1", "cred-3"]


async def test_aclose_waits_for_a_watch_check_in_flight_and_its_emit() -> None:
    wake = TestClient()
    ev = wake.define_event("job.done", description="A job finished", args=Job, payload=Done)

    async def check() -> Done:
        await asyncio.sleep(0.05)  # an async check: nothing has emitted when watch returns
        return Done(pages=3)

    ev.watch({"job": "j1"}, check)
    assert wake.emits == []
    await wake.aclose()
    assert [(e.event, e.args, e.payload) for e in wake.emits] == [("job.done", {"job": "j1"}, {"pages": 3})]


async def test_events_poll_of_a_tools_only_event_names_the_webhook() -> None:
    c = Crawler()
    async with served(c.server) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        listed = await rpc("events/list", {}, headers=CHATGPT)
        polled = await rpc("events/poll", {"name": "crawl.finished", "arguments": {"job": "j1"}}, headers=CHATGPT)
    entry = next(e for e in listed["result"]["events"] if e["name"] == "crawl.finished")
    assert entry["delivery"] == ["webhook"]
    assert (
        polled["error"]["message"]
        == "crawl.finished can't be polled: it's subscribed to only through this app's tools, with a webhook"
    )
