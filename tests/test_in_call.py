"""Requests made inside a tool call (subscribe, unsubscribe, resolve_session,
the result's addition, a rollback): never retried, bounded by call_timeouts in
all, and skipped by the client's circuit breaker while Wakeshift keeps
failing. Everything else (emits) keeps the client's timeout and retries."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import httpx2
import pytest

from wakeshift import APIConnectionError, APIError, CallTimeouts, ToolCall, UsageError, Wakeshift
from wakeshift import _breaker as breaker_module

from .helpers import FakeCore, Logged, app_key, fake_wake
from .test_retry import gfe500
from .test_toolcall import Wrapped, call_tool, texts

FAST = CallTimeouts(subscribe=0.2, block=0.2)

SKIPPED = (
    "wakeshift: can't reach Wakeshift at https://core.test (skipped: 5 requests in a row failed inside tool calls, so "
    "they pause for 30 seconds). Check that WAKESHIFT_CORE, or the core option, is the address of the console your key "
    "came from. https://wakeshift.ai/docs/errors#unreachable"
)


class Hang:
    """A transport that never answers the paths in hung (until release is set),
    and passes everything else to the core's handler."""

    def __init__(self, core: Any, *paths: str) -> None:
        self.core = core
        self.hung = set(paths)
        self.held: list[str] = []
        self.release = asyncio.Event()

    async def handler(self, request: httpx2.Request) -> httpx2.Response:
        path = request.url.path.removeprefix("/api/apps/v1")
        if path in self.hung:
            self.held.append(path)
            await self.release.wait()
        out = self.core.handler(request)
        return await out if asyncio.iscoroutine(out) else out


def hang(w: Wrapped, *paths: str) -> Hang:
    h = Hang(w.core, *paths)
    w.wake._http = httpx2.AsyncClient(transport=httpx2.MockTransport(h.handler))
    return h


# ── The budget ────────────────────────────────────────────────────────────


def test_call_timeouts_defaults_and_checks() -> None:
    assert CallTimeouts() == CallTimeouts(subscribe=5.0, block=1.0)
    assert Wakeshift(app_key(), core="https://core.test")._call_timeouts == CallTimeouts()
    for bad in (
        CallTimeouts(subscribe=0),
        CallTimeouts(block=-1),
        CallTimeouts(subscribe=float("inf")),
        CallTimeouts(block=float("nan")),
        CallTimeouts(subscribe=True),
        CallTimeouts(block="1"),  # type: ignore[arg-type]
        {"subscribe": 1.0},
    ):
        with pytest.raises(UsageError) as e:
            Wakeshift(app_key(), core="https://core.test", call_timeouts=bad)  # type: ignore[arg-type]
        assert e.value.code == "config", bad
    Wakeshift(app_key(), core="https://core.test", call_timeouts=CallTimeouts(subscribe=1, block=0.5))


async def test_a_hanging_subscribe_returns_within_the_budget() -> None:
    w = Wrapped(call_timeouts=FAST)
    h = hang(w, "/event-subscriptions")
    caught: list[str] = []

    @w.attached.tool(subscribes=[w.shared])
    async def join(table: str, call: ToolCall) -> str:
        try:
            await call.subscribe(w.shared, {"table": table})
        except APIConnectionError as err:
            caught.append(err.code)
        return "Seated."

    start = time.monotonic()
    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "join", {"table": "oak"})
    elapsed = time.monotonic() - start
    h.release.set()
    assert elapsed < 2, elapsed  # the client timeout is 20 s, with 2 retries
    assert caught == ["timeout"]
    assert texts(res) == ["Seated."] and not res["result"].get("isError")
    assert h.held == ["/event-subscriptions"]


async def test_a_hanging_addition_leaves_the_result_unchanged_and_rolls_back() -> None:
    w = Wrapped(call_timeouts=FAST)
    h = hang(w, "/calls/addition")

    @w.attached.tool(subscribes=[w.shared])
    async def join(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return "Seated."

    start = time.monotonic()
    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "join", {"table": "oak"})
    elapsed = time.monotonic() - start
    h.release.set()
    assert elapsed < 2, elapsed
    assert res["result"] == {"content": [{"type": "text", "text": "Seated."}], "isError": False}, res
    ends = w.core.bodies("/event-subscriptions/unsubscribe")
    assert len(ends) == 1 and ends[0]["reason"] == "rolled_back"
    assert [getattr(r, "wakeshift_error_type", "") for r in w.warnings.records] == ["APIConnectionError"]
    assert w.warnings.codes() == ["block_failed"]


async def test_a_hanging_resolve_session_returns_within_the_budget() -> None:
    w = Wrapped(call_timeouts=FAST)
    h = hang(w, "/calls/session")
    caught: list[str] = []

    @w.attached.tool(subscribes=[w.shared])
    async def where(call: ToolCall) -> str:
        try:
            await call.resolve_session()
        except APIConnectionError as err:
            caught.append(err.code)
        return "Here."

    start = time.monotonic()
    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "where", {"session_key": "wsc_s1"})
    h.release.set()
    assert time.monotonic() - start < 2
    assert caught == ["timeout"] and texts(res)[0].startswith("Here.")


async def test_the_budget_includes_the_manifest_send_which_still_finishes() -> None:
    w = Wrapped(call_timeouts=FAST)
    h = hang(w, "/manifest/merge")
    caught: list[str] = []

    @w.attached.tool(subscribes=[w.shared])
    async def join(table: str, call: ToolCall) -> str:
        try:
            await call.subscribe(w.shared, {"table": table})
        except APIConnectionError as err:
            caught.append(err.code)
        return "Seated."

    start = time.monotonic()
    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "join", {"table": "oak"})
    assert time.monotonic() - start < 2
    assert caught == ["timeout"] and texts(res) == ["Seated."]
    assert "/event-subscriptions" not in w.core.paths()
    # The send goes on in the background, and Wakeshift gets the manifest.
    assert not w.wake._published
    h.release.set()
    for _ in range(100):
        if w.wake._published:
            break
        await asyncio.sleep(0.01)
    assert w.wake._published and not w.wake._manifest_tasks
    # The call that gave up sends nothing once the manifest is in.
    await asyncio.sleep(0.05)
    assert "/event-subscriptions" not in w.core.paths()


def native_join(w: Wrapped) -> None:
    @w.attached.tool(subscribes=[w.turn])
    async def join(table: str, call: ToolCall) -> str:
        await call.subscribe(w.turn, {"table": table})  # personal: open to a signed-in native client
        return "Seated."


async def test_a_cold_first_native_call_waits_for_the_manifest_within_the_subscribe_budget() -> None:
    # A native call makes no subscribe request, so on a fresh process its
    # addition is the first to wait for the manifest: that wait gets the
    # subscribe budget, not the block budget, so the line still goes out.
    w = Wrapped(is_native_client=lambda r: True, call_timeouts=CallTimeouts(subscribe=2.0, block=0.1))
    w.core.addition = {"text": "native line"}
    h = hang(w, "/manifest/merge")
    native_join(w)

    async def release() -> None:
        await asyncio.sleep(0.3)
        h.release.set()

    async with w.served() as (_, rpc):
        task = asyncio.ensure_future(release())
        start = time.monotonic()
        res = await call_tool(rpc, "join", {"table": "oak"})
        took = time.monotonic() - start
        await task
    assert took < 2, took
    assert w.core.paths().count("/calls/addition") == 1
    assert w.core.bodies("/calls/addition")[0].get("native")
    assert any("native line" in t for t in texts(res)), texts(res)


async def test_a_native_calls_manifest_wait_is_bounded_by_the_subscribe_budget() -> None:
    w = Wrapped(is_native_client=lambda r: True, call_timeouts=CallTimeouts(subscribe=0.2, block=0.1))
    h = hang(w, "/manifest/merge")
    native_join(w)
    start = time.monotonic()
    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "join", {"table": "oak"})
        took = time.monotonic() - start
        h.release.set()
    assert took < 1, took
    assert texts(res)[0] == "Seated."
    assert "/calls/addition" not in w.core.paths()


# ── No retries in a tool call; retries elsewhere ──────────────────────────


@pytest.mark.parametrize("fail", ["connect", "refused"])
async def test_in_call_requests_are_sent_once(fail: str) -> None:
    sent: list[str] = []

    def handler(r: httpx2.Request) -> httpx2.Response:
        path = r.url.path.removeprefix("/api/apps/v1")
        sent.append(path)
        if fail == "connect":
            raise httpx2.ConnectError("connection refused", request=r)
        return gfe500()

    wake = Wakeshift(app_key(), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)))
    wake._retry_pauses = (0.001, 0.001)
    with pytest.raises(APIError) as e:
        await wake._in_call("subscribe", "POST", "/event-subscriptions", {"name": "x"})
    assert e.value.code == ("unreachable" if fail == "connect" else "")
    assert sent == ["/event-subscriptions"]
    # An emit is sent again, as before.
    sent.clear()
    with pytest.raises(APIError):
        await wake._request("POST", "/events/emit", {"name": "x"})
    assert sent == ["/events/emit"] * 3


# ── The circuit breaker ───────────────────────────────────────────────────


def breaker_wake(core: FakeCore | None = None) -> tuple[Wakeshift, FakeCore, list[float]]:
    wake, core = fake_wake(core or FakeCore())
    clock = [100.0]
    wake._breaker.clock = lambda: clock[0]
    wake._log.clock = lambda: clock[0]
    wake._retry_pauses = (0.001, 0.001)
    return wake, core, clock


async def sub(wake: Wakeshift) -> dict[str, Any]:
    return await wake._in_call("subscribe", "POST", "/event-subscriptions", {"name": "x"})


def answer(status: list[int]) -> Any:
    return lambda b: (status[0], {"error": {"code": "unavailable", "message": "down"}} if status[0] >= 300 else {"ok": True})


async def test_the_breaker_opens_skips_probes_and_closes() -> None:
    wake, core, clock = breaker_wake()
    logged = Logged(logging.INFO)
    status = [503]
    core.on("POST", "/event-subscriptions", answer(status))
    for _ in range(5):
        with pytest.raises(APIError) as e:
            await sub(wake)
        assert e.value.status == 503
    assert len(core.bodies("/event-subscriptions")) == 5
    # Open: skipped at once, with no request, logged once (rate-limited).
    for _ in range(3):
        with pytest.raises(APIConnectionError) as c:
            await sub(wake)
        assert c.value.code == "unreachable" and str(c.value) == SKIPPED
    assert len(core.bodies("/event-subscriptions")) == 5
    assert logged.codes() == ["core_unavailable"]
    assert logged.records[0].levelno == logging.WARNING and logged.records[0].wakeshift_count == 1  # type: ignore[attr-defined]
    assert logged[0] == (
        "wakeshift: Wakeshift failed 5 times in a row inside tool calls, so tool calls skip it for 30 seconds. "
        "Nothing to change; tool calls go on without Wakeshift (no wakes, results unchanged) until it answers again."
    )
    # After the cool-down a probe goes out; it fails, so the breaker opens again.
    clock[0] += 29
    with pytest.raises(APIConnectionError):
        await sub(wake)
    clock[0] += 1
    with pytest.raises(APIError) as e:
        await sub(wake)
    assert e.value.status == 503 and len(core.bodies("/event-subscriptions")) == 6
    with pytest.raises(APIConnectionError) as c:
        await sub(wake)
    assert str(c.value) == SKIPPED and len(core.bodies("/event-subscriptions")) == 6
    # A successful probe closes it, and the count starts again.
    clock[0] += 30
    status[0] = 200
    assert await sub(wake) == {"ok": True}
    status[0] = 503
    for _ in range(4):
        with pytest.raises(APIError):
            await sub(wake)
    status[0] = 200
    assert await sub(wake) == {"ok": True}  # four failures don't open it; a success resets the count
    status[0] = 503
    for _ in range(4):
        with pytest.raises(APIError):
            await sub(wake)
    assert len(core.bodies("/event-subscriptions")) == 16


async def test_the_constants_set_the_breaker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(breaker_module, "BREAKER_FAILURES", 2)
    monkeypatch.setattr(breaker_module, "BREAKER_PAUSE", 5.0)
    wake, core, clock = breaker_wake()
    core.on("POST", "/event-subscriptions", answer([500]))
    for _ in range(2):
        with pytest.raises(APIError):
            await sub(wake)
    with pytest.raises(APIConnectionError) as c:
        await sub(wake)
    assert "skipped: 2 requests in a row failed inside tool calls, so they pause for 5 seconds" in str(c.value)
    clock[0] += 5
    with pytest.raises(APIError) as e:
        await sub(wake)
    assert e.value.status == 500


@pytest.mark.parametrize("fail", ["connect", "timeout"])
async def test_unreachable_and_timeouts_count(fail: str) -> None:
    sent: list[str] = []

    async def handler(r: httpx2.Request) -> httpx2.Response:
        sent.append(r.url.path)
        if fail == "connect":
            raise httpx2.ConnectError("connection refused", request=r)
        await asyncio.sleep(10)
        return httpx2.Response(200, json={})

    wake = Wakeshift(
        app_key(),
        core="https://core.test",
        call_timeouts=CallTimeouts(subscribe=0.05, block=0.05),
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)),
    )
    for _ in range(5):
        with pytest.raises(APIConnectionError) as c:
            await wake._in_call("block", "POST", "/calls/addition", {})
        assert c.value.code == ("unreachable" if fail == "connect" else "timeout")
    with pytest.raises(APIConnectionError) as c:
        await wake._in_call("block", "POST", "/calls/addition", {})
    assert str(c.value) == SKIPPED and len(sent) == 5


async def test_4xx_answers_dont_trip_it() -> None:
    wake, core, _ = breaker_wake()
    core.on("POST", "/event-subscriptions", lambda b: (403, {"error": {"code": "forbidden", "message": "no"}}))
    for _ in range(12):
        with pytest.raises(APIError) as e:
            await sub(wake)
        assert e.value.status == 403
    core.on("POST", "/event-subscriptions", lambda b: (400, {"error": {"code": "bad_arguments", "message": "no"}}))
    for _ in range(6):
        with pytest.raises(APIError):
            await sub(wake)
    assert len(core.bodies("/event-subscriptions")) == 18


async def test_one_probe_at_a_time_and_a_canceled_probe_frees_the_slot() -> None:
    wake, core, clock = breaker_wake()
    status = [503]
    core.on("POST", "/event-subscriptions", answer(status))
    for _ in range(5):
        with pytest.raises(APIError):
            await sub(wake)
    clock[0] += 30
    gate = asyncio.Event()
    entered = asyncio.Event()

    async def slow(r: httpx2.Request) -> httpx2.Response:
        entered.set()
        await gate.wait()
        return core.handler(r)

    wake._http = httpx2.AsyncClient(transport=httpx2.MockTransport(slow))
    wake._call_timeouts = CallTimeouts(subscribe=30, block=30)
    probe = asyncio.create_task(sub(wake))
    await asyncio.wait_for(entered.wait(), 5)
    with pytest.raises(APIConnectionError) as c:
        await sub(wake)  # skipped while the probe is in flight
    assert str(c.value) == SKIPPED
    probe.cancel()
    with pytest.raises(asyncio.CancelledError):
        await probe
    # The next request probes, and its success closes the breaker.
    status[0] = 200
    gate.set()
    assert await sub(wake) == {"ok": True}
    assert await sub(wake) == {"ok": True}


async def test_an_emit_still_retries_and_goes_out_while_the_breaker_is_open() -> None:
    answers: list[str] = []

    def handler(r: httpx2.Request) -> httpx2.Response:
        path = r.url.path.removeprefix("/api/apps/v1")
        answers.append(path)
        if path == "/event-subscriptions":
            return httpx2.Response(503, json={"error": {"code": "unavailable", "message": "down"}})
        if path == "/events/emit":
            if answers.count("/events/emit") == 1:
                return gfe500()  # refused by the load balancer: sent again
            return httpx2.Response(200, json={"matched": 1, **json.loads(r.content)})
        return httpx2.Response(200, json={})

    wake = Wakeshift(app_key(), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)))
    wake._retry_pauses = (0.001, 0.001)
    ev = wake.define_event("table.news", description="News", args={"type": "object"}, payload={"type": "object"})
    for _ in range(5):
        with pytest.raises(APIError):
            await sub(wake)
    with pytest.raises(APIConnectionError):
        await sub(wake)
    out = await ev.emit({}, {})
    assert out.matched == 1
    assert answers.count("/events/emit") == 2
