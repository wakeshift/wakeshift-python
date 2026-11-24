"""call.wakes: True only when, in this call, a subscribe made or renewed a
subscription still live and the core answered wakes: true; False otherwise,
so an integration keeps its own polling or waiting. The testing module's
stand-in call too."""

from __future__ import annotations

from typing import Any

import pytest
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import APIError, ToolCall, UsageError, Wakeshift
from wakeshift import _breaker as breaker_module
from wakeshift.testing import TestClient, tool_call

from .helpers import served
from .test_toolcall import Wrapped, call_tool


class Job(BaseModel):
    job: str


class Done(BaseModel):
    ok: bool


def join_tool(w: Wrapped, seen: list[Any], *, unsubscribe: bool = False, personal: bool = False) -> None:
    ev = w.turn if personal else w.shared

    @w.attached.tool(subscribes=[w.turn, w.shared])
    async def join(table: str, call: ToolCall) -> str:
        seen.append(call.wakes)
        try:
            await call.subscribe(ev, {"table": table})
        except APIError as e:
            seen.append(e.status or e.code)
        seen.append(call.wakes)
        if unsubscribe:
            await call.unsubscribe(ev, {"table": table})
            seen.append(call.wakes)
        return "Seated."


@pytest.mark.parametrize(("answer", "want"), [(True, True), (False, False), (None, False), ("true", False), (1, False)])
async def test_the_cores_answer_sets_it(answer: Any, want: bool) -> None:
    # None: the field left out, as an older core does; anything not a bool is False.
    w = Wrapped()
    w.core.wakes = answer
    seen: list[Any] = []
    join_tool(w, seen)
    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {"table": "oak"})
    assert seen == [False, want]


async def test_pending_notify_choice_is_false_until_a_later_call_is_confirmed() -> None:
    # Claude Code's first call: the result asks the user how to be notified,
    # so the core answers wakes: false though the subscription holds. Once the
    # user's pick runs, a later call's subscribe answers true.
    w = Wrapped()
    seen: list[Any] = []
    join_tool(w, seen)
    async with w.served() as (_, rpc):
        w.core.wakes = False
        await call_tool(rpc, "join", {"table": "oak"})
        w.core.wakes = True
        await call_tool(rpc, "join", {"table": "oak"})
    assert seen == [False, False, False, True]


async def test_a_subscribe_that_raised_leaves_it_false() -> None:
    w = Wrapped()
    w.core.wakes = True
    w.core.fail.add("/event-subscriptions")
    seen: list[Any] = []
    join_tool(w, seen)
    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {"table": "oak"})
    assert seen == [False, 503, False]


async def test_any_failed_subscribe_makes_it_false_even_after_one_that_succeeded() -> None:
    w = Wrapped()
    w.core.wakes = True
    seen: list[Any] = []

    @w.attached.tool(subscribes=[w.turn, w.shared])
    async def both(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        seen.append(call.wakes)
        try:
            await call.subscribe(w.turn, {"table": table})  # personal, and no user: refused
        except UsageError:
            pass
        seen.append(call.wakes)
        return "ok"

    async with w.served() as (_, rpc):
        await call_tool(rpc, "both", {"table": "oak"}, user="")
    assert seen == [True, False]


async def test_the_breaker_open_leaves_it_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(breaker_module, "BREAKER_FAILURES", 1)
    w = Wrapped()
    w.core.wakes = True
    w.core.fail.add("/event-subscriptions")
    seen: list[Any] = []
    join_tool(w, seen)
    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {"table": "oak"})  # opens the breaker
        w.core.fail.clear()
        w.core.reset()
        seen.clear()
        await call_tool(rpc, "join", {"table": "oak"})
    assert seen == [False, "unreachable", False]
    assert w.core.bodies("/event-subscriptions") == []


async def test_unsubscribing_what_it_subscribed_makes_it_false() -> None:
    w = Wrapped()
    w.core.wakes = True
    seen: list[Any] = []
    join_tool(w, seen, unsubscribe=True)
    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {"table": "oak"})
    assert seen == [False, True, False]


@pytest.mark.parametrize(
    ("user", "unsubscribe", "want"),
    [
        ("ada", False, [False, True]),  # a native line offered: woken through MCP Events
        ("", False, [False, False]),  # unavailable (no_user): no line, no wake
        ("ada", True, [False, True, False]),  # the line unsubscribed again in the call
    ],
)
async def test_a_native_caller_is_woken_by_its_line(user: str, unsubscribe: bool, want: list[Any]) -> None:
    w = Wrapped(is_native_client=lambda r: True)
    w.core.wakes = False  # never asked: a native subscribe makes no conversation subscription
    w.core.addition = {"text": "native"}
    seen: list[Any] = []
    join_tool(w, seen, unsubscribe=unsubscribe, personal=True)  # personal: open to a signed-in native client
    async with w.served() as (_, rpc):
        await call_tool(rpc, "join", {"table": "oak"}, user=user)
    assert seen == want
    assert w.core.bodies("/event-subscriptions") == []
    # A line unsubscribed again in the call isn't offered in the result.
    if unsubscribe:
        assert all(not b.get("native") for b in w.core.bodies("/calls/addition"))


async def test_a_disabled_clients_call_is_false() -> None:
    off = Wakeshift("")
    ev = off.define_event("job.done", description="A job finished", args=Job, payload=Done)
    call = off._disabled_call("start", [ev])
    assert (await call.subscribe(ev, {"job": "j1"})).state == "disabled"
    assert call.wakes is False


async def test_the_stand_in_call() -> None:
    wake = TestClient()
    ev = wake.define_event("job.done", description="A job finished", args=Job, payload=Done)
    call = tool_call(wake=wake)
    assert call.wakes is False
    await call.subscribe(ev, {"job": "j1"})
    assert call.wakes is True
    await call.unsubscribe(ev, {"job": "j1"})
    assert call.wakes is False
    # wakes=False: a conversation Wakeshift can't wake, for the tool's fallback.
    off = tool_call(wakes=False)
    await off.subscribe(ev, {"job": "j1"})
    assert off.wakes is False
    native = tool_call(user="u1", native=True)
    assert native.wakes is False
    await native.subscribe(ev, {"job": "j1"})
    assert native.wakes is True
    await native.unsubscribe(ev, {"job": "j1"})
    assert native.wakes is False
    native_off = tool_call(user="u1", native=True, wakes=False)
    await native_off.subscribe(ev, {"job": "j1"})
    assert native_off.wakes is False
    failed = tool_call()
    failed.fail_next("subscribe", "unreachable")
    with pytest.raises(APIError):
        await failed.subscribe(ev, {"job": "j1"})
    assert failed.wakes is False


async def test_the_test_clients_core_answers_wakes() -> None:
    wake = TestClient()
    ev = wake.define_event("job.done", description="A job finished", args=Job, payload=Done)
    server = MCPServer("jobs")
    attached = wake.attach(server)
    seen: list[bool] = []

    @attached.tool(subscribes=[ev])
    async def start(call: ToolCall) -> str:
        """Start a job."""
        await call.subscribe(ev, {"job": "j1"})
        seen.append(call.wakes)
        return "started"

    async with served(server) as (_, rpc):
        res = await rpc("tools/call", {"name": "start", "arguments": {}}, headers={"User-Agent": "claude-code/2.1.280"})
    assert "result" in res, res
    assert seen == [True]
