"""call.call_limit and call.should_skip_wait: a tool asked for a long explicit
wait skips it only when Wakeshift wakes the conversation and the program
would cut the call off before the wait ends (skip-wait.json, shared with the
TypeScript and Go SDKs)."""

from __future__ import annotations

from typing import Any

import pytest

from wakeshift import ToolCall, Wakeshift
from wakeshift.testing import TestClient, tool_call

from .helpers import golden_file
from .test_toolcall import Wrapped, call_tool
from .test_wakes import Done, Job

CASES = golden_file("skip-wait.json")["cases"]


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
async def test_skip_wait_fixture(case: dict[str, Any]) -> None:
    wake = TestClient()
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Done)
    call = tool_call(user="u1", wakes=case["wakes"], call_limit=case["call_limit"], wake=wake)
    assert call.call_limit is None  # before a subscribe
    await call.subscribe(ev, {"job": "j1"})
    assert call.call_limit == case["call_limit"]
    assert call.should_skip_wait(case["seconds"]) is case["skip"]


async def test_a_native_stand_in_has_no_limit() -> None:
    wake = TestClient()
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Done)
    call = tool_call(user="u1", native=True, call_limit=60, wake=wake)
    await call.subscribe(ev, {"job": "j1"})
    assert call.wakes is True
    assert call.call_limit is None
    assert call.should_skip_wait(300) is False


@pytest.mark.parametrize(
    ("setup", "limit", "skip"),
    [
        ({"call_limit": 60, "wakes": True}, 60.0, True),
        ({"call_limit": 300, "wakes": True}, 300.0, True),
        ({"call_limit": 600, "wakes": True}, 600.0, False),
        # The user's notify choice pending: the core answers wakes false.
        ({"call_limit": 60, "wakes": False}, 60.0, False),
        # Left out: a program whose limit isn't known.
        ({"wakes": True}, None, False),
        ({"call_limit": "60", "wakes": True}, None, False),
        ({"call_limit": True, "wakes": True}, None, False),
        ({"call_limit": -5, "wakes": True}, None, False),
    ],
)
async def test_the_cores_call_limit_sets_it(setup: dict[str, Any], limit: float | None, skip: bool) -> None:
    w = Wrapped()
    for k, v in setup.items():
        setattr(w.core, k, v)
    seen: list[Any] = []

    @w.attached.tool(subscribes=[w.shared])
    async def render(table: str, call: ToolCall) -> str:
        seen.append(call.call_limit)
        await call.subscribe(w.shared, {"table": table})
        seen.append(call.call_limit)
        seen.append(call.should_skip_wait(300))
        return "ok"

    async with w.served() as (_, rpc):
        await call_tool(rpc, "render", {"table": "oak"})
    assert seen == [None, limit, skip]


async def test_a_disabled_clients_call_never_skips() -> None:
    off = Wakeshift("")
    ev = off.define_event("job.done", description="A job finished", args=Job, payload=Done)
    call = off._disabled_call("start", [ev])
    await call.subscribe(ev, {"job": "j1"})
    assert call.call_limit is None
    assert call.should_skip_wait(300) is False
