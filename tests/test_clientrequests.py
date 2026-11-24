"""Client requests (_clientrequests.py): the
shared vectors (client-requests.json), then the form end to end with an MCP
client that answers it, against a fake core."""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import re
from collections.abc import Awaitable, Callable
from typing import Any

import httpx2
import mcp_types as types
import pytest
from mcp import Client
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import ToolCall, UsageError, Wakeshift
from wakeshift import _clientrequests as cr

from .conftest import FASTMCP
from .helpers import FakeCore, Logged, app_key, golden_file, initialize, served

VECTORS = golden_file("client-requests.json")


def expand(v: Any) -> Any:
    """v with {N x} and {N lines} turned into their strings."""
    if isinstance(v, str):
        if m := re.fullmatch(r"\{(\d+) x\}", v):
            return "x" * int(m.group(1))
        if m := re.fullmatch(r"\{(\d+) lines\}", v):
            return "\n".join(["x"] * int(m.group(1)))
        return v
    if isinstance(v, list):
        return [expand(x) for x in v]
    if isinstance(v, dict):
        return {k: expand(x) for k, x in v.items()}
    return v


def request_of(vec: dict[str, Any]) -> dict[str, Any]:
    if "request" in vec:
        return expand(vec["request"])
    r = copy.deepcopy(VECTORS["validate"][vec["base"]]["request"])
    r.update(expand(vec.get("patch") or {}))
    if "drop_outcome" in vec:
        del r["outcomes"][vec["drop_outcome"]]
    if "patch_outcome" in vec:
        rest = dict(vec["patch_outcome"])
        r["outcomes"][rest.pop("key")].update(expand(rest))
    return r


def test_the_bounds_are_the_shared_ones() -> None:
    assert dataclasses.asdict(cr.BOUNDS) == VECTORS["bounds"]
    assert list(cr.OUTCOME_KEYS) == VECTORS["outcome_keys"]
    assert cr.request_prefix("Shipyard") == VECTORS["prefix"].replace("{app}", "Shipyard")
    assert cr.request_prefix("") == "Wakeshift, for an app:"
    assert cr.request_prefix("  " + "y" * 100) == "Wakeshift, for " + "y" * 80 + ":"


@pytest.mark.parametrize("vec", VECTORS["validate"], ids=lambda v: v["name"])
def test_validate(vec: dict[str, Any]) -> None:
    assert cr.validate_request(request_of(vec), vec["app"]) == vec["want"]


@pytest.mark.parametrize("vec", VECTORS["outcome"], ids=lambda v: v["name"])
def test_outcome(vec: dict[str, Any]) -> None:
    r = request_of(VECTORS["validate"][vec["base"]])
    assert cr.outcome_key(vec["result"], r) == vec["want"]


@pytest.mark.parametrize("vec", VECTORS["offer"], ids=lambda v: v["name"])
def test_offer(vec: dict[str, Any]) -> None:
    got = cr.offers_form(
        vec["capabilities"],
        vec["protocol"],
        vec["http"],
        vec["session"],
        off=vec.get("off", False),
        recent=vec.get("recent", False),
    )
    assert got == vec["want"]


def test_client_requests_is_true_or_false() -> None:
    wake = Wakeshift(app_key("shipyard"), core="https://core.test")
    with pytest.raises(UsageError, match="client_requests is True or False"):
        wake.attach(MCPServer("shipyard"), client_requests="no")  # pyright: ignore[reportArgumentType]


# ── End to end ───────────────────────────────────────────────────────────

TOKEN = VECTORS["validate"][0]["request"]["token"]


class N(BaseModel):
    n: int


class Done(BaseModel):
    ok: bool


def form() -> dict[str, Any]:
    return copy.deepcopy(VECTORS["validate"][0]["request"])


class Shipyard:
    """An MCPServer with a tool that subscribes, attached to a fake core whose
    addition carries request when the call offers elicit.form."""

    def __init__(self, request: Any, **attach: Any) -> None:
        cr.reset_recent_forms()
        self.core = FakeCore(default_ok=True)
        self.core.on(
            "POST",
            "/event-subscriptions",
            lambda b: (200, {"event_subscription": {"id": "sub-1", "created": True}, "session": "wsc_s1", "pending": "pa_1"}),
        )

        def addition(b: Any) -> tuple[int, Any]:
            out: dict[str, Any] = {"text": "Today's text.", "trailer": "", "meta": {"wakeshift/connector": "beacon-1"}}
            if "elicit.form" in ((b or {}).get("call") or {}).get("client_requests", []):
                out["requests"] = [request]
            return 200, out

        self.core.on("POST", "/calls/addition", addition)
        self.core.on("GET", "/app", lambda b: (200, {"id": "shipyard", "name": "Shipyard"}))
        self.core.on("POST", "/requests/outcome", lambda b: (200, {"recorded": True}))
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(self.core.handler))
        self.wake = Wakeshift(app_key("shipyard"), core="https://core.test", http_client=http)
        build = self.wake.define_event("build.finished", description="A build finished", args=N, payload=Done)
        self.server = MCPServer("shipyard")
        self.attached = self.wake.attach(self.server, **attach)

        @self.attached.tool(subscribes=[build])
        async def watch_build(n: int, call: ToolCall) -> str:
            """Watch a build."""
            await call.subscribe(build, {"n": n})
            return "Watching."

    def addition(self) -> Any:
        return self.core.bodies("/calls/addition")[0]

    def reports(self) -> list[Any]:
        return self.core.bodies("/requests/outcome")


Answer = Callable[[types.ElicitRequestFormParams], Awaitable[types.ElicitResult]]


def in_memory(server: Any) -> Any:
    """What the official Client connects to in memory: the server, or on the
    FastMCP lane (WAKESHIFT_TEST_SERVER=fastmcp) its low-level server."""
    return server._mcp_server if FASTMCP else server


async def run(app: Shipyard, answer: Answer | None) -> tuple[types.CallToolResult, list[types.ElicitRequestFormParams]]:
    """Calls watch_build from an MCP client (in-process, the stdio path) that
    declares elicitation and answers with answer, if given."""
    asked: list[types.ElicitRequestFormParams] = []

    async def elicit(context: Any, params: types.ElicitRequestParams) -> types.ElicitResult:
        assert isinstance(params, types.ElicitRequestFormParams)
        asked.append(params)
        assert answer is not None
        return await answer(params)

    kw: dict[str, Any] = {"elicitation_callback": elicit} if answer else {}
    async with Client(in_memory(app.server), mode="legacy", **kw) as client:
        result = await client.call_tool("watch_build", {"n": 1})
    await app.wake.aclose()
    return result, asked


def texts(result: types.CallToolResult) -> list[str]:
    return [c.text for c in result.content if isinstance(c, types.TextContent)]


def answering(**result: Any) -> Answer:
    async def answer(params: types.ElicitRequestFormParams) -> types.ElicitResult:
        return types.ElicitResult(**result)

    return answer


async def test_accept_puts_the_answers_text_before_and_reports_it() -> None:
    app = Shipyard(form())
    result, asked = await run(app, answering(action="accept", content={"choice": "setup"}))
    assert app.addition()["call"]["client_requests"] == ["elicit.form"]
    assert len(asked) == 1
    assert asked[0].message.startswith("Wakeshift, for Shipyard:\nShipyard sends its notifications"), asked[0].message
    assert asked[0].requested_schema == form()["schema"]
    # The answer's text, placed before the app's (a string result stays one block).
    assert texts(result) == ["Set up.\n\nWatching."]
    assert app.reports() == [{"token": TOKEN, "outcome": "choice=setup"}]
    assert (result.meta or {}).get("wakeshift/connector") == "beacon-1"


async def test_decline_keeps_the_cores_text_and_reports_decline() -> None:
    app = Shipyard(form())
    result, asked = await run(app, answering(action="decline"))
    assert len(asked) == 1
    assert texts(result) == ["Watching.\n\nToday's text."]
    assert app.reports() == [{"token": TOKEN, "outcome": "decline"}]


async def test_a_dropped_beacon_leaves_the_result() -> None:
    r = form()
    r["outcomes"]["choice=later"]["beacon"] = "drop"
    app = Shipyard(r)
    result, _ = await run(app, answering(action="accept", content={"choice": "later"}))
    assert texts(result) == ["Watching.\n\nNot now."]
    assert "wakeshift/connector" not in (result.meta or {})
    assert app.reports() == [{"token": TOKEN, "outcome": "choice=later"}]


async def test_a_request_past_the_bounds_is_never_shown() -> None:
    logged = Logged()
    app = Shipyard({**form(), "kind": "elicit.url"})
    result, asked = await run(app, answering(action="accept", content={"choice": "setup"}))
    assert asked == []
    assert texts(result) == ["Watching.\n\nToday's text."]
    assert app.reports() == [{"token": TOKEN, "outcome": "unsupported"}]
    assert logged.codes() == ["client_request_refused"]
    assert logged.codes() == ["client_request_refused"]
    assert "past this SDK's bounds (kind)" in logged[0]
    assert logged.records[0].wakeshift_reason == "kind"  # pyright: ignore[reportAttributeAccessIssue]


async def test_no_answer_in_time_is_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cr, "BOUNDS", dataclasses.replace(cr.BOUNDS, timeout_ms=50))
    app = Shipyard(form())

    async def never(params: types.ElicitRequestFormParams) -> types.ElicitResult:
        await asyncio.sleep(5)
        return types.ElicitResult(action="accept", content={"choice": "setup"})

    result, asked = await run(app, never)
    assert len(asked) == 1
    assert texts(result) == ["Watching.\n\nToday's text."]
    assert app.reports() == [{"token": TOKEN, "outcome": "timeout"}]


async def test_a_report_that_fails_never_fails_the_result() -> None:
    logged = Logged()
    app = Shipyard(form())
    app.core.on("POST", "/requests/outcome", lambda b: (500, {"error": {"code": "internal", "message": "down"}}))
    result, _ = await run(app, answering(action="accept", content={"choice": "setup"}))
    assert texts(result) == ["Set up.\n\nWatching."]
    assert "client_request_report_failed" in logged.codes()


async def test_one_form_per_session_in_the_window() -> None:
    app = Shipyard(form())
    asked: list[Any] = []

    async def elicit(context: Any, params: types.ElicitRequestParams) -> types.ElicitResult:
        asked.append(params)
        return types.ElicitResult(action="decline")

    async with Client(in_memory(app.server), mode="legacy", elicitation_callback=elicit) as client:
        await client.call_tool("watch_build", {"n": 1})
        await client.call_tool("watch_build", {"n": 2})
    await app.wake.aclose()
    offers = [b["call"].get("client_requests") for b in app.core.bodies("/calls/addition")]
    assert offers == [["elicit.form"], None]
    assert len(asked) == 1


async def test_turned_off_or_not_declared_offers_nothing() -> None:
    off = Shipyard(form(), client_requests=False)
    result, asked = await run(off, answering(action="accept", content={"choice": "setup"}))
    assert "client_requests" not in off.addition()["call"]
    assert asked == []
    assert texts(result) == ["Watching.\n\nToday's text."]
    assert off.reports() == []

    none = Shipyard(form())
    result, _ = await run(none, None)
    assert "client_requests" not in none.addition()["call"]
    assert texts(result) == ["Watching.\n\nToday's text."]


@pytest.mark.parametrize("stateless", [True, False], ids=["stateless", "stateful JSON responses"])
async def test_http_that_cant_carry_the_request_offers_nothing(stateless: bool) -> None:
    app = Shipyard(form())
    async with served(app.server, stateless=stateless) as (_, rpc):
        await initialize(rpc, "codex-mcp-client", {"elicitation": {"form": {}}})
        res = await rpc("tools/call", {"name": "watch_build", "arguments": {"n": 1}})
    await app.wake.aclose()
    assert "result" in res, res
    assert "client_requests" not in app.addition()["call"]
    assert app.reports() == []


async def test_a_request_the_call_didnt_offer_is_refused() -> None:
    # A core that sends a request anyway: never shown, reported unsupported.
    app = Shipyard(form(), client_requests=False)
    app.core.on(
        "POST",
        "/calls/addition",
        lambda b: (200, {"text": "Today's text.", "trailer": "", "meta": {}, "requests": [form()]}),
    )
    logged = Logged()
    result, asked = await run(app, answering(action="accept", content={"choice": "setup"}))
    assert asked == []
    assert texts(result) == ["Watching.\n\nToday's text."]
    assert app.reports() == [{"token": TOKEN, "outcome": "unsupported"}]
    assert logged.codes() == ["client_request_refused"]


async def test_stateful_http_with_streamed_responses_runs_the_form() -> None:
    from .test_push import live

    app = Shipyard(form())
    asked: list[Any] = []

    async def elicit(context: Any, params: types.ElicitRequestParams) -> types.ElicitResult:
        asked.append(params)
        return types.ElicitResult(action="accept", content={"choice": "setup"})

    async with live(app.server, stateless=False) as base:
        async with Client(base + "/mcp", mode="legacy", elicitation_callback=elicit) as client:
            result = await client.call_tool("watch_build", {"n": 1})
    await app.wake.aclose()
    assert app.addition()["call"]["client_requests"] == ["elicit.form"]
    assert len(asked) == 1
    assert texts(result) == ["Set up.\n\nWatching."]
    assert app.reports() == [{"token": TOKEN, "outcome": "choice=setup"}]


async def test_the_client_cancelling_the_call_reports_cancel() -> None:
    import anyio

    app = Shipyard(form())
    shown = anyio.Event()

    async def elicit(context: Any, params: types.ElicitRequestParams) -> types.ElicitResult:
        shown.set()
        await anyio.sleep(30)
        return types.ElicitResult(action="decline")

    async with Client(in_memory(app.server), mode="legacy", elicitation_callback=elicit) as client:
        async with anyio.create_task_group() as tg:

            async def call() -> None:
                with anyio.CancelScope() as scope:
                    tg.start_soon(lambda: _cancel_when(shown, scope))
                    await client.call_tool("watch_build", {"n": 1})

            tg.start_soon(call)
        with anyio.fail_after(5):
            while not app.reports():
                await anyio.sleep(0.01)
    await app.wake.aclose()
    assert app.reports() == [{"token": TOKEN, "outcome": "cancel"}]


async def _cancel_when(event: Any, scope: Any) -> None:
    await event.wait()
    scope.cancel()
