"""Construction, the disabled client and the client's lifecycle (design §2.1,
PR 3): disabled-client.json's construction cases and each method on a
disabled client, construction making no request, and one client shared by
event loops and threads."""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

import httpx2
import pytest
from mcp.server.mcpserver import MCPServer as OfficialMCPServer
from pydantic import BaseModel

from wakeshift import Recipient, ToolCall, UsageError, Wakeshift

from .helpers import FakeCore, Logged, app_key, golden_file, served

FIXTURE = golden_file("disabled-client.json")
KEY = golden_file("app-keys.json")["accepted"][0]["key"]


def no_requests() -> httpx2.AsyncClient:
    """An HTTP client that fails the test if anything uses it."""

    def refuse(r: httpx2.Request) -> httpx2.Response:
        raise AssertionError(f"a request was made: {r.method} {r.url.path}")

    return httpx2.AsyncClient(transport=httpx2.MockTransport(refuse))


@pytest.mark.parametrize("case", FIXTURE["construction"]["cases"], ids=lambda c: c["name"])
def test_construction(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    if (case.get("skip") or {}).get("python"):
        pytest.skip(case["skip"]["python"])
    monkeypatch.delenv("WAKESHIFT_KEY", raising=False)
    monkeypatch.delenv("WAKESHIFT_CORE", raising=False)
    for k, v in case["env"].items():
        monkeypatch.setenv(k, v)
    o = case["options"]
    kw: dict[str, Any] = {"http_client": no_requests()}
    for name, py in (("required", "required"), ("core", "core"), ("max_retries", "max_retries"), ("timeout_seconds", "timeout")):
        if name in o:
            kw[py] = o[name]
    args = (o["key"],) if "key" in o else ()
    logged = Logged(logging.DEBUG)
    want = case["want"]
    if "error" in want:
        with pytest.raises(UsageError) as e:
            Wakeshift(*args, **kw)
        assert e.value.code == want["error"]
        assert want["says"] in str(e.value), str(e.value)
        for bad in want.get("must_not_contain", []):
            assert bad not in str(e.value), (bad, str(e.value))
        return
    wake = Wakeshift(*args, **kw)
    assert wake.enabled is want["enabled"]
    assert wake.core == want["core"]
    got = [{"code": getattr(r, "wakeshift_code", ""), "level": r.levelname.lower()} for r in logged.records]
    assert got == want["logs"]


def test_no_key_is_logged_once_per_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WAKESHIFT_KEY", raising=False)
    logged = Logged(logging.DEBUG)
    Wakeshift()
    Wakeshift()
    assert logged.codes() == ["no_key", "no_key"]
    assert {r.levelname for r in logged.records} == {"DEBUG"}  # nothing at info: stdio stays as before


def test_the_client_never_prints_its_key() -> None:
    wake = Wakeshift(KEY)
    assert KEY not in repr(wake) and KEY not in str(wake)


class Job(BaseModel):
    job: str


class Crawl(BaseModel):
    status: str
    pages: int


class Table(BaseModel):
    table: str


class Turn(BaseModel):
    text: str


#: Ops Python has no counterpart for (design §5's Python surface has no actAs).
NOT_IN_PYTHON = {"act_as": "Python has no act_as in this release"}

#: Ops a later commit of this release implements.
PENDING: set[str] = set()


async def test_each_method_on_a_disabled_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WAKESHIFT_KEY", raising=False)
    wake = Wakeshift(http_client=no_requests())
    crawl = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    turn = wake.define_event("game.turn", description="Your move", args=Table, payload=Turn, personal=True)
    news = wake.define_event("table.news", description="News at a table", args=Table, payload=Turn)
    identified: list[Any] = []
    server = OfficialMCPServer("crawler")
    attached = wake.attach(server, identify=lambda r: identified.append(r))  # type: ignore[arg-type,func-returns-value]
    seen: dict[str, Any] = {}

    @attached.tool(subscribes=[crawl])
    async def start_crawl(url: str, call: ToolCall) -> str:
        """Crawl a site."""
        seen["user"] = call.user
        seen["current_call_in_tool"] = wake.current_call()
        seen["subscribe"] = await call.subscribe(crawl, {"job": "j1"}, once=True)
        seen["unsubscribe"] = await call.unsubscribe(crawl, {"job": "j1"})
        for op, f in (
            ("call_subscribe_bad_arguments", lambda: call.subscribe(crawl, {"job": 7})),
            ("call_subscribe_undeclared", lambda: call.subscribe(news, {"table": "oak"})),
            ("call_resolve_session", lambda: call.resolve_session()),
        ):
            try:
                await f()
            except UsageError as err:
                seen[op] = err.code
        return "started"

    async def expect_error(f: Any) -> str:
        with pytest.raises(UsageError) as e:
            await f()
        return e.value.code

    async with served(server) as (_, rpc):
        tools = (await rpc("tools/list", {}))["result"]["tools"]
        res = (await rpc("tools/call", {"name": "start_crawl", "arguments": {"url": "https://x.example"}}))["result"]

    results: dict[str, Any] = {
        "enabled": {"value": wake.enabled},
        "define_event": {"works": True},
        "attach": {"works": True, "adds": "nothing" if [t["name"] for t in tools] == ["start_crawl"] else tools},
        "register_tool": {
            "works": res["content"][0]["text"] == "started",
            "adds": "nothing" if set(tools[0]["inputSchema"]["properties"]) == {"url"} else tools[0],
        },
        "identify": {"called": bool(identified)},
        "current_call": {"works": True, "call": "disabled" if wake.current_call()._scope is None else "?"},
        "current_call_in_tool": {"works": True, "call": "disabled" if seen["current_call_in_tool"]._scope is None else "?"},
        "call_user": {"value": seen["user"]},
        "call_subscribe_bad_arguments": {"error": seen.get("call_subscribe_bad_arguments")},
        "call_subscribe_undeclared": {"error": seen.get("call_subscribe_undeclared")},
        "call_unsubscribe": {"resolves": {"ended": seen["unsubscribe"]}},
        "call_subscribe": {"resolves": {"state": seen["subscribe"].state}},
        "call_resolve_session": {"error": seen.get("call_resolve_session")},
        "emit": {"resolves": None},
        "emit_with_id": {"resolves": None},
        "emit_bad_payload": {"error": await expect_error(lambda: crawl.emit({"job": "j1"}, {"status": 5}))},
        "emit_personal_without_to": {"error": await expect_error(lambda: turn.emit({"table": "oak"}, {"text": "go"}))},
        "revoke_publish_url": {"error": await expect_error(lambda: wake.revoke_publish_url("pu_1"))},
        "create_publish_url": {"error": await expect_error(lambda: crawl.create_publish_url({"job": "j1"}))},
        "status": {"error": await expect_error(lambda: crawl.status({"job": "j1"}))},
        "status_of": {"error": await expect_error(lambda: crawl.status_of([Recipient(user="u1", subscription="s1")]))},
        "end": {"error": await expect_error(lambda: crawl.end("u1", "s1"))},
        "forget_user": {"error": await expect_error(lambda: wake.forget_user("u1"))},
        "publish_manifest": {"error": await expect_error(lambda: wake.publish_manifest({"description": "x"}))},
        "workspace": {"error": await expect_error(lambda: wake.workspace("u1", "U"))},
        "add_agent": {"error": await expect_error(lambda: wake.add_agent("ws", "a", "A"))},
        "check_agent": {"error": await expect_error(lambda: wake.check_agent("ws", "a"))},
    }
    with pytest.raises(UsageError) as e:
        _ = wake.app_id
    results["app_id"] = {"error": e.value.code}
    for op, kw in (("emit", {}), ("emit_with_id", {"id": "e1"})):
        out = await crawl.emit({"job": "j1"}, {"status": "done", "pages": 3}, **kw)
        results[op] = {
            "resolves": {
                "matched": out.matched,
                "duplicate": out.duplicate,
                "unmatched": out.unmatched,
                "ended": out.ended,
                "textTrimmed": out.text_trimmed,
            }
        }
    for case in FIXTURE["methods"]["cases"]:
        op = case["op"]
        if (case.get("skip") or {}).get("python") or op in NOT_IN_PYTHON or op in PENDING:
            continue
        want = case["want"].get("python", case["want"])
        assert op in results, f"{case['name']}: no op {op} here"
        got = results[op]
        for k, v in want.items():
            assert got.get(k) == v, (op, k, got)


def test_construction_makes_no_request() -> None:
    # The no_requests client would fail the test; constructing, defining and
    # attaching ask nothing.
    wake = Wakeshift(app_key(), core="https://core.test", http_client=no_requests())
    wake.define_event("table.news", description="News at a table", args=Table, payload=Turn)
    wake.attach(OfficialMCPServer("t"))


def _counting_wake() -> tuple[Wakeshift, list[httpx2.AsyncClient]]:
    """A client against a fake core, recording each HTTP client it opens (one per loop)."""
    core = FakeCore(default_ok=True)
    core.on("POST", "/users/forget", lambda b: (200, {"forgotten": True}))
    wake = Wakeshift(app_key(), core="https://core.test")
    made: list[httpx2.AsyncClient] = []

    def new() -> httpx2.AsyncClient:
        c = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
        made.append(c)
        return c

    wake._new_http = new  # type: ignore[method-assign]
    return wake, made


def test_two_asyncio_runs_on_one_client() -> None:
    wake, made = _counting_wake()
    assert asyncio.run(wake.forget_user("ada")) is True
    assert asyncio.run(wake.forget_user("ada")) is True
    # Each run's loop got a client of its own; the first's was dropped with
    # its loop, not reused (it belonged to a closed loop).
    assert len(made) == 2 and made[0] is not made[1]
    assert asyncio.run(wake.forget_user("ada")) is True
    assert len(wake._loops) <= 1, "clients of closed loops are dropped"


_shared: dict[str, Any] = {}


async def test_function_scoped_loops_one() -> None:
    _shared["wake"], _shared["made"] = _counting_wake()
    assert await _shared["wake"].forget_user("ada") is True
    assert await _shared["wake"].forget_user("ada") is True
    assert len(_shared["made"]) == 1, "one client per loop, reused"


async def test_function_scoped_loops_two() -> None:
    # pytest-asyncio gives each test its own loop: the shared client opens a
    # new HTTP client on this one rather than use the last test's.
    assert await _shared["wake"].forget_user("ada") is True
    assert len(_shared["made"]) == 2


def test_two_threads_each_running_asyncio_run() -> None:
    wake, made = _counting_wake()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            assert asyncio.run(wake.forget_user("ada")) is True
        except BaseException as err:  # noqa: BLE001
            errors.append(err)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == [] and len(made) == 2


async def test_aclose_is_idempotent_and_never_closes_a_callers_client() -> None:
    wake, _ = _counting_wake()
    assert await wake.forget_user("ada") is True
    own = wake._loop().http
    assert own is not None
    await wake.aclose()
    assert own.is_closed and wake._loop().http is None
    await wake.aclose()  # idempotent
    # It opens a new one on next use.
    assert await wake.forget_user("ada") is True
    assert wake._loop().http is not None and wake._loop().http is not own

    theirs = httpx2.AsyncClient(transport=httpx2.MockTransport(lambda r: httpx2.Response(200, json={"forgotten": True})))
    async with Wakeshift(app_key(), core="https://core.test", http_client=theirs) as w2:
        assert await w2.forget_user("ada") is True
    assert not theirs.is_closed, "a caller's http_client is never closed"
