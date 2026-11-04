"""@attached.tool(subscribes=[...]): tools/list says a declaring tool is how
the app notifies the agent, from its events' own descriptions (the shared
fixture subscribes.json), and a subscribe the tool doesn't declare warns
once per tool, or, once undeclared subscribes are an error, is refused."""

import time
from typing import Any

import pytest

import wakeshift.client as client
import wakeshift.toolcall as toolcall
from wakeshift import ToolCall, UndeclaredSubscribe, notifies
from wakeshift.events import _with_subscribes

from .helpers import golden_file
from .test_toolcall import Wrapped, call_tool

NEWS = "News at a table"


def sentence(app: str, of: str) -> str:
    return (
        f"This tool is how {app} notifies you in this conversation {of}. {app[:1].upper() + app[1:]} delivers its "
        "notifications through Wakeshift; the result may ask you to start a background listener and to offer the user "
        "a one-time install. Follow it."
    )


def test_shared_fixture() -> None:
    cases = golden_file("subscribes.json")["cases"]
    assert len(cases) >= 8
    for c in cases:
        assert _with_subscribes(c["description"], c["events"], c["app"]) == c["want"], c["name"]


def _app() -> Wrapped:
    w = Wrapped()

    @w.attached.tool(description="Watch a table.", subscribes=[w.shared])
    async def watch(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return "ok"

    @w.attached.tool(description="Join a table.", subscribes=[w.turn, w.shared])
    async def join(table: str, call: ToolCall) -> str:
        return "ok"

    @w.attached.tool(description=notifies("Watch a table.", when="news breaks"), subscribes=[w.shared])
    async def custom(call: ToolCall) -> str:
        return "ok"

    @w.attached.tool(description="Look at a table.", subscribes=[])
    async def nothing(call: ToolCall) -> str:
        return "ok"

    @w.attached.tool(description="Old style.")
    async def old(call: ToolCall) -> str:
        return "ok"

    return w


async def _listed(w: Wrapped, rpc: Any) -> dict[str, str]:
    return {t["name"]: t.get("description") for t in (await rpc("tools/list", {}))["result"]["tools"]}


async def test_tools_list_adds_the_sentence_naming_the_app() -> None:
    w = _app()
    w.core.app_name = "Shipyard"
    async with w.served() as (_, rpc):
        listed = await _listed(w, rpc)
        assert listed["watch"] == "Watch a table. " + sentence("Shipyard", f"of this event: {NEWS}")
        assert listed["join"] == "Join a table. " + sentence("Shipyard", f"of these events: Your turn; {NEWS}")
        assert listed["custom"] == "Watch a table. " + sentence("Shipyard", "when news breaks")
        assert listed["nothing"] == "Look at a table."
        assert listed["old"] == "Old style."
        await _listed(w, rpc)
        assert w.core.paths().count("/app") == 1
        # A declared subscribe doesn't warn.
        res = await call_tool(rpc, "watch", {"table": "oak"})
        assert not res["result"].get("isError"), res
    assert not w.warnings


async def test_tools_list_says_this_app_without_a_name() -> None:
    w = _app()
    async with w.served() as (_, rpc):
        listed = await _listed(w, rpc)
        assert listed["watch"] == "Watch a table. " + sentence("this app", f"of this event: {NEWS}")


async def test_tools_list_waits_only_so_long_for_the_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(client._NAME_FETCH, "wait", 0.1)
    monkeypatch.setitem(client._NAME_FETCH, "retry", 60.0)
    # Wakeshift doesn't answer in time: "this app", and no wait (or ask) again within retry.
    w = _app()
    w.core.app_name = "Shipyard"
    w.core.app_delay = 5.0
    async with w.served() as (_, rpc):
        for _ in range(3):
            t0 = time.monotonic()
            listed = await _listed(w, rpc)
            assert time.monotonic() - t0 < 2.0
            assert listed["watch"] == "Watch a table. " + sentence("this app", f"of this event: {NEWS}")
        assert w.core.paths().count("/app") == 1
    # Wakeshift fails (a 404: no name): asked once within retry, then again after it.
    w = _app()
    async with w.served() as (_, rpc):
        await _listed(w, rpc)
        await _listed(w, rpc)
        assert w.core.paths().count("/app") == 1
        monkeypatch.setitem(client._NAME_FETCH, "retry", 0.0)
        w.core.app_name = "Shipyard"
        listed = await _listed(w, rpc)
        assert listed["watch"] == "Watch a table. " + sentence("Shipyard", f"of this event: {NEWS}")
        assert w.core.paths().count("/app") == 2


def _undeclared() -> Wrapped:
    w = Wrapped()

    @w.attached.tool()
    async def old(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return "ok"

    @w.attached.tool(subscribes=[w.turn])
    async def other(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return "ok"

    @w.attached.tool(subscribes=[])
    async def none(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return "ok"

    @w.attached.tool(subscribes=[w.shared])
    async def fine(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return "ok"

    return w


def _warning(tool: str, what: str, fix: str) -> str:
    return (
        f"wakeshift: the tool {tool} subscribed to table.news{what}, so tools/list doesn't tell the agent that the tool is how "
        f"this app notifies it. Fix: @attached.tool(subscribes=[{fix}]). It works for now; from the next release an "
        "undeclared subscribe raises."
    )


async def test_an_undeclared_subscribe_works_and_warns_once_per_tool() -> None:
    w = _undeclared()
    async with w.served() as (_, rpc):
        for _ in range(2):
            for tool in ("old", "other", "none", "fine"):
                res = await call_tool(rpc, tool, {"table": "oak"})
                assert not res["result"].get("isError"), (tool, res)
    assert len(w.core.bodies("/event-subscriptions")) == 8
    assert w.warnings == [
        _warning("old", " without declaring the events it subscribes to", "<table.news's event>"),
        _warning("other", ", which its subscribes doesn't list (game.turn)", "<game.turn's event>, <table.news's event>"),
        _warning("none", ", but declares that it subscribes to nothing", "<table.news's event>"),
    ]


async def test_an_undeclared_subscribe_is_refused_in_error_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(toolcall, "_UNDECLARED_IS_ERROR", True)
    w = _undeclared()
    async with w.served() as (_, rpc):
        res = await call_tool(rpc, "old", {"table": "oak"})
        # The tool fails (MCPServer and FastMCP show their own text for an
        # exception that isn't a ToolError; the exception says the fix).
        assert "error" in res or res["result"].get("isError"), res
        assert w.core.bodies("/event-subscriptions") == []
        res = await call_tool(rpc, "fine", {"table": "oak"})
        assert not res["result"].get("isError"), res
    assert not w.warnings


async def test_the_error_is_undeclared_subscribe() -> None:
    w = Wrapped()
    got: list[BaseException] = []

    @w.attached.tool()
    async def old(table: str, call: ToolCall) -> str:
        toolcall._UNDECLARED_IS_ERROR = True
        try:
            await call.subscribe(w.shared, {"table": table})
        except UndeclaredSubscribe as err:
            got.append(err)
        finally:
            toolcall._UNDECLARED_IS_ERROR = False
        return "ok"

    async with w.served() as (_, rpc):
        await call_tool(rpc, "old", {"table": "oak"})
    assert [str(e) for e in got] == [
        "wakeshift: the tool old subscribed to table.news without declaring the events it subscribes to, so the "
        "subscribe was refused. Fix: @attached.tool(subscribes=[<table.news's event>])."
    ]


def test_subscribes_takes_only_this_apps_events_as_a_list() -> None:
    w = Wrapped()
    theirs = Wrapped()
    with pytest.raises(TypeError, match="isn't one of this app's events"):
        w.attached.tool(subscribes=[theirs.shared])
    with pytest.raises(TypeError, match="lists the events the tool subscribes to"):
        w.attached.tool(subscribes=w.shared)  # type: ignore[arg-type]
