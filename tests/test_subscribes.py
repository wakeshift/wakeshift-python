"""@attached.tool(subscribes=[...]): tools/list adds a sentence to a
declaring tool's description saying the tool is how the app notifies the
agent, from its events' own descriptions (the shared fixture subscribes.json;
the compact bench kept it as it was), and a subscribe the tool doesn't
declare is refused."""

import time
from typing import Any

import pytest

import wakeshift._client as client
from wakeshift import ToolCall, UsageError
from wakeshift._events import _with_subscribes
from wakeshift._text import COMPACT

from .helpers import golden_file
from .test_toolcall import Wrapped, call_tool

NEWS = "News at a table"


def sentence(app: str, events: str) -> str:
    of = f"of these events: {events}" if "; " in events else f"of this event: {events}"
    return (
        f"This tool is how {app} notifies you in this conversation {of}. {app[:1].upper() + app[1:]} delivers its "
        "notifications through Wakeshift; the result may ask you to start a background listener and to offer the user "
        "a one-time install. Follow it."
    )


def test_shared_fixture() -> None:
    cases = golden_file("subscribes.json")["compact_cases"]
    assert len(cases) >= 8
    for c in cases:
        assert _with_subscribes(c["description"], c["events"], c["app"]) == c["want"], c["name"]


def test_compact_strings_come_from_one_place() -> None:
    """COMPACT is subscribing-tools-list.json's compact_strings, key for key."""
    want = {k: v for k, v in golden_file("subscribing-tools-list.json")["compact_strings"].items() if not k.startswith("_")}
    assert COMPACT == want


def _app() -> Wrapped:
    w = Wrapped()

    @w.attached.tool(description="Watch a table.", subscribes=[w.shared])
    async def watch(table: str, call: ToolCall) -> str:
        await call.subscribe(w.shared, {"table": table})
        return "ok"

    @w.attached.tool(description="Join a table.", subscribes=[w.turn, w.shared])
    async def join(table: str, call: ToolCall) -> str:
        return "ok"

    @w.attached.tool(description="Look at a table.", subscribes=[])
    async def nothing(call: ToolCall) -> str:
        return "ok"

    return w


async def _listed(w: Wrapped, rpc: Any) -> dict[str, str]:
    return {t["name"]: t.get("description") for t in (await rpc("tools/list", {}))["result"]["tools"]}


async def test_tools_list_adds_the_sentence_naming_the_app() -> None:
    w = _app()
    w.core.app_name = "Shipyard"
    async with w.served() as (_, rpc):
        listed = await _listed(w, rpc)
        assert listed["watch"] == "Watch a table. " + sentence("Shipyard", NEWS)
        assert listed["join"] == "Join a table. " + sentence("Shipyard", f"Your turn; {NEWS}")
        assert listed["nothing"] == "Look at a table."
        await _listed(w, rpc)
        assert w.core.paths().count("/app") == 1
        res = await call_tool(rpc, "watch", {"table": "oak"})
        assert not res["result"].get("isError"), res
    assert not w.warnings


async def test_tools_list_says_this_app_without_a_name() -> None:
    w = _app()
    async with w.served() as (_, rpc):
        listed = await _listed(w, rpc)
        assert listed["watch"] == "Watch a table. " + sentence("this app", NEWS)


async def test_tools_list_asks_for_the_name_once() -> None:
    # Construction, define_event and attach ask Wakeshift nothing (_app() builds
    # all three); the first tools/list asks for the name, and later ones reuse it.
    w = _app()
    w.core.app_name = "Shipyard"
    assert w.core.calls == []
    async with w.served() as (_, rpc):
        for _ in range(3):
            listed = await _listed(w, rpc)
            assert listed["watch"] == "Watch a table. " + sentence("Shipyard", NEWS)
    assert w.core.paths().count("/app") == 1


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
            assert listed["watch"] == "Watch a table. " + sentence("this app", NEWS)
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
        assert listed["watch"] == "Watch a table. " + sentence("Shipyard", NEWS)
        assert w.core.paths().count("/app") == 2


async def test_an_undeclared_subscribe_is_refused() -> None:
    """A subscribe to an event the tool doesn't declare raises UsageError
    undeclared_subscribe and asks Wakeshift nothing; it logs nothing either."""
    w = Wrapped()
    got: dict[str, str] = {}

    def register(name: str, subscribes: list[Any]) -> None:
        @w.attached.tool(name, subscribes=subscribes)
        async def tool(table: str, call: ToolCall) -> str:
            try:
                await call.subscribe(w.shared, {"table": table})
                got[name] = "subscribed"
            except UsageError as err:
                got[name] = err.code
            return "ok"

    register("old", [])
    register("other", [w.turn])
    register("fine", [w.shared])
    async with w.served() as (_, rpc):
        for tool in ("old", "other", "fine"):
            res = await call_tool(rpc, tool, {"table": "oak"})
            assert not res["result"].get("isError"), (tool, res)
    assert got == {"old": "undeclared_subscribe", "other": "undeclared_subscribe", "fine": "subscribed"}
    assert len(w.core.bodies("/event-subscriptions")) == 1
    assert not w.warnings


def test_subscribes_takes_only_this_apps_events_as_a_list() -> None:
    w = Wrapped()
    theirs = Wrapped()
    with pytest.raises(UsageError, match="isn't one of this app's events"):
        w.attached.tool(subscribes=[theirs.shared])
    with pytest.raises(UsageError, match="lists the events the tool subscribes to"):
        w.attached.tool(subscribes=w.shared)  # type: ignore[arg-type]
