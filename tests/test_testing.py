"""wakeshift.testing: the offline test client, the stand-in ToolCall and
with_call, as a vendor's unit tests use them."""

from __future__ import annotations

from typing import Any

import pytest
from mcp.server.lowlevel import Server
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import BUILTIN_TOOL_NAMES, Identity, Subscriber, ToolCall, UsageError, Wakeshift
from wakeshift.testing import TestClient, TestToolCall, tool_call, with_call

from .helpers import served


class Job(BaseModel):
    job: str


class Crawl(BaseModel):
    status: str


async def test_the_test_client_is_enabled_offline_and_records() -> None:
    wake = TestClient(app="Crawler")
    assert wake.enabled and "Crawler" in repr(wake)
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    server = MCPServer("crawler")
    attached = wake.attach(server)

    @attached.tool(subscribes=[ev])
    async def start_crawl(url: str, call: ToolCall) -> str:
        """Crawl a site."""
        await call.subscribe(ev, {"job": "j1"}, once=True)
        return "started"

    async with served(server) as (_, rpc):
        listed = (await rpc("tools/list", {}))["result"]["tools"]
        names = [t["name"] for t in listed]
        assert sorted(names) == ["list_subscriptions", "start_crawl", "unsubscribe_events"]
        tool = listed[names.index("start_crawl")]
        assert "session_key" in tool["inputSchema"]["properties"]
        assert "This tool is how Crawler notifies you" in tool["description"]
        res = await rpc(
            "tools/call",
            {"name": "start_crawl", "arguments": {"url": "https://x", "session_key": "wsc_a"}},
            headers={"User-Agent": "claude-code/2"},
        )
        assert "test client" in res["result"]["content"][-1]["text"]
        lst = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {"session_key": "wsc_a"}})
        assert lst["result"]["structuredContent"]["subscriptions"][0]["event"] == "crawl.finished"
    [sub] = wake.subscriptions
    assert (sub.event, sub.args, sub.tool, sub.via, sub.state, sub.once) == (
        "crawl.finished",
        {"job": "j1"},
        "start_crawl",
        "tool",
        "created",
        True,
    )
    out = await ev.emit({"job": "j1"}, {"status": "done"})
    assert out.matched == 1
    [e] = wake.emits
    assert (e.event, e.args, e.payload, e.text, e.id) == (
        "crawl.finished",
        {"job": "j1"},
        {"status": "done"},
        "A crawl finished (job j1)",
        None,
    )
    url = await ev.create_publish_url({"job": "j2"})
    assert wake.publish_urls[0].id == url.id and url.url.startswith("https://wakeshift.test/")
    assert (await ev.status({"job": "j1"})).subscriptions == 1
    wake.clear()
    assert wake.emits == [] and wake.subscriptions == []


async def test_a_retried_emit_with_an_id_is_a_duplicate() -> None:
    wake = TestClient()
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    await ev.emit({"job": "j1"}, {"status": "done"}, id="e1")
    again = await ev.emit({"job": "j1"}, {"status": "done"}, id="e1")
    assert again.duplicate and len(wake.emits) == 1 and wake.emits[0].id == "e1"


async def test_the_test_client_never_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WAKESHIFT_KEY", "not a key")
    monkeypatch.setenv("WAKESHIFT_CORE", "https://elsewhere.example")
    assert TestClient().core == "https://wakeshift.test"
    # And an ordinary client stays inert with an empty key, whatever the environment says.
    inert = Wakeshift("")
    assert not inert.enabled


async def test_a_stand_in_call() -> None:
    wake = TestClient()
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    other = wake.define_event("crawl.started", description="A crawl started", args=Job, payload=Crawl)

    async def start_crawl(url: str, call: ToolCall) -> str:
        sub = await call.subscribe(ev, {"job": "j1"}, replay="1h")
        return f"{sub.state} {sub.id}"

    call = tool_call(tool="start_crawl", user="u1", session="wsc_forged", events=[ev], wake=wake)
    assert isinstance(call, ToolCall) and isinstance(call, TestToolCall)
    assert call.session is None  # never the raw argument
    assert await start_crawl("https://x", call) == "created sub_test_1"
    assert await start_crawl("https://x", call) == "renewed sub_test_2"
    assert call.session is not None and call.session.reveal() == "wsc_forged"
    assert [s.replay for s in call.subscriptions] == [3600, 3600] and len(wake.subscriptions) == 2
    with pytest.raises(UsageError) as e:
        await call.subscribe(other, {"job": "j1"})
    assert e.value.code == "undeclared_subscribe"
    with pytest.raises(UsageError) as e:
        await call.subscribe(ev, {"nope": 1})
    assert e.value.code == "bad_arguments"
    assert await call.unsubscribe(ev, {"job": "j1"}) is True
    native = tool_call(user="u1", native=True)
    assert (await native.subscribe(ev, {"job": "j1"})).state == "native"


async def test_with_call_for_a_wrapped_tool() -> None:
    wake = TestClient()
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    wake.attach(Server("crawler"), wrap={"start_crawl": [ev]})

    async def handler() -> str:
        await wake.current_call().subscribe(ev, {"job": "j9"})
        return "ok"

    with pytest.raises(UsageError):
        await handler()  # no call outside one
    with with_call(wake, tool_call(tool="start_crawl")) as call:
        assert await handler() == "ok"
    assert call.subscriptions[0].args == {"job": "j9"}
    # A disabled client too.
    off = Wakeshift("")
    with with_call(off, tool_call()) as c:
        assert off.current_call() is c


async def test_authorize_is_public_for_unit_tests() -> None:
    wake = TestClient()

    def authorize(s: Subscriber, args: Job) -> bool:
        return s.user == "u1" and args.job.startswith("u1-")

    ev = wake.define_event("job.done", description="A job finished", args=Job, payload=Crawl, authorize=authorize)
    assert ev.authorize is authorize
    plain = wake.define_event("job.other", description="Another", args=Job, payload=Crawl)
    assert plain.authorize is None
    assert ev.authorize(Subscriber(user="u1", client="claude-code", native=False), Job(job="u1-a")) is True


async def test_the_built_in_tool_names() -> None:
    assert BUILTIN_TOOL_NAMES == ("subscribe_events", "unsubscribe_events", "list_subscriptions", "wakeshift_wait")


async def test_list_and_unsubscribe_by_id_without_a_user_ask_by_the_conversation() -> None:
    wake = TestClient()
    ev = wake.define_event("chat.message", description="A message", args=Job, payload=Crawl, public=True)
    server = MCPServer("chat")
    wake.attach(server, identify=lambda r: Identity())
    seen: list[tuple[str, Any]] = []
    real = wake._request

    async def spy(method: str, path: str, body: Any = None, *, timeout: float | None = None) -> dict[str, Any]:
        seen.append((path, body))
        return await real(method, path, body, timeout=timeout)

    wake._request = spy  # type: ignore[method-assign]
    async with served(server) as (_, rpc):
        await rpc(
            "tools/call",
            {
                "name": "subscribe_events",
                "arguments": {"event": "chat.message", "arguments": {"job": "x"}, "session_key": "wsc_a"},
            },
        )
        lst = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {"session_key": "wsc_a"}})
        await rpc("tools/call", {"name": "unsubscribe_events", "arguments": {"id": "sub_test_1", "session_key": "wsc_a"}})
    assert [s["event"] for s in lst["result"]["structuredContent"]["subscriptions"]] == ["chat.message"]
    listed = next(b for p, b in seen if p == "/event-subscriptions/list")
    assert set(listed) == {"call"} and listed["call"]["session"] == "wsc_a" and listed["call"]["tool"] == "list_subscriptions"
    ended = next(b for p, b in seen if p == "/event-subscriptions/end")
    assert set(ended) == {"call", "id"} and ended["call"]["session"] == "wsc_a"
    assert ev.name == "chat.message"


async def test_a_stand_in_call_records_the_subscribes_context() -> None:
    wake = TestClient()
    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    call = tool_call(tool="start_crawl", wake=wake)
    await call.subscribe(ev, {"job": "j1"}, context={"key": "k1"})
    await call.subscribe(ev, {"job": "j2"})
    assert [s.context for s in call.subscriptions] == [{"key": "k1"}, None]


async def test_a_plain_with_warns_that_on_subscribe_needs_async_with() -> None:
    wake = TestClient()
    seen: list[str] = []

    async def on_subscribe(s: Any) -> None:
        seen.append(s.args.job)

    ev = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl, on_subscribe=on_subscribe)
    plain = wake.define_event("crawl.started", description="A crawl started", args=Job, payload=Crawl)
    with pytest.warns(RuntimeWarning, match="async with with_call"):
        with with_call(wake, tool_call(tool="start_crawl")) as call:
            await wake.current_call().subscribe(ev, {"job": "j1"})
    assert seen == []
    await call.finish()  # what the warning offers
    assert seen == ["j1"]
    # Nothing to run (no on_subscribe): no warning.
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with with_call(wake, tool_call(tool="start_crawl")):
            await wake.current_call().subscribe(plain, {"job": "j2"})
        async with with_call(wake, tool_call(tool="start_crawl")):
            await wake.current_call().subscribe(ev, {"job": "j3"})
    assert seen == ["j1", "j3"]
