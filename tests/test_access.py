"""Who may subscribe (design §3, PR 6): an event that declares neither
personal, authorize nor public is subscribed only by the app's own tools.
mcp-events-wire.json's next section (refusals on the MCP Events wire,
subscribe_events, and call.subscribe's state per access and caller),
subscribe-options.json's replay bounds and durations, and
disabled-client.json's option checks."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import httpx2
import pytest
from mcp.server.mcpserver import MCPServer as OfficialMCPServer
from pydantic import BaseModel

from wakeshift import Identity, Subscription, ToolCall, UsageError, Wakeshift

from .helpers import FakeCore, Logged, app_key, golden_file, initialize, served

WIRE = golden_file("mcp-events-wire.json")["next"]
OPTIONS = golden_file("subscribe-options.json")


class Job(BaseModel):
    job: str


class Pages(BaseModel):
    pages: float


class Env(BaseModel):
    env: str


class Status(BaseModel):
    status: str


class Channel(BaseModel):
    channel: str


class Text(BaseModel):
    text: str


class Table(BaseModel):
    table: str


class App:
    """The fixture's events: crawl.finished tools-only, deploy.finished with an
    authorize that allows only u1, chat.message public, game.turn personal."""

    def __init__(self, user: str | None, *, personal: bool = True) -> None:
        self.core = FakeCore(default_ok=True)
        self.core.on(
            "POST",
            "/event-subscriptions",
            lambda b: (
                200,
                {"event_subscription": {"id": "es_1", "user": b["call"].get("user"), "created": True}, "session": "wsc_s1"},
            ),
        )
        self.core.on("POST", "/event-subscriptions/unsubscribe", lambda b: (200, {"ended": True}))
        self.core.on("POST", "/events/unsubscribe", lambda b: (200, {"ended": True}))
        self.core.on("POST", "/calls/addition", lambda b: (200, {"text": "Wakeshift: subscribe yourself."}))
        self.logged = Logged(logging.INFO)
        self.wake = Wakeshift(
            app_key("crawler"),
            core="https://core.test",
            http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(self.core.handler)),
        )
        self.authorized: list[Any] = []
        w = self.wake
        self.events = {
            "tools": w.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Pages),
            "authorize": w.define_event(
                "deploy.finished", description="A deploy finished", args=Env, payload=Status, authorize=self._authorize
            ),
            "public": w.define_event("chat.message", description="A message", args=Channel, payload=Text, public=True),
        }
        if personal:
            self.events["personal"] = w.define_event(
                "game.turn", description="Your move", args=Table, payload=Text, personal=True
            )
        self.args = {
            "tools": {"job": "j1"},
            "authorize": {"env": "prod"},
            "public": {"channel": "eng"},
            "personal": {"table": "oak"},
        }
        self.server = OfficialMCPServer("crawler")
        self.attached = w.attach(self.server, identify=lambda r: Identity(user=user))
        self.results: list[Subscription | BaseException] = []
        app = self

        @self.attached.tool(subscribes=list(self.events.values()))
        async def watch(access: str, call: ToolCall) -> str:
            try:
                app.results.append(await call.subscribe(app.events[access], app.args[access]))
            except UsageError as err:
                app.results.append(err)
            return "ok"

    def _authorize(self, s: Any, a: Any) -> bool:
        self.authorized.append({"user": s.user, "native": s.native, "args": a.model_dump()})
        return s.user == "u1"

    def asked(self, path: str) -> bool:
        return bool(self.core.bodies(path))


def test_events_list_names_only_open_events() -> None:
    assert WIRE["events_list"] == ["chat.message", "deploy.finished"]


@pytest.mark.parametrize("case", WIRE["refused"], ids=lambda c: c["name"])
@pytest.mark.parametrize("dialect", ["draft", "chatgpt"])
async def test_refused_on_the_wire(case: dict[str, Any], dialect: str) -> None:
    a = App(case["user"], personal=False)  # the fixture's events: chat.message, crawl.finished, deploy.finished
    client = "openai-mcp" if dialect == "chatgpt" else "some-draft-client"
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, client)
        listed = (await rpc("events/list", {}))["result"]["events"]
        res = await rpc(case["method"], case["params"])
    assert sorted(e["name"] for e in listed) == sorted(WIRE["events_list"])
    err = res.get("error")
    assert err is not None, res
    want = case["error"]
    assert (err["code"], err["message"], err.get("data")) == (want["code"][dialect], want["message"], want["data"])
    assert not a.asked("/events/subscribe") and not a.asked("/events/poll"), "Wakeshift isn't asked"


async def test_unsubscribing_from_a_tools_only_event_stays_allowed() -> None:
    c = WIRE["unsubscribe_tools_only"]
    a = App(c["user"])
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, "some-draft-client")
        res = await rpc(c["method"], c["params"])
    assert res.get("result") == c["result"], res
    body = dict(a.core.bodies("/events/unsubscribe")[0])
    body.pop("call")
    body.pop("dialect", None)
    assert body == c["core_request"]


@pytest.mark.parametrize("case", WIRE["builtin_calls"], ids=lambda c: c["name"])
async def test_builtin_calls(case: dict[str, Any]) -> None:
    a = App(case["user"])
    async with served(a.server) as (_, rpc):
        res = await rpc("tools/call", {"name": case["tool"], "arguments": case["arguments"]})
    r = res["result"]
    assert bool(r.get("isError")) is case["isError"], r
    if "text" in case:
        assert r["content"][0]["text"] == case["text"]
    asked = a.asked("/event-subscriptions") or a.asked("/event-subscriptions/unsubscribe")
    assert asked is case["core_asked"]


@pytest.mark.parametrize("row", WIRE["tool_subscribe_states"]["rows"], ids=lambda r: f"{r['access']}-{r['caller']}-{r['user']}")
async def test_tool_subscribe_states(row: dict[str, Any]) -> None:
    access = {"authorize_allows": "authorize", "authorize_refuses": "authorize"}.get(row["access"], row["access"])
    a = App(row["user"])
    ua = "openai-mcp/1.0" if row["caller"] == "native" else "claude-code/2.1"
    async with served(a.server) as (_, rpc):
        res = await rpc("tools/call", {"name": "watch", "arguments": {"access": access}}, headers={"User-Agent": ua})
    assert not res["result"].get("isError"), res
    got = a.results[0]
    want = row["want"]
    if "error" in want:
        assert isinstance(got, UsageError) and got.code == want["error"] and want["says"] in str(got), got
    else:
        assert isinstance(got, Subscription), got
        assert got.state == want["state"] and got.reason == want.get("reason"), got
    adds = a.core.bodies("/calls/addition")
    native_line = bool(adds and adds[0].get("native"))
    assert native_line is row["native_line"]
    assert a.asked("/event-subscriptions") is row["core_asked"]
    if "logs" in row:
        assert row["logs"] in a.logged.codes()
    if "authorize_called_with" in row:
        assert a.authorized == [row["authorize_called_with"]]
    if row["access"] == "authorize_allows" and row["user"] is None and row["caller"] == "native":
        assert a.authorized == [], "authorize isn't called without a user"


@pytest.mark.parametrize("case", OPTIONS["replay_bounds"], ids=lambda c: c["name"])
async def test_replay_bounds(case: dict[str, Any]) -> None:
    secs = case["replay_seconds"]
    forms: list[Any] = [secs, float(secs), timedelta(seconds=secs)]
    if case.get("replay_string"):
        forms.append(case["replay_string"])
    a = App("u1")
    form: list[Any] = []
    out: list[Any] = []

    @a.attached.tool(subscribes=[a.events["tools"]])
    async def replayed(call: ToolCall) -> str:
        try:
            out.append(await call.subscribe(a.events["tools"], {"job": "j1"}, replay=form[0]))
        except UsageError as err:
            out.append(err)
        return "ok"

    async with served(a.server) as (_, rpc):
        for f in forms:
            form[:] = [f]
            out.clear()
            a.core.requests.clear()
            await rpc("tools/call", {"name": "replayed", "arguments": {}})
            got = out[0]
            if "error" in case:
                assert isinstance(got, UsageError) and got.code == case["error"] and case["says"] in str(got), (f, got)
                assert not a.asked("/event-subscriptions")
            else:
                body = a.core.bodies("/event-subscriptions")[0]
                assert {k: body[k] for k in case["body"]} == case["body"], (f, body)


@pytest.mark.parametrize("case", OPTIONS["duration_strings"], ids=lambda c: c["string"] or "empty")
def test_duration_strings(case: dict[str, Any]) -> None:
    from wakeshift._toolcall import _seconds

    if "want_error" in case:
        with pytest.raises(UsageError) as e:
            _seconds("ttl", case["string"])
        assert e.value.code == case["want_error"]
    else:
        assert _seconds("ttl", case["string"]) == case["seconds"]


OPTION_PENDING = {"attach_unknown_builtin", "attach_builtin_not_boolean"}


class Out(BaseModel):
    wakeshift: str


@pytest.mark.parametrize("enabled", [True, False])
def test_option_checks(enabled: bool) -> None:
    checks = golden_file("disabled-client.json")["option_checks"]["cases"]
    wake = Wakeshift(app_key("crawler") if enabled else "", core="https://core.test")
    other = Wakeshift(app_key("other"), core="https://core.test")
    crawl = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Pages)
    theirs = other.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Pages)
    server = OfficialMCPServer("crawler")
    attached = wake.attach(server)

    async def tool(call: ToolCall) -> str:
        return ""

    async def outputs(call: ToolCall) -> Out:
        return Out(wakeshift="")

    ops: dict[str, Any] = {
        "define_event_public_not_boolean": lambda: wake.define_event(
            "a.b",
            description="d",
            args=Job,
            payload=Pages,
            public="yes",  # type: ignore[arg-type]
        ),
        "define_event_public_and_personal": lambda: wake.define_event(
            "a.c", description="d", args=Job, payload=Pages, public=True, personal=True
        ),
        "define_event_public_and_authorize": lambda: wake.define_event(
            "a.d", description="d", args=Job, payload=Pages, public=True, authorize=lambda s, a: True
        ),
        "attach_wrap_not_events": lambda: wake.attach(OfficialMCPServer("w"), wrap={"start_crawl": ["crawl.finished"]}),  # type: ignore[list-item]
        "attach_twice": lambda: wake.attach(server),
        "register_builtin_name": lambda: attached.tool("subscribe_events")(tool),
        "register_schema_names_wake_arg": lambda: attached.tool("t1")(_with_session_key),
        "register_output_names_wakeshift": lambda: attached.tool("t2")(outputs),
        "register_event_from_another_client": lambda: attached.tool("t3", subscribes=[theirs]),
    }
    for c in checks:
        if (c.get("skip") or {}).get("python") or c["op"] in OPTION_PENDING:
            continue
        assert c["op"] in ops, c["name"]
        with pytest.raises(UsageError) as e:
            ops[c["op"]]()
        assert e.value.code == c["error"] and c["says"] in str(e.value), (c["name"], str(e.value), enabled)
    assert crawl.name == "crawl.finished"


async def _with_session_key(session_key: str, call: ToolCall) -> str:
    return ""


async def test_a_server_with_its_own_mcp_events_keeps_them() -> None:
    """Appwrite's own events/* handlers survive attach (#70), and its native
    callers get no line for Wakeshift's events."""
    a_core = FakeCore(default_ok=True)
    wake = Wakeshift(
        app_key("aw"), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(a_core.handler))
    )
    turn = wake.define_event("game.turn", description="Your move", args=Table, payload=Text, personal=True)
    server = OfficialMCPServer("appwrite")
    from mcp_types import RequestParams

    async def own(ctx: Any, params: Any) -> dict[str, Any]:
        return {"resultType": "complete", "events": [{"name": "appwrite.own"}]}

    server._lowlevel_server.add_request_handler("events/list", RequestParams, own)
    logged = Logged(logging.INFO)
    attached = wake.attach(server, identify=lambda r: Identity(user="u1"))
    got: list[Any] = []

    @attached.tool(subscribes=[turn])
    async def watch(call: ToolCall) -> str:
        got.append(await call.subscribe(turn, {"table": "oak"}))
        return "ok"

    async with served(server, stateless=False) as (_, rpc):
        init = await initialize(rpc, "openai-mcp")
        assert "events" not in init["result"]["capabilities"]
        listed = await rpc("events/list", {})
        await rpc("tools/call", {"name": "watch", "arguments": {}}, headers={"User-Agent": "openai-mcp/1.0"})
    assert listed["result"]["events"] == [{"name": "appwrite.own"}]
    # Info, not a warning: serving its own MCP Events is the server's choice.
    assert [(r.wakeshift_code, r.levelname) for r in logged.records if r.wakeshift_code == "events_capability_kept"] == [
        ("events_capability_kept", "INFO")
    ]
    assert got[0].state == "unavailable" and got[0].reason == "client_unsupported"


async def test_manifest_changes_log_event_closed() -> None:
    core = FakeCore(default_ok=True)
    changed = [
        {"event": "crawl.finished", "from": "any", "to": "tools", "ended": 0},
        {"event": "build.finished", "from": "tools", "to": "any", "ended": 0},
    ]
    core.on("PUT", "/manifest", lambda b: (200, {**b, "changed": changed}))
    wake = Wakeshift(
        app_key("crawler"), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    )
    wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Pages)
    logged = Logged(logging.INFO)
    await wake.publish_manifest({"description": "Crawls"})
    assert [(r.wakeshift_code, r.wakeshift_event, r.wakeshift_ended) for r in logged.records] == [  # type: ignore[attr-defined]
        ("event_closed", "crawl.finished", 0)
    ]
    assert core.bodies("/manifest")[0]["events"][0]["subscribe"] == "tools"
