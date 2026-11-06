"""Subscribe vouchers (docs/subscribe-vouchers.md §4.3, §4.4, §7): a tool
subscribing a native client (ChatGPT) to a tools-only event gives it a
native line, whose voucher the core renders; the client writes it back as
wakeshift_voucher, which the SDK takes out of the arguments and sends to
Wakeshift as voucher. Shared fixtures: mcp-events-wire.json vouchers,
events-list.json vouchers, event-tools.json, secret-pattern.json."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any

import httpx2
import pytest
from mcp.server.mcpserver import MCPServer as OfficialMCPServer
from pydantic import BaseModel

from wakeshift import Identity, Subscription, ToolCall, UsageError, Wakeshift, redact_secrets

from .helpers import FakeCore, Logged, app_key, golden_file, initialize, served

WIRE = golden_file("mcp-events-wire.json")["vouchers"]
V: str = WIRE["voucher"]
DIALECTS = {"draft": "some-draft-client", "chatgpt": "openai-mcp"}


class Job(BaseModel):
    job: str


class Pages(BaseModel):
    pages: float


class Channel(BaseModel):
    channel: str


class Text(BaseModel):
    text: str


class Env(BaseModel):
    env: str


class Status(BaseModel):
    status: str


class App:
    """The fixture's events: crawl.finished tools-only, chat.message public,
    deploy.finished with an authorize that allows only u1."""

    def __init__(self, user: str | None, *, feature: bool = True) -> None:
        self.core = FakeCore(default_ok=True)
        features = ["subscribe_vouchers"] if feature else []
        self.core.on("POST", "/manifest/merge", lambda b: (200, {**b, "features": features}))
        self.core.on("GET", "/app", lambda b: (200, {"id": "crawler", "name": "Sample"}))
        self.core.on("POST", "/calls/addition", lambda b: (200, {"text": "Woken."}))
        self.logged = Logged(logging.DEBUG)
        self.wake = Wakeshift(
            app_key("crawler"),
            core="https://core.test",
            http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(self.core.handler)),
        )
        self.subscribed: list[dict[str, Any]] = []
        self.authorized: list[Any] = []
        self.results: list[Subscription] = []
        w = self.wake

        def on_subscribe(s: Any) -> None:
            args = s.args.model_dump() if isinstance(s.args, BaseModel) else s.args
            self.subscribed.append({"args": args, "user": s.user, "via": s.via, "tool": s.tool})

        self.crawl = w.define_event(
            "crawl.finished", description="A crawl finished", args=Job, payload=Pages, on_subscribe=on_subscribe
        )
        self.chat = w.define_event(
            "chat.message", description="A new message", args=Channel, payload=Text, public=True, on_subscribe=on_subscribe
        )
        w.define_event("deploy.finished", description="A deploy finished", args=Env, payload=Status, authorize=self._authorize)
        self.server = OfficialMCPServer("crawler")
        self.attached = w.attach(self.server, identify=lambda r: Identity(user=user))
        app = self

        @self.attached.tool(subscribes=[self.chat])
        async def watch_chat(channel: str, call: ToolCall) -> str:
            await call.subscribe(app.chat, {"channel": channel}, once=True)
            return "ok"

        @self.attached.tool(subscribes=[self.crawl])
        async def start_crawl(job: str, call: ToolCall) -> str:
            app.results.append(await call.subscribe(app.crawl, {"job": job}, once=True, ttl="1h", replay="10m"))
            return f"Crawl {job} started."

    def _authorize(self, s: Any, a: Any) -> bool:
        self.authorized.append({"user": s.user, "args": a.model_dump()})
        return s.user == "u1"

    def no_voucher_logged(self, what: str) -> None:
        for r in self.logged.records:
            assert "wsv_" not in r.getMessage() and "wsv_" not in json.dumps(r.__dict__, default=str), f"{what}: {r.getMessage()}"


def _body(core: FakeCore, path: str) -> dict[str, Any]:
    bodies = core.bodies(path)
    assert bodies, f"{path} wasn't asked"
    return dict(bodies[-1])


@pytest.mark.parametrize("case", WIRE["subscribe"], ids=lambda c: c["name"])
@pytest.mark.parametrize("dialect", ["draft", "chatgpt"])
async def test_subscribe_with_a_voucher(case: dict[str, Any], dialect: str) -> None:
    a = App(case["user"])
    a.core.on("POST", "/events/subscribe", lambda b: (200, case["core_response"]))
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, DIALECTS[dialect])
        res = await rpc("events/subscribe", case["params"])
    assert res.get("result") == case["result"], res
    body = _body(a.core, "/events/subscribe")
    call, d = body.pop("call"), body.pop("dialect")
    assert d == dialect
    assert body == case["core_request"]
    assert call.get("user") == case["call_user"]
    if "voucher" in case["core_request"]:
        # A stateful server issues Mcp-Session-Id: the voucher is bound to its hash.
        assert rpc.session
        assert call.get("transport_session") == hashlib.sha256(rpc.session.encode()).hexdigest()
    else:
        assert "transport_session" not in call
    await asyncio.sleep(0.05)
    assert a.subscribed == ([case["on_subscribe"]] if case["on_subscribe"] else [])
    assert a.authorized == []
    a.no_voucher_logged(case["name"])


@pytest.mark.parametrize("case", WIRE["unsubscribe"], ids=lambda c: c["name"])
async def test_unsubscribe_with_a_voucher(case: dict[str, Any]) -> None:
    a = App(case["user"])
    a.core.on("POST", "/events/unsubscribe", lambda b: (200, case["core_response"]))
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        res = await rpc("events/unsubscribe", case["params"])
    assert res.get("result") == case["result"], res
    body = _body(a.core, "/events/unsubscribe")
    call = body.pop("call")
    body.pop("dialect")
    assert body == case["core_request"]
    assert call.get("user") == case["call_user"]


@pytest.mark.parametrize("case", WIRE["refused"], ids=lambda c: c["name"])
@pytest.mark.parametrize("dialect", ["draft", "chatgpt"])
async def test_refused_without_asking(case: dict[str, Any], dialect: str) -> None:
    a = App(case["user"], feature=case.get("feature", True))
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, DIALECTS[dialect])
        res = await rpc(case["method"], case["params"])
    err = res.get("error")
    assert err is not None, res
    want = case["error"]
    assert (err["code"], err["message"], err.get("data")) == (want["code"][dialect], want["message"], want["data"])
    assert not any(p.startswith("/events/") for _, p, _ in a.core.requests), "Wakeshift was asked"
    a.no_voucher_logged(case["name"])


@pytest.mark.parametrize("case", WIRE["core_refused"], ids=lambda c: c["name"])
@pytest.mark.parametrize("dialect", ["draft", "chatgpt"])
async def test_core_refusals(case: dict[str, Any], dialect: str) -> None:
    a = App(case["user"])
    a.core.on("POST", "/events/subscribe", lambda b: (case["core_status"], case["core_body"]))
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, DIALECTS[dialect])
        res = await rpc(case["method"], case["params"])
    err = res.get("error")
    assert err is not None, res
    want = case["error"]
    assert (err["code"], err["message"], err.get("data")) == (want["code"][dialect], want["message"], want["data"])
    for word in ("again", "retry", "poll", "call the", "call this"):
        assert word not in err["message"].lower()
    await asyncio.sleep(0.05)
    assert a.subscribed == []
    a.no_voucher_logged(case["name"])


@pytest.mark.parametrize(
    "key,client,feature",
    [
        ("native_feature", "openai-mcp", True),
        ("bridged_feature", "claude-code", True),
        ("native_no_feature", "openai-mcp", False),
    ],
)
async def test_events_list(key: str, client: str, feature: bool) -> None:
    a = App("u1", feature=feature)
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, client)
        events = (await rpc("events/list", {}))["result"]["events"]
    assert sorted(e["name"] for e in events) == WIRE["events_list"][key]
    for e in events:
        if e["name"] == "crawl.finished":
            assert e["inputSchema"]["required"] == ["job", "wakeshift_voucher"]
            assert (
                e["inputSchema"]["properties"]["wakeshift_voucher"]["description"]
                == "The voucher from Sample's tool result. Only its tools hand them out."
            )
        if e["name"] == "chat.message":
            assert "wakeshift_voucher" not in e["inputSchema"]["properties"]


@pytest.mark.parametrize("row", WIRE["tool_subscribe_states"]["rows"], ids=lambda r: f"feature={r['feature']},user={r['user']}")
async def test_tool_subscribe_states(row: dict[str, Any]) -> None:
    a = App(row["user"], feature=row["feature"])
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        await rpc("tools/call", {"name": "start_crawl", "arguments": {"job": "j1"}})
    got = a.results[0]
    assert got.state == row["want"]["state"]
    if "user" in row["want"]:
        assert got.user == row["want"]["user"]
    if "reason" in row["want"]:
        assert got.reason == row["want"]["reason"]
    native = (a.core.bodies("/calls/addition") or [{}])[-1].get("native") or []
    assert bool(native) == row["native_line"]
    if native:
        assert native == [{"name": "crawl.finished", "arguments": {"job": "j1"}, "once": True, "ttl": 3600, "replay": 600}]
    assert not a.core.bodies("/event-subscriptions")
    codes = [c for c in a.logged.codes() if c == "native_unavailable"]
    assert codes == ([row["logs"]] if row["logs"] else [])


async def test_a_native_only_addition_failure_returns_the_tools_result() -> None:
    a = App(None)
    a.core.on("POST", "/calls/addition", lambda b: (500, {"error": {"code": "internal", "message": "try again"}}))
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        res = await rpc("tools/call", {"name": "start_crawl", "arguments": {"job": "j1"}})
    r = res["result"]
    assert not r.get("isError"), r
    assert [c["text"] for c in r["content"]] == ["Crawl j1 started."]
    assert "Try again" not in json.dumps(r)
    recs = [x for x in a.logged.records if getattr(x, "wakeshift_code", "") == "native_unavailable"]
    assert len(recs) == 1 and "addition" in recs[0].getMessage()


async def test_the_voucher_never_reaches_authorize_or_the_core_on_an_open_event() -> None:
    a = App("u1")
    a.core.on(
        "POST",
        "/events/subscribe",
        lambda b: (
            200,
            {"id": "sub_1", "refresh_before": "2026-10-29T13:00:00Z", "cursor": "c1.0", "truncated": False, "created": True},
        ),
    )
    params = {
        "name": "deploy.finished",
        "arguments": {"env": "prod", "wakeshift_voucher": V},
        "delivery": {
            "mode": "webhook",
            "url": "https://agent.example/h",
            "secret": "whsec_dGVzdC1zZWNyZXQtdGVzdC1zZWNyZXQtdGVzdA==",
        },
    }
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        res = await rpc("events/subscribe", params)
    assert "result" in res, res
    assert a.authorized == [{"user": "u1", "args": {"env": "prod"}}]
    assert "wsv_" not in json.dumps(a.core.bodies("/events/subscribe"))
    a.no_voucher_logged("open event")


def test_define_event_refuses_wakeshift_voucher() -> None:
    class Bad(BaseModel):
        wakeshift_voucher: str

    wake = Wakeshift(app_key("crawler"), core="https://core.test")
    with pytest.raises(UsageError) as e:
        wake.define_event("x.y", description="X", args=Bad, payload=Text)
    assert e.value.code == "config" and "wakeshift_voucher" in str(e.value)


async def test_an_old_core_keeps_tools_only_unavailable_and_forwards_no_voucher() -> None:
    a = App("u1")
    a.core.on("POST", "/manifest/merge", lambda b: (200, b))  # no features
    params = {
        "name": "crawl.finished",
        "arguments": {"job": "j1", "wakeshift_voucher": V},
        "delivery": {"mode": "webhook", "url": "https://a.example/h", "secret": "whsec_x"},
    }
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        await rpc("tools/call", {"name": "start_crawl", "arguments": {"job": "j1"}})
        res = await rpc("events/subscribe", params)
    assert a.results[0].state == "unavailable" and a.results[0].reason == "client_unsupported"
    assert "error" in res
    assert not any(p.startswith("/events/") for _, p, _ in a.core.requests)


async def test_the_canary_voucher_is_in_no_log_line() -> None:
    a = App(None)
    line = (
        'To be woken by crawl.finished with {"job":"j1"}, subscribe to it (MCP Events) with wakeshift_voucher set to '
        + V
        + " in its arguments. It works once, for these arguments."
    )
    a.core.on("POST", "/calls/addition", lambda b: (200, {"text": line}))
    n = {"n": 0}

    def sub(b: Any) -> tuple[int, Any]:
        n["n"] += 1
        if n["n"] < 3:
            return 200, {
                "id": "sub_1",
                "refresh_before": "2026-10-29T13:00:00Z",
                "cursor": "c1.0",
                "truncated": False,
                "created": n["n"] == 1,
                "via": "tool:start_crawl",
                "tool": "start_crawl",
            }
        return 403, {"error": {"code": "bad_voucher", "message": "This voucher can't subscribe to this event."}}

    a.core.on("POST", "/events/subscribe", sub)
    params = {
        "name": "crawl.finished",
        "arguments": {"job": "j1", "wakeshift_voucher": V},
        "delivery": {"mode": "webhook", "url": "https://a.example/h", "secret": "whsec_dGVzdC1zZWNyZXQtdGVzdC1zZWNyZXQtdGVzdA=="},
    }
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        r = await rpc("tools/call", {"name": "start_crawl", "arguments": {"job": "j1"}})
        for _ in range(3):
            last = await rpc("events/subscribe", params)
    assert V in json.dumps(r), "the result hands it to the client"
    assert "wsv_" not in repr(a.results[0])
    assert "wsv_" not in json.dumps(last)
    await asyncio.sleep(0.05)
    assert len(a.subscribed) == 1 and "wsv_" not in repr(a.subscribed)
    a.no_voucher_logged("canary")
    assert V not in redact_secrets(json.dumps(r))


@pytest.mark.parametrize("features", [None, [], ["subscribe_vouchers"]], ids=["absent", "empty", "on"])
async def test_voucher_fields_only_to_a_core_that_reported_the_feature(features: list[str] | None) -> None:
    """An older core refuses fields it doesn't know: without the feature,
    the wire is today's, byte for byte (no transport_session, voucher or line
    options)."""
    on = bool(features)
    a = App("u1", feature=on)
    a.core.on("POST", "/manifest/merge", lambda b: (200, b if features is None else {**b, "features": features}))
    a.core.on(
        "POST",
        "/events/subscribe",
        lambda b: (
            200,
            {"id": "sub_1", "refresh_before": "2026-10-29T13:00:00Z", "cursor": "c1.0", "truncated": False, "created": True},
        ),
    )
    open_params = {
        "name": "chat.message",
        "arguments": {"channel": "eng", "wakeshift_voucher": V},
        "delivery": {"mode": "webhook", "url": "https://a.example/h", "secret": "whsec_dGVzdC1zZWNyZXQtdGVzdC1zZWNyZXQtdGVzdA=="},
    }
    async with served(a.server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        await rpc("tools/call", {"name": "start_crawl", "arguments": {"job": "j1"}})
        # An open event's native line: an addition whatever the core offers.
        await rpc("tools/call", {"name": "watch_chat", "arguments": {"channel": "eng"}})
        await rpc("events/subscribe", open_params)
        session = rpc.session
    add = a.core.bodies("/calls/addition")
    sub = a.core.bodies("/events/subscribe")[-1]
    assert "voucher" not in sub and "transport_session" not in sub["call"]
    if on:
        assert session
        assert add[0]["call"]["transport_session"] == hashlib.sha256(session.encode()).hexdigest()
        assert add[0]["native"] == [
            {"name": "crawl.finished", "arguments": {"job": "j1"}, "once": True, "ttl": 3600, "replay": 600}
        ]
        assert add[1]["native"] == [{"name": "chat.message", "arguments": {"channel": "eng"}}]
    else:
        assert [b["native"] for b in add] == [[{"name": "chat.message", "arguments": {"channel": "eng"}}]]
        for _, _, body in a.core.requests:
            assert "transport_session" not in json.dumps(body) and "wsv_" not in json.dumps(body)
