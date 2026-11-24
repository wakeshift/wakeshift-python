"""MCP Events push (events/stream): attach relays Wakeshift's NDJSON upstream
to the client as notifications on the request's own stream.
The push cases of mcp-events-wire.json, shared
with the Node and Go SDKs, run over Streamable HTTP (stateless 2026-07-28
and a 2025-06-18 session) on a live server; stdio runs in memory."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx2
import pytest
import uvicorn
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import Identity, Request, Subscribed, ToolCall, Wakeshift
from wakeshift._push import push_config
from wakeshift._spec import EXTENSION_KEY, SUBSCRIPTION_ID_META
from wakeshift.testing import TestClient

from .helpers import Logged, app_key, golden_file

WIRE = golden_file("mcp-events-wire.json")
PUSH: dict[str, Any] = WIRE["push"]

#: The delivery modes, without the fixture: the contract's order.
DELIVERY = ["webhook", "poll", "push"]
DELIVERY_TOOLS_ONLY = ["webhook", "push"]
NOT_SERVED = {"feature": "deliveryMode", "value": "push"}


class Job(BaseModel):
    job: str


class Channel(BaseModel):
    channel: str


class Env(BaseModel):
    env: str


class Payload(BaseModel):
    text: str = ""


# ── A fake Wakeshift that serves the push upstream ────────────────────────


class _Body(httpx2.AsyncByteStream):
    """An NDJSON body the test writes frame by frame; closed is set when the
    SDK closes the connection (the upstream aborted)."""

    def __init__(self, frames: list[dict[str, Any]], hold: bool) -> None:
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        for f in frames:
            self.queue.put_nowait((json.dumps(f) + "\n").encode())
        if not hold:
            self.queue.put_nowait(None)
        self.closed = asyncio.Event()

    def write(self, frame: dict[str, Any]) -> None:
        self.queue.put_nowait((json.dumps(frame) + "\n").encode())

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = await self.queue.get()
            if chunk is None:
                return
            yield chunk

    async def aclose(self) -> None:
        self.closed.set()


@dataclass
class Core:
    """Wakeshift's app API, in memory: the manifest (with features), and
    events/stream answered by script (a status and error, or frames; hold
    keeps the stream open after them)."""

    features: list[str] = field(default_factory=lambda: ["subscribe_vouchers", "push"])
    status: int = 200
    error: dict[str, Any] | None = None
    frames: list[dict[str, Any]] = field(
        default_factory=lambda: [
            {"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000},
            {"type": "close"},
        ]
    )
    hold: bool = False
    requests: list[tuple[str, Any]] = field(default_factory=list)
    bodies: list[_Body] = field(default_factory=list)
    replies: dict[str, Callable[[Any], dict[str, Any]]] = field(default_factory=dict)
    retry_after: str = "1"
    #: A refusal's body that isn't Wakeshift's JSON (a proxy's page).
    raw_error: str | None = None

    def streams(self) -> list[Any]:
        return [b for p, b in self.requests if p == "/events/stream"]

    async def handler(self, request: httpx2.Request) -> httpx2.Response:
        path = request.url.path.removeprefix("/api/apps/v1")
        body = json.loads(await request.aread()) if request.method != "GET" else None
        self.requests.append((path, body))
        if path in ("/manifest", "/manifest/merge"):
            return httpx2.Response(200, json={**(body or {}), "features": self.features})
        if path == "/app":
            return httpx2.Response(200, json={"id": "a1", "name": "Next", "key": {"key_id": "k", "name": "k"}})
        if path == "/events/stream":
            if self.status != 200:
                headers = {"Retry-After": self.retry_after} if self.status == 503 else {}
                if self.raw_error is not None:
                    return httpx2.Response(self.status, text=self.raw_error, headers=headers)
                return httpx2.Response(self.status, json={"error": self.error} if self.error else {}, headers=headers)
            b = _Body(self.frames, self.hold)
            self.bodies.append(b)
            return httpx2.Response(200, headers={"Content-Type": "application/x-ndjson"}, stream=b)
        if path in self.replies:
            return httpx2.Response(200, json=self.replies[path](body))
        return httpx2.Response(200, json={})


def next_app(core: Core, *, push: Any = None, on_subscribe: Any = None) -> tuple[Wakeshift, MCPServer]:
    """The fixture's app ("next"): crawl.finished tools-only {job},
    chat.message public {channel}, deploy.finished authorize {env} (only u1).
    identify's user is the X-User header (none without it)."""
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    wake = Wakeshift(app_key("next"), core="https://core.test", http_client=http)
    crawl = wake.define_event(
        "crawl.finished", description="A crawl finished", args=Job, payload=Payload, on_subscribe=on_subscribe
    )
    wake.define_event(
        "chat.message", description="A new message", args=Channel, payload=Payload, public=True, on_subscribe=on_subscribe
    )
    wake.define_event(
        "deploy.finished", description="A deploy finished", args=Env, payload=Payload, authorize=lambda s, a: s.user == "u1"
    )
    server = MCPServer("next")

    def identify(r: Request) -> Identity | None:
        u = r.headers.get("x-user")
        return Identity(user=u) if u else None

    attached = wake.attach(server, identify=identify, push=push)

    @attached.tool(subscribes=[crawl])
    async def start_crawl(job: str, call: ToolCall) -> str:
        """Start a crawl."""
        await call.subscribe(crawl, {"job": job})
        return "started"

    return wake, server


# ── A live server and an SSE client ───────────────────────────────────────


@asynccontextmanager
async def live(server: Any, *, stateless: bool = True, json_response: bool = False) -> AsyncIterator[str]:
    """The server on 127.0.0.1 (uvicorn, a free port): its base URL."""
    app = server.streamable_http_app(stateless_http=stateless, json_response=json_response)
    srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on"))
    task = asyncio.create_task(srv.serve())
    while not srv.started:
        await asyncio.sleep(0.01)
    port = srv.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        srv.should_exit = True
        await task


def modern_meta(client: str = "spec-client", declares: bool = True) -> dict[str, Any]:
    return {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": client, "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {"extensions": {EXTENSION_KEY: {}}} if declares else {},
    }


@dataclass
class Answer:
    status: int
    notifications: list[dict[str, Any]]
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        # The host's own _meta on every result (serverInfo) isn't the SDK's.
        if isinstance(self.result, dict) and isinstance(self.result.get("_meta"), dict):
            own = {k: v for k, v in self.result["_meta"].items() if k != "io.modelcontextprotocol/serverInfo"}
            self.result = {k: v for k, v in self.result.items() if k != "_meta"}
            if own:
                self.result["_meta"] = own


def final(rid: Any) -> dict[str, Any]:
    """An events/stream's final result: complete, with the subscription ID as sent."""
    return {"resultType": "complete", "_meta": {SUBSCRIPTION_ID_META: rid}}


def fixture_result(case: dict[str, Any], rid: Any) -> dict[str, Any]:
    """The fixture's final result, with the subscription ID where it doesn't name one yet."""
    r = dict(case["result"])
    r.setdefault("_meta", {SUBSCRIPTION_ID_META: rid})
    return r


class Session:
    """An MCP client over Streamable HTTP: stateless 2026-07-28 (each
    request carries its envelope), or a 2025-06-18 session (initialize)."""

    def __init__(
        self, base: str, *, client: str = "spec-client", user: str | None = "u1", modern: bool = True, legacy: str = "2025-06-18"
    ) -> None:
        self.http = httpx2.AsyncClient(base_url=base, timeout=20)
        self.client = client
        self.user = user
        self.modern = modern
        self.legacy = legacy
        self.session: str | None = None

    def headers(self, method: str, extra: dict[str, str] | None = None) -> dict[str, str]:
        h = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": "2026-07-28" if self.modern else self.legacy,
        }
        if self.modern:
            h["Mcp-Method"] = method
        if self.session:
            h["Mcp-Session-Id"] = self.session
        if self.user:
            h["X-User"] = self.user
        h.update(extra or {})
        return h

    async def initialize(self, capabilities: dict[str, Any] | None = None) -> None:
        msg = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": self.legacy,
                "capabilities": capabilities or {},
                "clientInfo": {"name": self.client, "version": "1"},
            },
        }
        r = await self.http.post("/mcp", content=json.dumps(msg), headers=self.headers("initialize"))
        self.session = r.headers.get("mcp-session-id")
        await self.http.post(
            "/mcp", content=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}), headers=self.headers("x")
        )

    def params(self, params: dict[str, Any], declares: bool = True) -> dict[str, Any]:
        if not self.modern:
            return params
        return {**params, "_meta": {**modern_meta(self.client, declares), **(params.get("_meta") or {})}}

    async def call(self, method: str, params: dict[str, Any], *, id: Any = 1, header: str | None = None) -> Answer:
        extra = {"Mcp-Name": header} if header is not None else None
        msg = {"jsonrpc": "2.0", "id": id, "method": method, "params": self.params(params)}
        notes: list[dict[str, Any]] = []
        async with self.http.stream("POST", "/mcp", content=json.dumps(msg), headers=self.headers(method, extra)) as r:
            if "text/event-stream" not in r.headers.get("content-type", ""):
                body = json.loads(await r.aread())
                return Answer(r.status_code, [], body.get("result"), body.get("error"))
            async for line in r.aiter_lines():
                if not line.startswith("data:"):
                    continue
                m = json.loads(line[5:])
                if "id" in m and ("result" in m or "error" in m):
                    return Answer(r.status_code, notes, m.get("result"), m.get("error"))
                notes.append({"method": m["method"], "params": m.get("params")})
        return Answer(r.status_code, notes)

    async def aclose(self) -> None:
        await self.http.aclose()


# ── The shared fixture ────────────────────────────────────────────────────


def _core_for(case: dict[str, Any]) -> Core:
    return Core(frames=list(case["frames"]))


async def _run_streams(modern: bool) -> None:
    for case in PUSH["streams"]:
        core = _core_for(case)
        _, server = next_app(core)
        async with live(server, stateless=modern) as base:
            s = Session(base, user=case["user"], modern=modern)
            if not modern:
                await s.initialize({"extensions": {EXTENSION_KEY: {}}})
            got = await s.call("events/stream", case["params"], id=case["id"])
            await s.aclose()
        assert got.error is None, (case["name"], got.error)
        assert got.notifications == case["notifications"], case["name"]
        assert got.result == fixture_result(case, case["id"]), case["name"]
        (sent,) = core.streams()
        assert {k: v for k, v in sent.items() if k != "call"} == case["core_request"], case["name"]
        assert sent["call"].get("user") == case["call_user"], case["name"]


async def test_the_push_streams_of_the_fixture_stateless() -> None:
    await _run_streams(modern=True)


async def test_the_push_streams_of_the_fixture_in_a_session() -> None:
    await _run_streams(modern=False)


async def test_terminated_codes_by_dialect() -> None:
    for row in PUSH["terminated_codes"]:
        for dialect, client in (("draft", "spec-client"), ("chatgpt", "openai-mcp")):
            core = Core(
                frames=[
                    {"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000},
                    {"type": "terminated", "error": {"name": row["name"], "message": "gone"}},
                ]
            )
            _, server = next_app(core)
            async with live(server) as base:
                s = Session(base, client=client)
                got = await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}}, id=4)
                await s.aclose()
            assert got.result == final(4)
            assert got.notifications[-1]["params"]["error"] == {"code": row["code"][dialect], "message": "gone"}, (row, dialect)


async def test_refusals_before_the_stream_opens_in_both_dialects() -> None:
    for case in PUSH["refused"]:
        for dialect, client in (("draft", "spec-client"), ("chatgpt", "openai-mcp")):
            core = Core(
                status=case.get("core_status", 200),
                error=case.get("core_error"),
                retry_after=str(case.get("retry_after", 0)),
            )
            _, server = next_app(core)
            async with live(server) as base:
                s = Session(base, client=client, user=case["user"])
                t0 = time.monotonic()
                got = await s.call("events/stream", case["params"], id=5, header=case.get("header"))
                waited_ms = (time.monotonic() - t0) * 1000
                await s.aclose()
            if "result" in case:
                assert got.result == fixture_result(case, 5) and got.notifications == [], case["name"]
                assert waited_ms >= case.get("min_ms", 0), (case["name"], waited_ms)
                continue
            assert got.error is not None, (case["name"], dialect, got)
            want = case["error"]
            assert got.error["code"] == want["code"][dialect], (case["name"], dialect, got.error)
            assert got.error["message"] == want["message"], (case["name"], dialect, got.error)
            assert got.error.get("data") == want["data"], (case["name"], dialect, got.error)
            if "core_status" not in case:
                assert core.streams() == [], case["name"]  # refused before Wakeshift is asked
            if case.get("header"):
                assert got.status == 400


async def test_the_fixtures_delivery_and_not_served() -> None:
    assert PUSH["delivery"] == DELIVERY and PUSH["delivery_tools_only"] == DELIVERY_TOOLS_ONLY
    assert PUSH["not_served"]["data"] == NOT_SERVED
    from wakeshift._spec import STREAM_REFUSAL

    assert PUSH["not_served"]["message"] == STREAM_REFUSAL


# ── Against the contract, without the fixture ─────────────────────────────


async def test_relays_frames_with_the_subscription_id_as_sent() -> None:
    frames = [
        {"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 15000, "created": True},
        {
            "type": "event",
            "occurrence": {
                "eventId": "e1",
                "name": "chat.message",
                "timestamp": "2026-11-12T00:00:00Z",
                "data": {"text": "hi"},
                "cursor": "c2.a.1.m",
                "_meta": {"ai.wakeshift/text": "hi"},
            },
        },
        {"type": "heartbeat", "cursor": "c2.a.1.m"},
        {"type": "unknown-later"},
        {"type": "close"},
    ]
    for rid in (7, "s-1"):
        for modern in (True, False):
            core = Core(frames=frames)
            _, server = next_app(core)
            async with live(server, stateless=modern) as base:
                s = Session(base, modern=modern)
                if not modern:
                    await s.initialize()
                got = await s.call(
                    "events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}, "cursor": None}, id=rid
                )
                await s.aclose()
            assert got.result == final(rid)
            assert [n["method"] for n in got.notifications] == [
                "notifications/events/active",
                "notifications/events/event",
                "notifications/events/heartbeat",
            ]
            for n in got.notifications:
                assert n["params"]["_meta"][SUBSCRIPTION_ID_META] == rid
            # created never reaches the client
            assert got.notifications[0]["params"] == {
                "cursor": "c2.a.0.m",
                "truncated": False,
                "heartbeatMs": 15000,
                "_meta": {SUBSCRIPTION_ID_META: rid},
            }
            assert got.notifications[1]["params"]["_meta"]["ai.wakeshift/text"] == "hi"
            (sent,) = core.streams()
            assert sent["cursor"] is None and sent["max_age_ms"] is None and sent["heartbeat_ms"] is None
            assert sent["call"]["user"] == "u1" and "voucher" not in sent


async def test_push_is_listed_only_where_served() -> None:
    # Served: the core reports push, the response streams, push isn't off.
    for features, push, json_response, served in (
        (["subscribe_vouchers", "push"], None, False, True),
        (["subscribe_vouchers"], None, False, False),
        (["subscribe_vouchers", "push"], False, False, False),
        (["subscribe_vouchers", "push"], None, True, False),
    ):
        core = Core(features=features)
        _, server = next_app(core, push=push)
        async with live(server, json_response=json_response) as base:
            s = Session(base)
            listed = await s.call("events/list", {})
            stream = await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}})
            unknown = await s.call("events/stream", {"name": "no.such", "arguments": {}})
            await s.aclose()
        assert listed.result is not None
        by = {e["name"]: e["delivery"] for e in listed.result["events"]}
        why = (features, push, json_response)
        if served:
            assert by == {"crawl.finished": DELIVERY_TOOLS_ONLY, "chat.message": DELIVERY, "deploy.finished": DELIVERY}, why
            assert stream.result == final(1), why
            assert unknown.error is not None and unknown.error["code"] == -32023, why
        else:
            assert by == {
                "crawl.finished": ["webhook"],
                "chat.message": ["webhook", "poll"],
                "deploy.finished": ["webhook", "poll"],
            }, why
            for a in (stream, unknown):
                assert a.error is not None
                assert a.error["code"] == -32026 and a.error["data"] == NOT_SERVED, why
            assert core.streams() == [], why


async def test_max_heartbeat_ms_is_the_smaller_or_alone() -> None:
    for suggested, mx, asked in ((15000, 20000, 15000), (40000, 20000, 20000), (None, 20000, 20000), (12000, None, 12000)):
        core = Core()
        _, server = next_app(core, push={"max_heartbeat_ms": mx} if mx else None)
        async with live(server) as base:
            s = Session(base)
            params: dict[str, Any] = {"name": "chat.message", "arguments": {"channel": "eng"}}
            if suggested is not None:
                params["heartbeatMs"] = suggested
            await s.call("events/stream", params)
            await s.aclose()
        assert core.streams()[0]["heartbeat_ms"] == asked


async def test_silence_and_max_stream_end_the_stream_with_the_final_result() -> None:
    # Silent for 3 x the grant (20 ms): the final result, logged.
    core = Core(frames=[{"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 20}], hold=True)
    _, server = next_app(core)
    logs = Logged()
    async with live(server) as base:
        s = Session(base)
        got = await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}})
        await s.aclose()
    assert got.result == final(1) and len(got.notifications) == 1
    assert "push_upstream_failed" in logs.codes()
    await asyncio.wait_for(core.bodies[0].closed.wait(), 5)
    # max_stream_ms passing: the same, not logged.
    core = Core(frames=[{"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000}], hold=True)
    _, server = next_app(core, push={"max_stream_ms": 100})
    logs = Logged()
    async with live(server) as base:
        s = Session(base)
        got = await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}})
        await s.aclose()
    assert got.result == final(1) and "push_upstream_failed" not in logs.codes()
    # An upstream that ends without close: the final result, logged.
    core = Core(frames=[{"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000}])
    _, server = next_app(core)
    logs = Logged()
    async with live(server) as base:
        s = Session(base)
        got = await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}})
        await s.aclose()
    assert got.result == final(1) and "push_upstream_failed" in logs.codes()


async def test_the_clients_disconnect_or_session_end_aborts_the_upstream_and_ends_nothing() -> None:
    for modern in (True, False):
        core = Core(frames=[{"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000}], hold=True)
        _, server = next_app(core)
        async with live(server, stateless=modern) as base:
            s = Session(base, modern=modern)
            if not modern:
                await s.initialize()
            msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "events/stream",
                "params": s.params({"name": "chat.message", "arguments": {"channel": "eng"}}),
            }
            async with s.http.stream("POST", "/mcp", content=json.dumps(msg), headers=s.headers("events/stream")) as r:
                async for line in r.aiter_lines():
                    if line.startswith("data:"):
                        break  # active arrived: hang up
            if not modern:
                # A 2025-06-18 session keeps a request running after its POST
                # drops (the client may resume it), so the stream ends when
                # the session does (or Wakeshift's newest-wins takeover when
                # the client reconnects).
                await s.http.delete("/mcp", headers=s.headers("x"))
            await s.aclose()
            await asyncio.wait_for(core.bodies[0].closed.wait(), 10)
        assert not any(p == "/events/stream/end" for p, _ in core.requests), modern


async def test_fifty_open_streams_dont_block_tools_call() -> None:
    core = Core(frames=[{"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000}], hold=True)
    _, server = next_app(core)
    async with live(server) as base:
        s = Session(base)
        msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "events/stream",
            "params": s.params({"name": "chat.message", "arguments": {"channel": "eng"}}),
        }
        opened: list[Any] = []
        async with asyncio.timeout(30):
            for _ in range(50):
                cm = s.http.stream("POST", "/mcp", content=json.dumps(msg), headers=s.headers("events/stream"))
                r = await cm.__aenter__()
                opened.append(cm)
                async for line in r.aiter_lines():
                    if line.startswith("data:"):
                        break
            listed = await s.call("tools/list", {})
            assert listed.result is not None
            for b in core.bodies:
                b.write({"type": "close"})
                b.queue.put_nowait(None)
            for cm in opened:
                await cm.__aexit__(None, None, None)
        await s.aclose()
    assert len(core.bodies) == 50


async def test_a_voucher_and_a_cursor_never_reach_a_log(caplog: pytest.LogCaptureFixture) -> None:
    voucher = "wsv_" + "q" * 32
    cursor = "c2.secretcursor.1.mac"
    caplog.set_level(logging.DEBUG)
    for status, error, frames in (
        (200, None, [{"type": "active", "cursor": cursor, "truncated": False, "heartbeat_ms": 20}]),
        (403, {"code": "bad_voucher", "message": "no"}, []),
        (503, None, []),
        (500, None, []),
    ):
        core = Core(status=status, error=error, frames=frames, hold=True)
        _, server = next_app(core)
        async with live(server) as base:
            s = Session(base, user=None)
            await s.call(
                "events/stream",
                {"name": "crawl.finished", "arguments": {"job": "j1", "wakeshift_voucher": voucher}, "cursor": cursor},
            )
            await s.aclose()
    text = caplog.text
    assert voucher not in text and cursor not in text and "secretcursor" not in text


async def test_a_stream_that_created_its_subscription_runs_on_subscribe() -> None:
    seen: list[Subscribed[Any]] = []
    core = Core(
        frames=[
            {"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000, "created": True},
            {"type": "close"},
        ]
    )
    wake, server = next_app(core, on_subscribe=seen.append)
    async with live(server) as base:
        s = Session(base)
        await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}})
        core.frames = [
            {
                "type": "active",
                "cursor": "c2.b.0.m",
                "truncated": False,
                "heartbeat_ms": 30000,
                "created": True,
                "via": "tool:start_crawl",
                "tool": "start_crawl",
            },
            {"type": "close"},
        ]
        s.user = None
        await s.call(
            "events/stream", {"name": "crawl.finished", "arguments": {"job": "j1", "wakeshift_voucher": "wsv_" + "a" * 32}}
        )
        # Resumed (not created): no on_subscribe.
        core.frames = [{"type": "active", "cursor": "c2.b.1.m", "truncated": False, "heartbeat_ms": 30000}, {"type": "close"}]
        await s.call(
            "events/stream",
            {"name": "crawl.finished", "arguments": {"job": "j1", "wakeshift_voucher": "wsv_" + "a" * 32}, "cursor": "c2.b.0.m"},
        )
        await s.aclose()
    for _ in range(100):
        if len(seen) >= 2:
            break
        await asyncio.sleep(0.01)
    assert [(x.via, x.tool, x.user) for x in seen] == [("native", None, "u1"), ("tool", "start_crawl", None)]
    await wake.aclose()


# ── stdio: an explicit cancel ends the subscription ───────────────────────


async def test_stdio_cancel_ends_the_subscription_and_the_other_stream_goes_on() -> None:
    import anyio
    from mcp.client._memory import InMemoryTransport
    from mcp.shared.message import SessionMessage
    from mcp_types import JSONRPCNotification, JSONRPCRequest

    core = Core(frames=[{"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000}], hold=True)
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    wake = Wakeshift(app_key("next"), core="https://core.test", http_client=http)
    wake.define_event("chat.message", description="A new message", args=Channel, payload=Payload, public=True)
    server = MCPServer("next")
    wake.attach(server, identify=lambda r: Identity(user="u1"))
    low = getattr(server, "_mcp_server", server)
    async with InMemoryTransport(low) as (read, write):

        async def send(m: Any) -> None:
            await write.send(SessionMessage(m))

        async def until(pred: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
            with anyio.fail_after(10):
                while True:
                    m = await read.receive()
                    d = m.message.model_dump(by_alias=True, exclude_none=True)
                    if pred(d):
                        return d

        await send(
            JSONRPCRequest(
                jsonrpc="2.0",
                id=0,
                method="initialize",
                params={
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"extensions": {EXTENSION_KEY: {}}},
                    "clientInfo": {"name": "spec-client", "version": "1"},
                },
            )
        )
        await until(lambda d: d.get("id") == 0)
        await send(JSONRPCNotification(jsonrpc="2.0", method="notifications/initialized"))
        for rid in ("a", 2):
            await send(
                JSONRPCRequest(
                    jsonrpc="2.0",
                    id=rid,
                    method="events/stream",
                    params={"name": "chat.message", "arguments": {"channel": "eng"}},
                )
            )
            active = await until(lambda d: d.get("method") == "notifications/events/active")
            assert active["params"]["_meta"][SUBSCRIPTION_ID_META] == rid
        await send(JSONRPCNotification(jsonrpc="2.0", method="notifications/cancelled", params={"requestId": "a"}))
        await asyncio.wait_for(core.bodies[0].closed.wait(), 10)
        for _ in range(200):
            if any(p == "/events/stream/end" for p, _ in core.requests):
                break
            await asyncio.sleep(0.01)
        ends = [b for p, b in core.requests if p == "/events/stream/end"]
        assert len(ends) == 1 and ends[0]["cursor"] == "c2.a.0.m" and ends[0]["name"] == "chat.message"
        # The other stream goes on: an event reaches it, with its own ID.
        core.bodies[1].write({"type": "heartbeat", "cursor": "c2.a.0.m"})
        hb = await until(lambda d: d.get("method") == "notifications/events/heartbeat")
        assert hb["params"]["_meta"][SUBSCRIPTION_ID_META] == 2
        assert not core.bodies[1].closed.is_set()
        core.bodies[1].write({"type": "close"})
        done = await until(lambda d: d.get("id") == 2)
        assert done["result"]["resultType"] == "complete" and done["result"]["_meta"][SUBSCRIPTION_ID_META] == 2


# ── opencode ──────────────────────────────────────────────────────────────


async def test_opencode_declaring_events_where_push_is_served() -> None:
    for native_line in (True, False):
        core = Core()
        reply: dict[str, Any] = {"event_subscription": {"id": "sub-1", "created": True}, "session": "wsc_s1", "pending": "pa_1"}
        if native_line:
            reply["native_line"] = True
        core.replies["/event-subscriptions"] = lambda b, reply=reply: reply
        core.replies["/calls/addition"] = lambda b: {
            "text": "\n".join(f"LINE {n['name']}" for n in b.get("native") or []) or "BRIDGED"
        }
        _, server = next_app(core)
        async with live(server, stateless=False) as base:
            s = Session(base, client="opencode", modern=False)
            await s.initialize({"extensions": {EXTENSION_KEY: {}}})
            tools = await s.call("tools/list", {})
            assert tools.result is not None
            names = {t["name"] for t in tools.result["tools"]}
            assert "subscribe_events" not in names, "opencode is native where push is served"
            listed = await s.call("events/list", {})
            assert listed.result is not None
            assert {e["name"] for e in listed.result["events"]} == {"crawl.finished", "chat.message", "deploy.finished"}
            got = await s.call("tools/call", {"name": "start_crawl", "arguments": {"job": "j1"}})
            await s.aclose()
        assert got.result is not None
        sub = next(b for p, b in core.requests if p == "/event-subscriptions")
        assert sub["call"]["events_ext"] is True
        add = next(b for p, b in core.requests if p == "/calls/addition")
        text = json.dumps(got.result)
        if native_line:
            assert [n["name"] for n in add.get("native") or []] == ["crawl.finished"]
            assert "LINE crawl.finished" in text
        else:
            # The mixed state: subscribe_events hidden, but the block is Wakeshift's bridged one.
            assert not add.get("native")
            assert "BRIDGED" in text


async def test_opencode_where_push_isnt_served_stays_bridged() -> None:
    for features, push in ((["subscribe_vouchers"], None), (["subscribe_vouchers", "push"], False)):
        core = Core(features=features)
        # Even a core that answers native_line gets no native line rendered here.
        core.replies["/event-subscriptions"] = lambda b: {
            "event_subscription": {"id": "sub-1", "created": True},
            "session": "wsc_s1",
            "native_line": True,
        }
        _, server = next_app(core, push=push)
        async with live(server, stateless=False) as base:
            s = Session(base, client="opencode", modern=False)
            await s.initialize({"extensions": {EXTENSION_KEY: {}}})
            tools = await s.call("tools/list", {})
            await s.call("tools/call", {"name": "start_crawl", "arguments": {"job": "j1"}})
            await s.aclose()
        assert tools.result is not None
        assert "subscribe_events" in {t["name"] for t in tools.result["tools"]}, (features, push)
        sub = next(b for p, b in core.requests if p == "/event-subscriptions")
        assert "events_ext" not in sub["call"] and "transport_session" not in sub["call"], (features, push)
        add = next((b for p, b in core.requests if p == "/calls/addition"), {})
        assert not add.get("native"), (features, push)


async def test_opencode_stateless_tools_list_leaves_out_subscribe_events_only_where_push_is_served() -> None:
    for features, hidden in ((["subscribe_vouchers", "push"], True), (["subscribe_vouchers"], False)):
        core = Core(features=features)
        _, server = next_app(core)
        async with live(server) as base:
            s = Session(base, client="opencode")
            tools = await s.call("tools/list", {})
            await s.aclose()
        assert tools.result is not None
        assert ("subscribe_events" not in {t["name"] for t in tools.result["tools"]}) == hidden, features


async def test_opencode_legacy_initialize_declaration() -> None:
    """opencode's default config speaks the legacy protocol (initialize at
    2025-11-25) and declares MCP Events only there, never in a request's
    _meta: where push is served its session's tools/list leaves out
    subscribe_events and its call asks with events_ext; without the
    declaration, where push isn't served, or for a bridged harness, both stay."""
    for why, features, declares, client, native in (
        ("declared, push served", ["subscribe_vouchers", "push"], True, "opencode", True),
        ("not declared", ["subscribe_vouchers", "push"], False, "opencode", False),
        ("declared, push not served", ["subscribe_vouchers"], True, "opencode", False),
        ("declared, a bridged harness", ["subscribe_vouchers", "push"], True, "claude-code", False),
    ):
        core = Core(features=features)
        _, server = next_app(core)
        async with live(server, stateless=False) as base:
            s = Session(base, client=client, modern=False, legacy="2025-11-25")
            await s.initialize({"extensions": {EXTENSION_KEY: {}}} if declares else {})
            tools = await s.call("tools/list", {})
            await s.call("tools/call", {"name": "start_crawl", "arguments": {"job": "j1"}})
            await s.aclose()
        assert tools.result is not None, why
        assert ("subscribe_events" not in {t["name"] for t in tools.result["tools"]}) == native, why
        sub = next(b for p, b in core.requests if p == "/event-subscriptions")
        assert (sub["call"].get("events_ext") is True) == native, why


# ── The option, and the test client ───────────────────────────────────────


def test_the_push_option() -> None:
    wake = Wakeshift("")
    assert push_config(wake, False, {}).on is False
    c = push_config(wake, None, {})
    assert c.on and c.max_stream_ms is None and c.max_heartbeat_ms is None
    c = push_config(wake, {"max_stream_ms": 1000, "max_heartbeat_ms": 20000}, {"VERCEL": "1"})
    assert (c.max_stream_ms, c.max_heartbeat_ms) == (1000, 20000)
    for env, ms in (({"VERCEL": "1"}, 280_000), ({"AWS_LAMBDA_FUNCTION_NAME": "f"}, 840_000), ({"K_SERVICE": "s"}, 3_300_000)):
        assert push_config(wake, None, env).max_stream_ms == ms
    for bad in (True, "on", {"maxStreamMs": 1}, {"max_stream_ms": 0}, {"max_heartbeat_ms": 1.5}, {"max_stream_ms": True}):
        with pytest.raises(Exception, match="push"):
            push_config(wake, bad, {})  # type: ignore[arg-type]


async def test_the_test_client_serves_push() -> None:
    wake = TestClient()
    chat = wake.define_event("chat.message", description="A new message", args=Channel, payload=Payload, public=True)
    server = MCPServer("t")
    wake.attach(server, identify=lambda r: Identity(user="u1"))
    async with live(server) as base:
        s = Session(base)
        listed = await s.call("events/list", {})
        assert listed.result is not None and listed.result["events"][0]["delivery"] == DELIVERY

        async def emit_then_close() -> None:
            for _ in range(200):
                if wake._streams:
                    break
                await asyncio.sleep(0.01)
            await chat.emit({"channel": "eng"}, {"text": "hi"})
            await chat.emit({"channel": "other"}, {"text": "no"})
            wake._close_streams()

        task = asyncio.create_task(emit_then_close())
        got = await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}}, id=3)
        await task
        await s.aclose()
    assert got.result == final(3)
    methods = [n["method"] for n in got.notifications]
    assert methods == ["notifications/events/active", "notifications/events/event"]
    assert got.notifications[1]["params"]["data"] == {"text": "hi"}
    assert wake.subscriptions[0].via == "native" and wake.subscriptions[0].user == "u1"
    await wake.aclose()


async def test_a_stream_that_ends_before_active_is_an_error() -> None:
    # SEP-3415: an events/stream is refused at once or confirmed by active
    # first, never a bare result: an upstream ending (closed, or no close)
    # before active is a retryable -32603, and a terminated before it the
    # stream's JSON-RPC error.
    terminated = {"type": "terminated", "error": {"name": "Forbidden", "message": "revoked", "data": {"reason": "revoked"}}}
    for frames, code, message in (
        ([{"type": "close"}], -32603, "Wakeshift couldn't be reached"),
        ([], -32603, "Wakeshift couldn't be reached"),
        ([{"type": "heartbeat", "cursor": "c"}], -32603, "Wakeshift couldn't be reached"),
        ([terminated], -32024, "revoked"),
    ):
        core = Core(frames=frames)
        _, server = next_app(core)
        async with live(server) as base:
            s = Session(base)
            got = await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}})
            await s.aclose()
        assert got.result is None and got.error is not None, (frames, got)
        assert (got.error["code"], got.error["message"]) == (code, message), (frames, got.error)
        assert all(n["method"] != "notifications/events/active" for n in got.notifications), frames


async def test_a_503_ends_the_stream_after_retry_after_and_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    import time

    import anyio

    from wakeshift._push import _retry_after

    assert [_retry_after(v) for v in ("1", "2.5", "600", None, "", "soon", "-3", "nan")] == [1, 2.5, 10, 0, 0, 0, 0, 0]
    # A retryable error (never a result with no active before it) comes after
    # Retry-After (capped at 10 s) and at most 1 s of jitter.
    core = Core(status=503, error={"code": "unavailable", "message": "too many streams"})
    _, server = next_app(core)
    async with live(server) as base:
        s = Session(base)
        t0 = time.monotonic()
        got = await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}})
        waited = time.monotonic() - t0
        await s.aclose()
    assert got.result is None and got.notifications == []
    assert got.error is not None and got.error["code"] == -32603, got.error
    assert got.error["message"] == "Wakeshift can't open this stream just now: try again shortly"
    assert 1.0 <= waited < 3.5, waited
    # The client hanging up during the wait cancels it: nothing is answered.
    sleeps: list[str] = []
    real_sleep = anyio.sleep

    async def sleep(delay: float) -> None:
        try:
            await real_sleep(delay)
            sleeps.append("done")
        except anyio.get_cancelled_exc_class():
            sleeps.append("cancelled")
            raise

    monkeypatch.setattr(anyio, "sleep", sleep)
    core = Core(status=503, error={"code": "unavailable", "message": "full"}, retry_after="10")
    _, server = next_app(core)
    async with live(server) as base:
        s = Session(base)
        msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "events/stream",
            "params": s.params({"name": "chat.message", "arguments": {"channel": "eng"}}),
        }
        with anyio.move_on_after(0.5):
            await s.http.post("/mcp", content=json.dumps(msg), headers=s.headers("events/stream"))
        await s.aclose()
        for _ in range(300):
            if "cancelled" in sleeps:
                break
            await real_sleep(0.01)
    assert "cancelled" in sleeps and "done" not in sleeps, sleeps


async def test_a_refusal_without_wakeshifts_json_says_push_upstream_failed() -> None:
    for status in (403, 502):
        core = Core(status=status, raw_error="<html>proxy page</html>")
        _, server = next_app(core)
        async with live(server) as base:
            s = Session(base)
            got = await s.call("events/stream", {"name": "chat.message", "arguments": {"channel": "eng"}})
            await s.aclose()
        assert got.error is not None, status
        assert "proxy" not in json.dumps(got.error), got.error
        if status == 403:
            assert got.error["message"].endswith("push upstream failed"), got.error


# ── authorize asked again on an open stream ───────────────────────────────


def test_reauthorize_ms_defaults_and_is_clamped() -> None:
    wake = Wakeshift("")
    assert push_config(wake, None, {}).reauthorize_ms == 300_000
    assert push_config(wake, {"reauthorize_ms": 1000}, {}).reauthorize_ms == 60_000
    assert push_config(wake, {"reauthorize_ms": 120_000}, {}).reauthorize_ms == 120_000
    assert push_config(wake, {"reauthorize_ms": 10 * 3_600_000}, {}).reauthorize_ms == 3_000_000
    for bad in ({"reauthorize_ms": 0}, {"reauthorize_ms": -5}, {"reauthorize_ms": 1.5}, {"reauthorize_ms": "5m"}):
        with pytest.raises(Exception, match="push"):
            push_config(wake, bad, {})  # type: ignore[arg-type]


@dataclass
class Gate:
    """What authorize and identify answer on an open stream, changed by the test."""

    allow: bool = True
    raises: bool = False
    user: str | None = "u1"
    asked: int = 0


def gated_app(core: Core, gate: Gate) -> MCPServer:
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    wake = Wakeshift(app_key("next"), core="https://core.test", http_client=http)

    def authorize(s: Any, a: Any) -> bool:
        gate.asked += 1
        if gate.raises:
            raise RuntimeError("upstream down")
        return gate.allow

    wake.define_event("deploy.finished", description="A deploy finished", args=Env, payload=Payload, authorize=authorize)
    server = MCPServer("next")
    # Asked every 100 ms here (the floor is 1 minute; the test lowers it).
    wake.attach(server, identify=lambda r: Identity(user=gate.user) if gate.user else None, push={"reauthorize_ms": 100})
    return server


async def _open_gated(
    monkeypatch: pytest.MonkeyPatch, change: Callable[[Gate], None], wait: float
) -> tuple[Answer, Core, Gate, Logged]:
    import wakeshift._push as push_mod

    monkeypatch.setattr(push_mod, "_REAUTHORIZE_MIN_MS", 1)
    gate = Gate()
    core = Core(frames=[{"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000}], hold=True)
    server = gated_app(core, gate)
    logs = Logged()

    async def later() -> None:
        while not core.bodies:
            await asyncio.sleep(0.01)
        change(gate)
        await asyncio.sleep(wait)
        core.bodies[0].write({"type": "close"})

    async with live(server) as base:
        s = Session(base)
        task = asyncio.create_task(later())
        got = await s.call("events/stream", {"name": "deploy.finished", "arguments": {"env": "prod"}}, id=11)
        task.cancel()
        await s.aclose()
    return got, core, gate, logs


async def test_a_refusal_on_an_open_stream_ends_it_as_a_revocation(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(g: Gate) -> None:
        g.allow = False

    got, core, _, _ = await _open_gated(monkeypatch, refuse, 5)
    assert [n["method"] for n in got.notifications] == ["notifications/events/active", "notifications/events/terminated"]
    assert got.notifications[1]["params"] == {
        "error": {
            "code": -32024,
            "message": "you may not subscribe to deploy.finished with these arguments",
            "data": {"reason": "revoked"},
        },
        "_meta": {SUBSCRIPTION_ID_META: 11},
    }
    assert got.result == final(11)
    await asyncio.wait_for(core.bodies[0].closed.wait(), 5)  # the upstream aborted
    assert not any(p == "/events/stream/end" for p, _ in core.requests)


async def test_a_user_who_no_longer_passes_identify_ends_the_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    def sign_out(g: Gate) -> None:
        g.user = None

    got, _, _, _ = await _open_gated(monkeypatch, sign_out, 5)
    assert [n["method"] for n in got.notifications][-1] == "notifications/events/terminated"
    assert got.result == final(11)


async def test_an_allowed_or_failing_authorize_keeps_the_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    def nothing(g: Gate) -> None:
        pass

    got, _, gate, _ = await _open_gated(monkeypatch, nothing, 0.5)
    assert [n["method"] for n in got.notifications] == ["notifications/events/active"]
    assert got.result == final(11) and gate.asked >= 3  # at open, then every 100 ms

    def fail(g: Gate) -> None:
        g.raises = True

    got, _, gate, logs = await _open_gated(monkeypatch, fail, 0.5)
    assert [n["method"] for n in got.notifications] == ["notifications/events/active"]
    assert got.result == final(11) and gate.asked >= 3
    assert "authorize_failed" in logs.codes()
    assert not any("upstream down" in line for line in logs)  # the type only, never the message


async def test_the_fixtures_reauthorize_case(monkeypatch: pytest.MonkeyPatch) -> None:
    import wakeshift._push as push_mod

    case = PUSH["reauthorize"]
    wake = Wakeshift("")
    assert (push_mod._REAUTHORIZE_MS, push_mod._REAUTHORIZE_MIN_MS, push_mod._REAUTHORIZE_MAX_MS) == (
        case["default_ms"],
        case["min_ms"],
        case["max_ms"],
    )
    assert push_config(wake, None, {}).reauthorize_ms == case["default_ms"]
    for row in case["clamp"]:
        assert push_config(wake, {"reauthorize_ms": row["reauthorize_ms"]}, {}).reauthorize_ms == row["want_ms"], row
    for bad in case["invalid"]:
        with pytest.raises(Exception, match="push"):
            push_config(wake, {"reauthorize_ms": bad}, {})  # type: ignore[typeddict-item]
    # The scenario, with default_ms passing in 100 ms (a fake clock's worth).
    monkeypatch.setattr(push_mod, "_REAUTHORIZE_MS", 100)
    for dialect, client in (("draft", "spec-client"), ("chatgpt", "openai-mcp")):
        asked: list[str | None] = []

        def authorize(sub: Any, a: Any, asked: list[str | None] = asked) -> bool:
            asked.append(sub.user)
            return len(asked) == 1 and sub.user == case["user"]  # allowed at open, refused at the first check

        core = Core(frames=[{"type": "active", "cursor": "c2.a.0.m", "truncated": False, "heartbeat_ms": 30000}], hold=True)
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
        app = Wakeshift(app_key("next"), core="https://core.test", http_client=http)
        app.define_event("deploy.finished", description="A deploy finished", args=Env, payload=Payload, authorize=authorize)
        server = MCPServer("next")
        app.attach(server, identify=lambda r: Identity(user=r.headers.get("x-user")))
        async with live(server) as base:
            s = Session(base, client=client, user=case["user"])
            with anyio_fail_after(10):
                got = await s.call("events/stream", case["params"], id=case["stream_id"])
            await s.aclose()
        want = json.loads(json.dumps(case["notification"]))
        want["params"]["error"]["code"] = case["code"][dialect]
        assert got.notifications[-1] == want, dialect
        assert got.result == fixture_result(case, case["stream_id"]), dialect
        await asyncio.wait_for(core.bodies[0].closed.wait(), 5)
        assert not any(p == "/events/stream/end" for p, _ in core.requests), dialect
        assert asked[:2] == [case["user"], case["user"]]


# ── Failing rechecks, bounded (_MAX_UNCHECKED_MS) ─────────────────────────


def test_max_unchecked_ms_is_15_minutes_or_three_intervals() -> None:
    from wakeshift._push import _MAX_UNCHECKED_MS, _max_unchecked_ms

    assert _MAX_UNCHECKED_MS == 15 * 60_000
    assert _max_unchecked_ms(PUSH["reauthorize"]["default_ms"]) == 15 * 60_000  # three failed rechecks
    assert _max_unchecked_ms(60_000) == 15 * 60_000
    assert _max_unchecked_ms(10 * 60_000) == 30 * 60_000


@dataclass
class Script:
    """What each recheck of an open stream answers, in order (the last
    repeats): ok, refuse, authorize_raises or identify_raises. The open
    always allows; rechecks counts the rechecks asked."""

    answers: list[str]
    armed: bool = False
    rechecks: int = 0

    def now(self) -> str:
        return self.answers[min(self.rechecks, len(self.answers)) - 1] if self.armed and self.rechecks else "ok"


#: Canary values: none may reach a log.
_CANARY_PARAMS = {"name": "deploy.finished", "arguments": {"env": "env-canary"}, "cursor": "c2.cursor-canary.0.m"}


async def _open_scripted(monkeypatch: pytest.MonkeyPatch, script: Script) -> tuple[Answer, Core, Logged]:
    """A stream rechecked every 100 ms, with the bound at 300 ms (three
    failed rechecks, as 15 minutes is at the default 5)."""
    import wakeshift._push as push_mod

    monkeypatch.setattr(push_mod, "_REAUTHORIZE_MIN_MS", 1)
    monkeypatch.setattr(push_mod, "_MAX_UNCHECKED_MS", 300)
    core = Core(
        frames=[{"type": "active", "cursor": "c2.cursor-canary.0.m", "truncated": False, "heartbeat_ms": 30000}], hold=True
    )
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    wake = Wakeshift(app_key("next"), core="https://core.test", http_client=http)

    def authorize(s: Any, a: Any) -> bool:
        if script.now() == "authorize_raises":
            raise RuntimeError("secret-canary")
        return script.now() != "refuse"

    def identify(r: Request) -> Identity:
        if script.armed:
            script.rechecks += 1
        if script.now() == "identify_raises":
            raise RuntimeError("secret-canary")
        return Identity(user="u1")

    wake.define_event("deploy.finished", description="A deploy finished", args=Env, payload=Payload, authorize=authorize)
    server = MCPServer("next")
    wake.attach(server, identify=identify, push={"reauthorize_ms": 100})
    logs = Logged(logging.DEBUG)

    async def later() -> None:
        while not core.bodies:
            await asyncio.sleep(0.005)
        script.armed = True  # the open's checks are done: each identify now is a recheck
        await asyncio.sleep(5)
        core.bodies[0].write({"type": "close"})  # a safety: the test fails on its notifications

    async with live(server) as base:
        s = Session(base)
        task = asyncio.create_task(later())
        got = await s.call("events/stream", _CANARY_PARAMS, id=11)
        task.cancel()
        await s.aclose()
    return got, core, logs


def _no_canary(logs: Logged) -> None:
    for r in logs.records:
        line = r.getMessage() + repr({k: v for k, v in vars(r).items() if k.startswith("wakeshift_")})
        assert "canary" not in line, line


@pytest.mark.parametrize("fails", ["authorize_raises", "identify_raises"])
async def test_failing_rechecks_end_the_stream_at_the_bound_for_a_reconnect(monkeypatch: pytest.MonkeyPatch, fails: str) -> None:
    script = Script([fails])
    got, core, logs = await _open_scripted(monkeypatch, script)
    # Ended at the third failed recheck, with the final result and no
    # terminated: the client reconnects with its cursor.
    assert script.rechecks == 3
    assert [n["method"] for n in got.notifications] == ["notifications/events/active"]
    assert got.result == final(11)
    await asyncio.wait_for(core.bodies[0].closed.wait(), 5)  # the upstream aborted
    assert not any(p == "/events/stream/end" for p, _ in core.requests)
    ours = [r for r in logs.records if getattr(r, "wakeshift_code", "").startswith("push_recheck")]
    # Rate-limited: the first failed recheck's line (the others counted), then the end.
    assert [(r.levelno, r.wakeshift_code) for r in ours] == [  # type: ignore[attr-defined]
        (logging.WARNING, "push_recheck_failed"),
        (logging.WARNING, "push_recheck_stale"),
    ]
    for r in ours:
        assert sorted(k for k in vars(r) if k.startswith("wakeshift_")) == [
            "wakeshift_code",
            "wakeshift_count",
            "wakeshift_event",
        ]
        assert r.wakeshift_event == "deploy.finished"  # type: ignore[attr-defined]
    _no_canary(logs)


async def test_a_successful_recheck_resets_the_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    script = Script(["authorize_raises", "authorize_raises", "ok", "identify_raises", "identify_raises", "identify_raises"])
    got, _, logs = await _open_scripted(monkeypatch, script)
    assert script.rechecks == 6  # three failed rechecks after the successful one
    assert [n["method"] for n in got.notifications] == ["notifications/events/active"]
    assert got.result == final(11)
    assert "push_recheck_stale" in logs.codes()
    _no_canary(logs)


async def test_a_refusal_after_failing_rechecks_still_ends_as_a_revocation(monkeypatch: pytest.MonkeyPatch) -> None:
    script = Script(["authorize_raises", "refuse"])
    got, _, logs = await _open_scripted(monkeypatch, script)
    assert script.rechecks == 2
    assert [n["method"] for n in got.notifications][-1] == "notifications/events/terminated"
    assert got.notifications[-1]["params"]["error"]["data"] == {"reason": "revoked"}
    assert got.result == final(11)
    assert "push_recheck_stale" not in logs.codes()
    _no_canary(logs)


@pytest.mark.parametrize("client,forbidden", [("spec-client", -32024), ("openai-mcp", -32012)])
@pytest.mark.parametrize("how", ["identify_raises", "authorize_raises", "authorize_refuses", "identify_no_user"])
async def test_a_failing_check_at_open_is_retryable_and_a_refusal_forbidden(how: str, client: str, forbidden: int) -> None:
    """A stream's open (as every reconnect is) whose identify or authorize
    raised answers a retryable Internal error, never Forbidden, so the client
    retries once the app's check answers; a refusal is still Forbidden."""
    from wakeshift._push import CHECK_FAILED

    core = Core(frames=[{"type": "close"}])
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    wake = Wakeshift(app_key("next"), core="https://core.test", http_client=http)

    def authorize(s: Any, a: Any) -> bool:
        if how == "authorize_raises":
            raise RuntimeError("secret-canary")
        return how != "authorize_refuses"

    def identify(r: Request) -> Identity | None:
        if how == "identify_raises":
            raise RuntimeError("secret-canary")
        return None if how == "identify_no_user" else Identity(user="u1")

    wake.define_event("deploy.finished", description="A deploy finished", args=Env, payload=Payload, authorize=authorize)
    server = MCPServer("next")
    wake.attach(server, identify=identify)
    logs = Logged(logging.DEBUG)
    async with live(server) as base:
        s = Session(base, client=client)
        got = await s.call("events/stream", _CANARY_PARAMS, id=11)
        await s.aclose()
    assert got.error is not None
    ours = [r for r in logs.records if getattr(r, "wakeshift_code", "") == "push_open_check_failed"]
    if how.endswith("_raises"):
        assert got.error == {"code": -32603, "message": CHECK_FAILED}
        assert len(ours) == 1 and ours[0].levelno == logging.WARNING
        assert sorted(k for k in vars(ours[0]) if k.startswith("wakeshift_")) == [
            "wakeshift_code",
            "wakeshift_count",
            "wakeshift_event",
        ]
    else:
        assert got.error["code"] == forbidden
        assert not ours
    assert not core.streams()  # no upstream opened
    _no_canary(logs)


def anyio_fail_after(seconds: float) -> Any:
    import anyio

    return anyio.fail_after(seconds)


def test_push_woken_clients_come_from_the_client_table() -> None:
    from wakeshift._spec import _push_client

    assert _push_client("opencode", "")
    assert _push_client("", "opencode/1.2 node")
    assert not _push_client("claude-code", "")
    assert not _push_client("openai-mcp", "")
    assert not _push_client("spec-client", "")
