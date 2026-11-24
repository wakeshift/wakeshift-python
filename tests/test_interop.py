"""mcp-events-interop.json (with the Go and Node SDKs): where the SDKs used to
answer MCP Events requests differently. Over HTTP, on protocol 2026-07-28,
as a stateless MCPServer serves it."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx2
import pytest
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel, Field

from wakeshift import Identity, UsageError
from wakeshift._clients import CLIENT_TABLE
from wakeshift._spec import (
    EXTENSION_KEY,
    REVISION,
    ParamHeader,
    evaluate_client,
    header_value,
    native_client,
    param_header_refusal,
    param_headers_of,
)

from .helpers import FakeCore, fake_wake, golden_file

G = golden_file("mcp-events-interop.json")
DELIVERY = {"mode": "webhook", "url": "https://agent.example/hooks/1", "secret": "whsec_dGVzdC1zZWNyZXQtdGVzdC1zZWNyZXQtdGVzdA=="}


class ChatArgs(BaseModel):
    channel: str


class ChatMessage(BaseModel):
    text: str


class DeployArgs(BaseModel):
    """mcp-events-interop.json mcp_param_http: x-mcp-header through pydantic's json_schema_extra."""

    region: str = Field(json_schema_extra={"x-mcp-header": "Region"})
    attempt: int | None = Field(None, json_schema_extra={"x-mcp-header": "Attempt"})


class Deployed(BaseModel):
    region: str
    attempt: int


def chat_core() -> FakeCore:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/events/poll",
        lambda b: (
            200,
            {"events": [], "cursor": "c1.0", "truncated": False, "has_more": False, "next_poll_ms": 10000, "created": False},
        ),
    )
    core.on(
        "POST",
        "/events/subscribe",
        lambda b: (
            200,
            {"id": "sub_1", "refresh_before": "2026-11-11T13:00:00Z", "cursor": "c1.0", "truncated": False, "created": False},
        ),
    )
    core.on("POST", "/events/unsubscribe", lambda b: (200, {"ended": True}))
    return core


@asynccontextmanager
async def chat_app(core: FakeCore, with_event: bool = True, with_deploy: bool = False) -> AsyncIterator[httpx2.AsyncClient]:
    """chat.message (public, unless with_event is False), identify says u1,
    served statelessly over Streamable HTTP."""
    wake, _ = fake_wake(core)
    if with_event:
        wake.define_event(
            "chat.message", description="A new message in a channel", args=ChatArgs, payload=ChatMessage, public=True
        )
    if with_deploy:
        wake.define_event("deploy.finished", description="A deploy finished", args=DeployArgs, payload=Deployed, public=True)
    server = MCPServer("chat")
    wake.attach(server, identify=lambda r: Identity(user="u1"))
    app = server.streamable_http_app(stateless_http=True, json_response=True)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000") as client:
            yield client


def meta(client: str, declares: bool) -> dict[str, Any]:
    return {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": client, "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {"extensions": {EXTENSION_KEY: {}}} if declares else {},
    }


async def post(
    client: httpx2.AsyncClient,
    method: str,
    params: dict[str, Any],
    headers: dict[str, str] | None = None,
    more: list[tuple[str, str]] | None = None,
) -> tuple[int, dict[str, Any]]:
    h = [
        ("Content-Type", "application/json"),
        ("Accept", "application/json, text/event-stream"),
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", method),
        *(headers or {}).items(),
        *(more or []),
    ]
    r = await client.post("/mcp", content=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}), headers=h)
    return r.status_code, r.json()


def test_the_revision_is_the_accepted_sep() -> None:
    assert G["revision"] == REVISION


def test_decoding_mcp_name() -> None:
    for c in G["mcp_name_decode"]["cases"]:
        assert header_value(c["header"]) == c["value"], c["header"]


async def test_mcp_name_must_match_and_may_be_missing() -> None:
    core = chat_core()
    async with chat_app(core) as client:
        for c in G["mcp_name"]["cases"]:
            if c["method"] == "events/list":
                params: dict[str, Any] = {"_meta": meta(c["client"], c["declares"])}
            else:
                params = {"name": "chat.message", "arguments": {"channel": "eng"}, "_meta": meta(c["client"], c["declares"])}
            if c["method"] == "events/subscribe":
                params["delivery"] = DELIVERY
            h = {"User-Agent": f"{c['client']}/1"}
            if c["header"] is not None:
                h["Mcp-Name"] = c["header"]
            more = [("Mcp-Name", c["second_header"])] if c.get("second_header") is not None else None
            before = len(core.requests)
            status, msg = await post(client, c["method"], params, h, more)
            refused = status == 400 and msg.get("error", {}).get("code") == -32020
            assert ("refused" if refused else "passed") == c["want"], (c["name"], status, msg)
            if refused:
                assert not [p for _, p, _ in core.requests[before:] if p.startswith("/events/")], c["name"]


async def test_events_stream_answers_unsupported_push() -> None:
    core = chat_core()
    want = G["stream"]["error"]
    async with chat_app(core) as client:
        for dialect, name in (("draft", "some-draft-client"), ("chatgpt", "openai-mcp")):
            for event in ("chat.message", "no.such.event"):
                params = {**G["stream"]["params"], "name": event, "_meta": meta(name, False)}
                _, msg = await post(client, "events/stream", params, {"User-Agent": f"{name}/1"})
                assert msg.get("error") == {"code": want["code"][dialect], "message": want["message"], "data": want["data"]}, (
                    dialect,
                    event,
                    msg,
                )
    assert not [p for _, p, _ in core.requests if p.startswith("/events/")]


async def test_no_events_is_method_not_found() -> None:
    async with chat_app(chat_core(), with_event=False) as client:
        for m in G["no_events"]["methods"]:
            params = {"name": "chat.message", "arguments": {}, "_meta": meta("tanstack-ai", True)}
            _, msg = await post(client, m, params, {"Mcp-Name": "chat.message"})
            assert msg.get("error", {}).get("code") == G["no_events"]["code"], (m, msg)


def test_one_native_client_rule() -> None:
    # The client table's cases: the SDK's rule, with the case's serves push
    # (push served for the request), and the table's whole verdict (native,
    # dialect, matched entry), push or not.
    for c in G["native_client"]["cases"]:
        push = c.get("serves_push", False)
        got = native_client(c["client"], c["user_agent"], c["meta"], c["session_extensions"], push)
        assert got is c["native"], c["name"]
        v = evaluate_client(c["client"], c["user_agent"], c["meta"], c["session_extensions"], push)
        assert (v.native, v.dialect, v.entry) == (c["native"], c["dialect"], c["entry"]), c["name"]


def test_client_table_is_the_shared_one() -> None:
    # The generated constants are mcp-events-clients.json (regenerated in Wakeshift's
    # server repository by go run ./internal/mcpevents/clientsgen/gen),
    # without its documentation (_about, reason, source, about) and webhook_only (only
    # Wakeshift reads it), which the SDKs don't ship.
    def strip(v: Any) -> Any:
        if isinstance(v, dict):
            return {
                k: strip(x) for k, x in v.items() if not (k.startswith("_") or k in ("reason", "source", "about", "webhook_only"))
            }
        if isinstance(v, list):
            return [strip(x) for x in v]
        return v

    assert CLIENT_TABLE == strip(golden_file("mcp-events-clients.json"))


def test_x_mcp_header_annotations_are_checked_when_an_event_is_defined() -> None:
    for c in G["mcp_param_schema"]["cases"]:
        _, problem = param_headers_of(c["schema"])
        assert (problem is None) == c["valid"], (c["name"], problem)
    core = chat_core()
    wake, _ = fake_wake(core)
    payload = {"type": "object", "properties": {"ratio": {"type": "number"}}}
    # A dict schema, as given.
    with pytest.raises(UsageError) as e:
        wake.define_event(
            "bad.number",
            description="d",
            args={"type": "object", "properties": {"ratio": {"type": "number", "x-mcp-header": "Ratio"}}},
            payload=payload,
            topic="t",
        )
    assert e.value.code == "config" and "x-mcp-header on ratio" in str(e.value)

    # A pydantic model's json_schema_extra.
    class BadToken(BaseModel):
        ratio: str = Field(json_schema_extra={"x-mcp-header": "Ra tio"})

    with pytest.raises(UsageError) as e:
        wake.define_event("bad.token", description="d", args=BadToken, payload=ChatMessage, topic="t")
    assert e.value.code == "config"

    class Team(BaseModel):
        id: str = Field(json_schema_extra={"x-mcp-header": "Team-Id"})

    class Nested(BaseModel):
        team: Team

    ok = wake.define_event("good.event", description="d", args=Nested, payload=ChatMessage, topic="t")
    assert ok._param_headers == [ParamHeader(("team", "id"), "Team-Id", "string")]


def test_mcp_param_headers_must_match_the_annotated_arguments() -> None:
    annotated, problem = param_headers_of(G["mcp_param"]["schema"])
    assert problem is None
    for c in G["mcp_param"]["cases"]:
        fields: dict[str, list[str]] = {}
        for k, v in c["headers"]:
            fields.setdefault(k.lower(), []).append(v)
        refused = param_header_refusal(annotated, c["arguments"], fields.get)
        assert ("passed" if refused is None else "refused") == c["want"], (c["name"], refused)


async def test_mcp_param_headers_over_http() -> None:
    h = G["mcp_param_http"]
    core = chat_core()
    async with chat_app(core, with_deploy=True) as client:
        _, listed = await post(client, "events/list", {"_meta": meta("tanstack-ai", True)})
        [d] = [e for e in listed["result"]["events"] if e["name"] == h["event"]]
        assert d["inputSchema"] == h["input_schema"]
        for c in h["cases"]:
            name = c.get("event", h["event"])
            params: dict[str, Any] = {"name": name, "arguments": c["arguments"], "_meta": meta("tanstack-ai", True)}
            if c["method"] == "events/subscribe":
                params["delivery"] = DELIVERY
            headers = {"User-Agent": "tanstack-ai/1", **({} if c.get("no_name") else {"Mcp-Name": name}), **c["headers"]}
            before = len(core.requests)
            status, msg = await post(client, c["method"], params, headers)
            refused = status == 400 and msg.get("error", {}).get("code") == -32020
            assert ("refused" if refused else "passed") == c["want"], (c["name"], status, msg)
            if refused:
                assert not [p for _, p, _ in core.requests[before:] if p.startswith("/events/")], c["name"]


async def test_mcp_param_headers_are_not_expected_off_http() -> None:
    # stdio and in-memory transports carry no headers: the annotation is ignored there.
    import anyio
    from mcp.client._memory import InMemoryTransport
    from mcp.shared.message import SessionMessage
    from mcp_types import JSONRPCNotification, JSONRPCRequest

    core = chat_core()
    wake, _ = fake_wake(core)
    wake.define_event("deploy.finished", description="A deploy finished", args=DeployArgs, payload=Deployed, public=True)
    server = MCPServer("chat")
    wake.attach(server, identify=lambda r: Identity(user="u1"))
    low = getattr(server, "_mcp_server", server)
    async with InMemoryTransport(low) as (read, write):

        async def answer(rid: int) -> dict[str, Any]:
            with anyio.fail_after(10):
                while True:
                    m = await read.receive()
                    assert isinstance(m, SessionMessage), m
                    d = m.message.model_dump(by_alias=True, exclude_none=True)
                    if d.get("id") == rid:
                        return d

        init = {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "spec-client", "version": "1"}}
        await write.send(SessionMessage(JSONRPCRequest(jsonrpc="2.0", id=0, method="initialize", params=init)))
        await answer(0)
        await write.send(SessionMessage(JSONRPCNotification(jsonrpc="2.0", method="notifications/initialized")))
        poll = {"name": "deploy.finished", "arguments": {"region": "us-west1", "attempt": 2}, "cursor": None}
        await write.send(SessionMessage(JSONRPCRequest(jsonrpc="2.0", id=1, method="events/poll", params=poll)))
        got = await answer(1)
    assert "result" in got, got
    assert [p for _, p, _ in core.requests if p == "/events/poll"]


async def post_raw(
    client: httpx2.AsyncClient, method: str, params: dict[str, Any], headers: dict[str, str], path: str = "/mcp"
) -> tuple[int, dict[str, Any]]:
    """One JSON-RPC request with exactly these headers (and the content headers)."""
    h = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream", **headers}
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    r = await client.post(path, content=body, headers=h)
    try:
        return r.status_code, r.json()
    except ValueError:
        for line in r.text.splitlines():
            if line.startswith("data:"):
                m = json.loads(line[5:])
                if "result" in m or "error" in m:
                    return r.status_code, m
        return r.status_code, {}


def transport_params(method: str, meta_version: str | None) -> dict[str, Any]:
    m = meta("tanstack-ai", True)
    if meta_version is None:
        del m["io.modelcontextprotocol/protocolVersion"]
    else:
        m["io.modelcontextprotocol/protocolVersion"] = meta_version
    if method == "events/list":
        return {"_meta": m}
    params: dict[str, Any] = {"name": "chat.message", "arguments": {"channel": "eng"}, "_meta": m}
    if method == "events/subscribe":
        params["delivery"] = DELIVERY
    return params


async def check_transport_case(client: httpx2.AsyncClient, core: FakeCore, c: dict[str, Any], extra: dict[str, str]) -> None:
    h = {"User-Agent": "tanstack-ai/1", **extra, **c["headers"]}
    if c["method"] != "events/list":
        h["Mcp-Name"] = "chat.message"
    before = len(core.requests)
    status, msg = await post_raw(client, c["method"], transport_params(c["method"], c["meta_version"]), h)
    refused = status == 400 and msg.get("error", {}).get("code") == -32020
    assert ("refused" if refused else "passed") == c["want"], (c["name"], status, msg)
    if refused:
        assert not [p for _, p, _ in core.requests[before:] if p.startswith("/events/")], c["name"]


async def test_transport_headers_on_events() -> None:
    # The stateless 2026-07-28 server serves no legacy request: those cases run on a session below.
    core = chat_core()
    async with chat_app(core) as client:
        for c in G["transport_headers"]["cases"]:
            if not c.get("legacy"):
                await check_transport_case(client, core, c, {})


@asynccontextmanager
async def legacy_session(
    core: FakeCore, stateless: bool, json_response: bool
) -> AsyncIterator[tuple[httpx2.AsyncClient, dict[str, str]]]:
    """chat_app's server on an initialize session (protocol 2025-11-25): the
    client, and the headers its later requests carry."""
    wake, _ = fake_wake(core)
    wake.define_event("chat.message", description="A new message in a channel", args=ChatArgs, payload=ChatMessage, public=True)
    wake.define_event("deploy.finished", description="A deploy finished", args=DeployArgs, payload=Deployed, public=True)
    server = MCPServer("chat")
    wake.attach(server, identify=lambda r: Identity(user="u1"))
    app = server.streamable_http_app(stateless_http=stateless, json_response=json_response)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000") as client:
            init = {
                "protocolVersion": "2025-11-25",
                "capabilities": {"extensions": {EXTENSION_KEY: {}}},
                "clientInfo": {"name": "tanstack-ai", "version": "1"},
            }
            r = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": init},
                headers={"Accept": "application/json, text/event-stream"},
            )
            sid = r.headers.get("mcp-session-id")
            headers = {"MCP-Protocol-Version": "2025-11-25", **({"Mcp-Session-Id": sid} if sid else {})}
            await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                headers={"Accept": "application/json, text/event-stream", **headers},
            )
            yield client, headers


async def test_header_mismatch_is_400_on_initialize_sessions() -> None:
    # The legacy transport answers a handler's error with HTTP 200: Wakeshift's refusals still get 400.
    params = {"name": "chat.message", "arguments": {"channel": "eng"}}
    for stateless, json_response in ((False, True), (False, False), (True, True), (True, False)):
        core = chat_core()
        async with legacy_session(core, stateless, json_response) as (client, headers):
            where = (stateless, json_response)
            for c in G["transport_headers"]["cases"]:
                if c.get("legacy"):
                    await check_transport_case(client, core, c, {k: v for k, v in headers.items() if k != "MCP-Protocol-Version"})
            for extra, want in (
                ({"Mcp-Name": "chat.other"}, 400),
                ({"Mcp-Method": "events/subscribe"}, 400),
                ({"Mcp-Name": "chat.message", "Mcp-Method": "events/poll"}, 200),
                ({}, 200),
            ):
                before = len(core.requests)
                status, msg = await post_raw(client, "events/poll", params, {**headers, **extra})
                assert status == want, (where, extra, status, msg)
                if want == 400:
                    assert msg["error"]["code"] == -32020, (where, extra, msg)
                    assert not [p for _, p, _ in core.requests[before:] if p.startswith("/events/")], (where, extra)
                else:
                    assert "result" in msg, (where, extra, msg)
            # Mcp-Param (mcp_param_http): refused with 400 here too.
            deploy = {"name": "deploy.finished", "arguments": {"region": "us-west1"}}
            for extra, want in (({}, 400), ({"Mcp-Param-Region": "eu-west1"}, 400), ({"Mcp-Param-Region": "us-west1"}, 200)):
                before = len(core.requests)
                status, msg = await post_raw(client, "events/poll", deploy, {**headers, "Mcp-Name": "deploy.finished", **extra})
                assert status == want, (where, extra, status, msg)
                if want == 400:
                    assert msg["error"]["code"] == -32020, (where, extra, msg)
                    assert not [p for _, p, _ in core.requests[before:] if p.startswith("/events/")], (where, extra)
