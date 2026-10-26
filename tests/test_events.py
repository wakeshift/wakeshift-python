"""Events against a fake core: defining, emitting and serving them, natively
and bridged (the same cases as the Node SDK's events tests)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel, Field

from wakeshift import (
    EXTENSION_KEY,
    LIST_TOOL_DESCRIPTION,
    SUBSCRIBE_TOOL_DESCRIPTION,
    UNSUBSCRIBE_TOOL_DESCRIPTION,
    Identity,
    WakeshiftError,
    speaks_events,
)
from wakeshift import spec as wspec

from .helpers import FakeCore, fake_wake, initialize, served


class ChatArgs(BaseModel):
    channel: str
    sender: str | None = Field(None, alias="from")


class ChatMessage(BaseModel):
    sender: str = Field(alias="from")
    text: str


class Turn(BaseModel):
    hole: list[str]
    to_call: int = Field(alias="toCall")


def chat_event(wake: Any) -> Any:
    return wake.define_event(
        "chat.message",
        description="A new message in a channel",
        args=ChatArgs,
        payload=ChatMessage,
        text=lambda m, a: f"{m.sender}: {m.text}",
        authorize=lambda s, a: a.channel != "secret",
    )


def turn_event(wake: Any) -> Any:
    return wake.define_event(
        "game.turn",
        description="It's your move",
        # A JSON Schema works as well as a model.
        args={"type": "object", "properties": {"table": {"type": "string"}}, "required": ["table"]},
        payload=Turn,
        text=lambda t, a: f"Your move: {t.to_call} to call",
        personal=True,
    )


def text_of(res: dict[str, Any]) -> str:
    return res["result"]["content"][0]["text"]


def test_define_event_makes_the_manifests_event_type() -> None:
    wake, _ = fake_wake()
    chat = chat_event(wake)
    assert chat.manifest["topic"] == "ev/chat.message/{channel}"
    assert chat.manifest["filter"] == {"from": "{from}"}
    assert chat.manifest["arguments"]["type"] == "object"
    assert "$schema" not in chat.manifest["arguments"]
    assert chat.manifest["arguments"]["required"] == ["channel"]
    assert "personal" not in chat.manifest
    turn = turn_event(wake)
    assert turn.manifest["topic"] == "ev/game.turn/{table}"
    assert turn.manifest["personal"] is True
    assert turn.definition()["delivery"] == ["webhook"]


def test_define_event_refuses_what_wakeshift_would() -> None:
    wake, _ = fake_wake()

    class P(BaseModel):
        x: str

    class N(BaseModel):
        n: float

    class Y(BaseModel):
        y: str | None = None

    empty: dict[str, Any] = {"type": "object", "properties": {}}

    def text(p: Any, a: Any) -> str:
        return "x"

    with pytest.raises(ValueError, match="lowercase dot-separated"):
        wake.define_event("Chat", description="d", args=empty, payload=P, text=text)
    with pytest.raises(ValueError, match="Wakeshift's"):
        wake.define_event("wakeshift.message", description="d", args=empty, payload=P, text=text)
    with pytest.raises(ValueError, match="description"):
        wake.define_event("a.b", description="", args=empty, payload=P, text=text)
    with pytest.raises(ValueError, match="routes occurrences"):
        wake.define_event("a.c", description="d", args=N, payload=P, text=text)
    with pytest.raises(ValueError, match="payload field"):
        wake.define_event("a.d", description="d", args=Y, payload=P, text=text)
    wake.define_event("a.e", description="d", args=empty, payload=P, text=text)
    with pytest.raises(ValueError, match="twice"):
        wake.define_event("a.e", description="d", args=empty, payload=P, text=text)


async def test_emit_checks_the_payload_publishes_the_manifest_first_and_sends_the_rendered_line() -> None:
    core = FakeCore(default_ok=True)
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 2, "duplicate": False}))
    wake, _ = fake_wake(core)
    chat = chat_event(wake)
    with pytest.raises(WakeshiftError) as e:
        await chat.emit({"channel": "eng"}, {"from": "ada"})
    assert e.value.code == "bad_payload"
    with pytest.raises(WakeshiftError, match="isn't personal"):
        await chat.emit({"channel": "eng"}, {"from": "ada", "text": "hi"}, to="u1")
    out = await chat.emit({"channel": "eng"}, {"from": "ada", "text": "shipped"}, id="m1")
    assert (out.matched, out.duplicate) == (2, False)
    assert [f"{m} {p}" for m, p, _ in core.requests] == ["PUT /manifest", "POST /events/emit"]
    assert core.requests[0][2]["events"][0]["name"] == "chat.message"
    assert core.requests[1][2] == {
        "name": "chat.message",
        "arguments": {"channel": "eng"},
        "payload": {"from": "ada", "text": "shipped"},
        "text": "ada: shipped",
        "id": "m1",
    }
    # Published once: the next emit goes straight out.
    await chat.emit({"channel": "eng"}, {"from": "bob", "text": "ok"})
    assert len(core.bodies("/manifest")) == 1


async def test_a_personal_event_is_emitted_to_one_subscriber() -> None:
    core = FakeCore(default_ok=True)
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 1}))
    wake, _ = fake_wake(core)
    turn = turn_event(wake)
    with pytest.raises(WakeshiftError, match="personal: pass to="):
        await turn.emit({"table": "juniper"}, {"hole": ["As", "Kd"], "toCall": 2})
    await turn.emit({"table": "juniper"}, {"hole": ["As", "Kd"], "toCall": 2}, to="guest:abc")
    body = core.bodies("/events/emit")[-1]
    assert body["to"] == "guest:abc"
    assert body["payload"] == {"hole": ["As", "Kd"], "toCall": 2}


def test_speaks_events_the_extension_the_older_key_or_chatgpt() -> None:
    assert speaks_events({"extensions": {EXTENSION_KEY: {}}}, "x")
    assert speaks_events({"events": {}}, "x")
    assert speaks_events({}, "openai-mcp")
    assert not speaks_events({}, "claude-code")


async def test_bridged_clients_get_subscribe_events_every_client_gets_unsubscribe_and_list() -> None:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (
            200,
            {"event_subscription": {"id": "es1", "created": True}, "addition": {"text": "Start the Monitor tool now."}},
        ),
    )
    core.on(
        "POST",
        "/event-subscriptions/list",
        lambda b: (
            200,
            {
                "subscriptions": [
                    {
                        "id": "es1",
                        "event": "chat.message",
                        "arguments": {"channel": "eng"},
                        "destination": "conversation",
                        "created_at": "2026-10-21T10:00:00Z",
                    }
                ]
            },
        ),
    )
    core.on("POST", "/event-subscriptions/end", lambda b: (200, {"ended": True}))
    core.on("POST", "/event-subscriptions/unsubscribe", lambda b: (200, {"ended": True}))
    wake, _ = fake_wake(core)
    chat_event(wake)
    turn_event(wake)
    server = MCPServer("chat")
    wake.attach(server, identify=lambda ctx: Identity(user="u1"))
    async with served(server, stateless=False) as (_, rpc):
        await initialize(rpc, "claude-code")
        tools = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
        assert sorted(tools) == ["list_subscriptions", "subscribe_events", "unsubscribe_events"]
        sub = tools["subscribe_events"]
        assert sub["description"].startswith(SUBSCRIBE_TOOL_DESCRIPTION)
        assert "chat.message(channel, from): A new message in a channel" in sub["description"]
        assert "session_key" in sub["inputSchema"]["properties"]
        assert tools["unsubscribe_events"]["description"] == UNSUBSCRIBE_TOOL_DESCRIPTION
        assert tools["list_subscriptions"]["description"] == LIST_TOOL_DESCRIPTION

        r = await rpc(
            "tools/call",
            {
                "name": "subscribe_events",
                "arguments": {"event": "chat.message", "arguments": {"channel": "eng"}, "harness": "claude-code"},
            },
        )
        assert text_of(r) == 'Subscribed to chat.message {"channel":"eng"}.\n\nStart the Monitor tool now.'
        call = core.bodies("/event-subscriptions")[0]
        assert call["name"] == "chat.message"
        assert call["arguments"] == {"channel": "eng"}
        assert call["call"]["user"] == "u1"
        assert call["call"]["client"] == "claude-code"
        assert call["call"]["harness"] == "claude-code"

        # authorize refuses, bad arguments are explained, nothing reaches the core.
        before = len(core.requests)
        refused = await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "chat.message", "arguments": {"channel": "secret"}}}
        )
        assert refused["result"]["isError"] is True
        assert "may not subscribe" in text_of(refused)
        bad = await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "chat.message", "arguments": {"room": "eng"}}}
        )
        assert bad["result"]["isError"] is True
        assert "arguments for chat.message" in text_of(bad)
        assert len(core.requests) == before

        listed = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {}})
        assert re.search(r'- es1: chat\.message \{"channel":"eng"\}, wakes a conversation, since 2026-10-21', text_of(listed))
        assert "Stopped subscription es1" in text_of(
            await rpc("tools/call", {"name": "unsubscribe_events", "arguments": {"id": "es1"}})
        )
        assert core.bodies("/event-subscriptions/end")[0] == {"user": "u1", "id": "es1"}
        un = await rpc(
            "tools/call", {"name": "unsubscribe_events", "arguments": {"event": "chat.message", "arguments": {"channel": "eng"}}}
        )
        assert "Stopped chat.message" in text_of(un)


async def test_a_client_declaring_the_older_events_key_isnt_offered_subscribe_events() -> None:
    wake, _ = fake_wake()
    chat_event(wake)
    server = MCPServer("chat")
    wake.attach(server, identify=lambda ctx: Identity(user="u1"))
    async with served(server, stateless=False) as (_, rpc):
        await initialize(rpc, "some-client", {"events": {}})
        tools = sorted(t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"])
        assert tools == ["list_subscriptions", "unsubscribe_events"]


async def test_a_personal_event_needs_a_known_subscriber() -> None:
    wake, _ = fake_wake()
    turn_event(wake)
    server = MCPServer("poker")
    wake.attach(server, identify=lambda ctx: Identity())
    async with served(server, stateless=False) as (_, rpc):
        await initialize(rpc, "claude-code")
        r = await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "game.turn", "arguments": {"table": "juniper"}}}
        )
        assert r["result"]["isError"] is True and "personal" in text_of(r)
        listed = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {}})
        assert listed["result"]["isError"] is True


async def test_a_client_that_speaks_mcp_events_subscribes_natively_and_isnt_offered_subscribe_events() -> None:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/events/subscribe",
        lambda b: (
            200,
            {"id": "sub_1", "refresh_before": "2026-10-22T10:00:00.000Z", "cursor": "c1", "truncated": False, "created": True},
        ),
    )
    core.on("POST", "/events/unsubscribe", lambda b: (200, {"ended": False}))
    wake, _ = fake_wake(core)
    chat_event(wake)
    server = MCPServer("chat")
    wake.attach(server, identify=lambda ctx: Identity(user="u1"))
    async with served(server, stateless=False) as (_, rpc):
        init = await initialize(rpc, "openai-mcp")
        caps = init["result"]["capabilities"]
        assert caps["events"] == {"listChanged": False}
        assert caps["extensions"][EXTENSION_KEY] == {"listChanged": False}
        names = sorted(t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"])
        assert names == ["list_subscriptions", "unsubscribe_events"]

        lst = (await rpc("events/list", {}))["result"]
        assert lst["resultType"] == "complete"
        assert lst["events"][0]["name"] == "chat.message"
        assert lst["events"][0]["delivery"] == ["webhook"]

        delivery = {"mode": "webhook", "url": "https://chatgpt.example/cb", "secret": "whsec_x"}
        res = await rpc(
            "events/subscribe", {"name": "chat.message", "arguments": {"channel": "eng"}, "delivery": delivery, "ttlMs": 3600000}
        )
        assert res["result"] == {
            "resultType": "complete",
            "id": "sub_1",
            "refreshBefore": "2026-10-22T10:00:00Z",
            "cursor": "c1",
            "truncated": False,
        }
        body = core.bodies("/events/subscribe")[0]
        assert body["dialect"] == "chatgpt"
        assert body["call"]["user"] == "u1"
        assert body["ttl_ms"] == 3600000
        assert body["delivery"] == delivery

        async def code(params: dict[str, Any], method: str = "events/subscribe") -> int:
            out = await rpc(method, params)
            assert "error" in out, out
            return out["error"]["code"]

        assert await code({"name": "chat.nope", "arguments": {}, "delivery": delivery}) == -32011, "ChatGPT's NotFound"
        assert await code({"name": "chat.message", "arguments": {"channel": "secret"}, "delivery": delivery}) == -32012, (
            "ChatGPT's Forbidden"
        )
        assert await code({"name": "chat.message", "arguments": {"room": "x"}, "delivery": delivery}) == -32602
        assert await code({"name": "chat.message", "arguments": {"channel": "eng"}, "delivery": {"mode": "poll"}}) == -32602, (
            "ChatGPT has no Unsupported"
        )
        # The Mcp-Name header must match params.name.
        mismatch = await rpc(
            "events/subscribe",
            {"name": "chat.message", "arguments": {"channel": "eng"}, "delivery": delivery},
            headers={"Mcp-Name": "other.event"},
        )
        assert mismatch["error"]["code"] == wspec.HEADER_MISMATCH
        # Unsubscribing nothing: ChatGPT gets the empty result it always has.
        un = await rpc("events/unsubscribe", {"name": "chat.message", "arguments": {"channel": "eng"}, "delivery": delivery})
        assert un["result"] == {"resultType": "complete"}


async def test_draft_clients_get_the_drafts_codes() -> None:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/events/subscribe",
        lambda b: (
            400,
            {"error": {"code": "callback_failed", "message": "the callback didn't echo the challenge", "reason": "challenge"}},
        ),
    )
    core.on("POST", "/events/unsubscribe", lambda b: (200, {"ended": False}))
    wake, _ = fake_wake(core)
    chat_event(wake)
    server = MCPServer("chat")
    wake.attach(server, identify=lambda ctx: Identity(user="u1"))
    async with served(server, stateless=False) as (_, rpc):
        await initialize(rpc, "some-draft-client", capabilities={"extensions": {EXTENSION_KEY: {}}})
        delivery = {"mode": "webhook", "url": "https://x.example/cb", "secret": "whsec_x"}
        cb = (await rpc("events/subscribe", {"name": "chat.message", "arguments": {"channel": "eng"}, "delivery": delivery}))[
            "error"
        ]
        assert cb["code"] == -32027
        assert cb["data"] == {"reason": "challenge"}
        assert (await rpc("events/unsubscribe", {"name": "chat.message", "arguments": {"channel": "eng"}, "delivery": delivery}))[
            "error"
        ]["code"] == -32023
        assert (await rpc("events/poll", {}))["error"]["code"] == -32026
        # A client that declared the extension isn't offered subscribe_events either.
        names = sorted(t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"])
        assert names == ["list_subscriptions", "unsubscribe_events"]


def test_spec_constants_match_the_cores() -> None:
    go_file = Path(__file__).resolve().parents[3] / "internal" / "mcpevents" / "event" / "spec.go"
    if not go_file.exists():
        pytest.skip("not in the Wakeshift repository")
    go = go_file.read_text()

    def s(name: str) -> str | None:
        m = re.search(name + r'\s*=\s*"([^"]+)"', go)
        return m.group(1) if m else None

    assert wspec.REVISION == s("Revision")
    assert wspec.EXTENSION_KEY == s("ExtensionKey")
    assert wspec.LEGACY_KEY == s("LegacyKey")
    assert wspec.RESULT_TYPE == s("ResultType")
    assert wspec.TEXT_META == s("TextMeta")
    assert wspec.NAME_HEADER == (s("NameHeader") or "").lower()
    draft = re.search(
        r"return Codes\{NotFound: (-\d+), Forbidden: (-\d+), ResourceExhausted: (-\d+), Unsupported: (-\d+), CallbackEndpoint: (-\d+)\}",
        go,
    )
    assert draft
    d = wspec.codes_for("draft")
    assert [d.not_found, d.forbidden, d.resource_exhausted, d.unsupported, d.callback_endpoint] == [
        int(x) for x in draft.groups()
    ]
    chatgpt = re.search(
        r"return Codes\{NotFound: (-\d+), Forbidden: (-\d+), ResourceExhausted: InvalidParams, Unsupported: InvalidParams, CallbackEndpoint: (-\d+)\}",
        go,
    )
    assert chatgpt
    c = wspec.codes_for("chatgpt")
    assert [c.not_found, c.forbidden, c.callback_endpoint] == [int(x) for x in chatgpt.groups()]
    assert c.unsupported == wspec.INVALID_PARAMS


async def test_an_authorize_that_raises_refuses_on_both_paths_and_the_app_hears_why() -> None:
    warnings: list[str] = []
    core = FakeCore(default_ok=True)
    wake, _ = fake_wake(core, on_warning=warnings.append)

    class Args(BaseModel):
        channel: str

    class Msg(BaseModel):
        text: str

    def authorize(s: Any, a: Any) -> bool:
        raise RuntimeError("the ACL service is down")

    wake.define_event(
        "chat.message", description="A message", args=Args, payload=Msg, text=lambda m, a: m.text, authorize=authorize
    )
    server = MCPServer("chat")
    wake.attach(server, identify=lambda ctx: Identity(user="u1"))
    async with served(server, stateless=False) as (_, rpc):
        await initialize(rpc, "claude-code")
        r = await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "chat.message", "arguments": {"channel": "eng"}}}
        )
        assert r["result"]["isError"] is True and "may not subscribe" in text_of(r)
    async with served(server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        delivery = {"mode": "webhook", "url": "https://x.example/cb", "secret": "whsec_x"}
        out = await rpc("events/subscribe", {"name": "chat.message", "arguments": {"channel": "eng"}, "delivery": delivery})
        assert out["error"]["code"] == -32012
    assert len(warnings) == 2
    assert re.search(r"authorize for chat\.message raised.*ACL service is down", warnings[0])
    assert not [p for _, p, _ in core.requests if "subscribe" in p], "nothing reached the core"


def test_attach_needs_events_and_an_mcpserver() -> None:
    wake, _ = fake_wake()
    with pytest.raises(ValueError, match="at least one event"):
        wake.attach(MCPServer("x"))
    chat_event(wake)
    with pytest.raises(TypeError, match="MCPServer"):
        wake.attach(object())


async def test_an_events_subscribe_and_end_a_tool_subscribes_its_own_caller_and_ends_a_subscription_by_id() -> None:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (
            200,
            {
                "event_subscription": {"id": "es_1", "event": "game.turn", "arguments": {"table": "t1"}, "created": True},
                "session": "wsc_abc",
                "addition": {"text": "Watch with the Monitor tool."},
                "agent": {"harness": "claude-code", "via": "client", "claimed": None},
            },
        ),
    )
    core.on("POST", "/event-subscriptions/end", lambda b: (200, {"ended": b["id"] == "es_1"}))
    wake, _ = fake_wake(core)
    turn = turn_event(wake)
    with pytest.raises(WakeshiftError, match="personal: pass user"):
        await turn.subscribe(None, {"table": "t1"}, client="claude-code")
    with pytest.raises(ValueError, match="arguments for game.turn"):
        await turn.subscribe(None, {}, client="claude-code", user="u1")
    sub = await turn.subscribe(None, {"table": "t1"}, client="claude-code", user="u1", wake_args={"session_key": "wsc_abc"})
    assert (sub.id, sub.created, sub.session, sub.agent.harness) == ("es_1", True, "wsc_abc", "claude-code")
    res = sub.result("Seated.")
    texts = [c.text for c in res.content]
    assert texts[0] == "Seated.\n\nWatch with the Monitor tool."
    req = core.bodies("/event-subscriptions")[0]
    assert req["name"] == "game.turn"
    assert req["arguments"] == {"table": "t1"}
    assert req["call"]["user"] == "u1"
    assert req["call"]["session"] == "wsc_abc"
    assert await turn.end("u1", "es_1") is True
    assert await turn.end("u1", "es_other") is False
    assert core.bodies("/event-subscriptions/end") == [{"user": "u1", "id": "es_1"}, {"user": "u1", "id": "es_other"}]


async def test_an_events_unsubscribe_ends_the_callers_own_subscription_with_those_arguments() -> None:
    core = FakeCore(default_ok=True)
    core.on("POST", "/event-subscriptions/unsubscribe", lambda b: (200, {"ended": b["arguments"] == {"table": "t1"}}))
    wake, _ = fake_wake(core)
    turn = turn_event(wake)
    with pytest.raises(ValueError, match="arguments for game.turn"):
        await turn.unsubscribe(None, {}, client="claude-code", user="u1")
    assert core.bodies("/event-subscriptions/unsubscribe") == []
    assert (
        await turn.unsubscribe(None, {"table": "t1"}, client="claude-code", user="u1", wake_args={"session_key": "wsc_abc"})
        is True
    )
    assert await turn.unsubscribe(None, {"table": "t2"}, client="claude-code", user="u1") is False
    req = core.bodies("/event-subscriptions/unsubscribe")[0]
    assert req["name"] == "game.turn"
    assert req["arguments"] == {"table": "t1"}
    assert req["call"] == {"client": "claude-code", "user": "u1", "session": "wsc_abc"}
