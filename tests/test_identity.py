"""Identity and authorization (design §2.8, §2.9, PR 7): identify gets a
wakeshift.Request, may be async, runs lazily and at most once per request; a
failing, hanging or (with require_user) anonymous identify refuses
Wakeshift's part of the request and nothing else; each POST of a stateful
session is identified by its own token; authorize has a time limit too."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx2
import pytest
from mcp.server.auth.provider import AccessToken
from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer as OfficialMCPServer
from mcp.types import CallToolResult, TextContent
from pydantic import BaseModel

from wakeshift import Identity, Request, ToolCall, UsageError, Wakeshift
from wakeshift import _events as wevents

from .helpers import FakeCore, Logged, app_key, golden_file, initialize, served


class Table(BaseModel):
    table: str


class Msg(BaseModel):
    text: str


class Boom(Exception):
    pass


def setup(identify: Any, *, require_user: bool = False, auth: bool = False) -> tuple[Wakeshift, Any, FakeCore, Any, Any]:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (
            200,
            {"event_subscription": {"id": "es_1", "user": b["call"].get("user"), "created": True}, "session": "wsc_s"},
        ),
    )
    core.on("POST", "/calls/addition", lambda b: (200, {"text": "Wakeshift: woken."}))
    wake = Wakeshift(
        app_key("cards"), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    )
    news = wake.define_event("table.news", description="News", args=Table, payload=Msg, public=True)
    kw: dict[str, Any] = {}
    if auth:

        class Verifier:
            async def verify_token(self, token: str) -> AccessToken | None:
                if not token.startswith("tok-"):
                    return None
                return AccessToken(token=token, client_id="cli", scopes=[], subject="ada", claims={"turn": token})

        kw = {
            "token_verifier": Verifier(),
            "auth": AuthSettings(
                issuer_url="https://auth.example", resource_server_url="https://cards.example/mcp", validate_token_resource=False
            ),
        }
    server = OfficialMCPServer("cards", **kw)
    attached = wake.attach(server, identify=identify, require_user=require_user)
    return wake, server, core, attached, news


async def test_identify_is_lazy_and_runs_once_per_request() -> None:
    asked: list[Request] = []

    async def identify(r: Request) -> Identity:  # async is fine
        asked.append(r)
        await asyncio.sleep(0)
        return Identity(user="ada")

    wake, server, core, attached, news = setup(identify)
    seen: list[Any] = []

    @attached.tool(subscribes=[news])
    async def watch(call: ToolCall) -> str:
        seen.append(call.user)
        await call.subscribe(news, {"table": "oak"})
        await call.subscribe(news, {"table": "elm"})
        return "ok"

    @server.tool()
    async def plain() -> str:
        return "plain"

    async with served(server, stateless=False) as (_, rpc):
        await initialize(rpc, "claude-code")
        await rpc("ping")
        await rpc("tools/list", {})
        await rpc("tools/call", {"name": "plain", "arguments": {}})
        assert asked == [], "nothing needed identify yet"
        await rpc("tools/call", {"name": "watch", "arguments": {}}, headers={"X-Who": "ada"})
        assert len(asked) == 1, "once for the call, however much it subscribes"
        assert asked[0].headers.get("x-who") == "ada" and asked[0].client_name == "claude-code"
        await rpc("tools/call", {"name": "list_subscriptions", "arguments": {}})
        assert len(asked) == 2
    assert seen == ["ada"]
    assert "x-who" not in repr(asked[0]) and "ada" not in repr(asked[0])


@pytest.mark.parametrize("failure", ["raises", "hangs", "anonymous"])
async def test_a_failed_identify_refuses_wakeshifts_part_and_nothing_else(failure: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(wevents.LIMITS, "identify", 0.05)

    async def identify(r: Request) -> Identity | None:
        if failure == "raises":
            raise Boom("boom-canary")
        if failure == "hangs":
            await asyncio.sleep(1)
        return None

    logged = Logged(logging.INFO)
    wake, server, core, attached, news = setup(identify, require_user=failure == "anonymous")
    ran: list[str] = []
    errors: list[UsageError] = []

    @attached.tool(subscribes=[news])
    async def watch(call: ToolCall) -> str:
        ran.append("vendor code")
        try:
            await call.subscribe(news, {"table": "oak"})
        except UsageError as err:
            errors.append(err)
        return "vendor reply"

    async with served(server, stateless=False) as (_, rpc):
        await initialize(rpc, "openai-mcp")
        res = await rpc("tools/call", {"name": "watch", "arguments": {}}, headers={"User-Agent": "claude-code/2"})
        builtin = await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "table.news", "arguments": {"table": "oak"}}}
        )
        delivery = {"mode": "webhook", "url": "https://agent.example/cb", "secret": "whsec_dGVzdA=="}
        native = await rpc("events/subscribe", {"name": "table.news", "arguments": {"table": "oak"}, "delivery": delivery})
    assert ran == ["vendor code"]
    assert res["result"]["content"][0]["text"] == "vendor reply", "the tool's own reply, without a block"
    assert len(errors) == 1 and errors[0].code == ("user_required" if failure == "anonymous" else "identify_failed")
    assert "boom-canary" not in str(errors[0])
    if failure == "anonymous":
        assert "require_user" in str(errors[0])
    assert builtin["result"]["isError"] is True
    assert native["error"]["code"] == -32012
    assert not core.bodies("/event-subscriptions") and not core.bodies("/events/subscribe")
    if failure != "anonymous":
        assert logged.codes().count("identify_error") == 1, "rate-limited"
        rec = next(r for r in logged.records if getattr(r, "wakeshift_code", "") == "identify_error")
        assert rec.wakeshift_error_type == ("Boom" if failure == "raises" else "TimeoutError")  # type: ignore[attr-defined]
    assert not any("boom-canary" in m for m in logged)


async def test_each_post_of_a_stateful_session_is_identified_by_its_own_token() -> None:
    """SEC L3: a stateful session whose second POST carries a new token gets
    the new identity, not the one from initialize."""
    seen: list[str | None] = []

    def identify(r: Request) -> Identity:
        seen.append(getattr(r.token, "token", None))
        return Identity(user=(r.token.claims or {}).get("turn") if r.token else None)

    wake, server, core, attached, news = setup(identify, auth=True)

    @attached.tool(subscribes=[news])
    async def watch(call: ToolCall) -> str:
        await call.subscribe(news, {"table": "oak"})
        return "ok"

    async with served(server, stateless=False) as (_, rpc):
        rpc.headers["Authorization"] = "Bearer tok-1"
        await initialize(rpc, "claude-code")
        await rpc("tools/call", {"name": "watch", "arguments": {}})
        rpc.headers["Authorization"] = "Bearer tok-2"
        await rpc("tools/call", {"name": "watch", "arguments": {}})
        rpc.headers["Authorization"] = "Bearer forged"
        refused = await rpc("tools/call", {"name": "watch", "arguments": {}})
    assert seen == ["tok-1", "tok-2"]
    assert [b["call"]["user"] for b in core.bodies("/event-subscriptions")] == ["tok-1", "tok-2"]
    assert refused == {"status": 401}, "a forged token never reaches identify"


async def test_authorize_that_hangs_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(wevents.LIMITS, "authorize", 0.05)
    core = FakeCore(default_ok=True)
    wake = Wakeshift(
        app_key("cards"), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    )
    got: list[Any] = []

    async def authorize(s: Any, a: Table) -> bool:
        got.append((s.user, s.native, a.table, repr(s)))
        await asyncio.sleep(1)
        return True

    wake.define_event("table.news", description="News", args=Table, payload=Msg, authorize=authorize)
    server = OfficialMCPServer("cards")
    logged = Logged()
    wake.attach(server, identify=lambda r: Identity(user="ada"))
    async with served(server) as (_, rpc):
        res = await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "table.news", "arguments": {"table": "oak"}}}
        )
    assert res["result"]["isError"] is True and "may not subscribe" in res["result"]["content"][0]["text"]
    assert got[0][:3] == ("ada", False, "oak") and "request" not in got[0][3]
    assert logged.codes() == ["authorize_failed"]
    assert not core.bodies("/event-subscriptions")


@pytest.mark.parametrize("op", ["identify_throws", "identify_times_out", "require_user_anonymous"])
async def test_client_checks_next_for_identify(op: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(wevents.LIMITS, "identify", 0.05)
    c = {c["op"]: c for c in golden_file("errors.json")["client_checks_next"]["cases"]}[op]

    async def identify(r: Request) -> Identity | None:
        if op == "identify_throws":
            raise Boom("boom-canary")
        if op == "identify_times_out":
            await asyncio.sleep(1)
        return None

    wake, server, core, attached, news = setup(identify, require_user=op == "require_user_anonymous")
    errors: list[UsageError] = []

    @attached.tool(subscribes=[news])
    async def check(call: ToolCall) -> str:
        try:
            await call.subscribe(news, {"table": "oak"})
        except UsageError as err:
            errors.append(err)
        return "ok"

    async with served(server) as (_, rpc):
        await rpc("tools/call", {"name": "check", "arguments": {}})
    assert errors and errors[0].code == c["code"] and c["says"] in str(errors[0]), (op, errors)
    assert c.get("must_not_contain", "\0") not in str(errors[0])


async def test_a_refused_caller_reads_words_for_the_agent_and_the_log_says_it_once() -> None:
    """The agent reads the same neutral words as in every SDK, never the
    developer's fix; identify_error names the error's type once, ends with
    its fix, and is rate-limited (one line, then a count)."""

    def boom(r: Request) -> Identity:
        raise Boom("token=secret")

    wake, server, core, attached, news = setup(boom)
    logged = Logged()
    async with served(server) as (_, rpc):
        a = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {}})
        b = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {}})
    for r in (a, b):
        assert r["result"]["isError"] and r["result"]["content"][0]["text"] == (
            "This app couldn't verify who you are, so it can't subscribe you to anything now."
        )
    assert logged.codes() == ["identify_error"]
    msg = logged[0]
    assert msg.count("Boom") == 1 and "secret" not in msg and msg.count("10 seconds") == 1, msg
    _, server, _, _, _ = setup(lambda r: Identity(), require_user=True)
    async with served(server) as (_, rpc):
        c = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {}})
    assert c["result"]["content"][0]["text"] == "This app needs you to sign in before it can subscribe you to anything."


@pytest.mark.parametrize("how", ["raises", "is_error"])
async def test_a_tool_that_passes_the_refusal_on_reads_neutral_words(how: str) -> None:
    """A tool that passes call.subscribe's identify refusal on (raising it,
    or in an error result): the agent reads neutral words, never the
    developer's fix."""

    def boom(r: Request) -> Identity:
        raise Boom("boom-canary")

    wake, server, core, attached, news = setup(boom)

    @attached.tool(subscribes=[news])
    async def watch(call: ToolCall) -> Any:
        try:
            await call.subscribe(news, {"table": "oak"})
        except UsageError as err:
            if how == "raises":
                raise
            return CallToolResult(content=[TextContent(type="text", text=str(err))], isError=True)
        return "watching"

    async with served(server) as (_, rpc):
        res = await rpc("tools/call", {"name": "watch", "arguments": {}})
    texts = [c.get("text") for c in res["result"]["content"]]
    assert res["result"]["isError"] is True
    assert not any("Check identify" in (t or "") or "boom-canary" in (t or "") for t in texts), texts
    if how == "is_error":
        # (Raised, MCPServer answers with its own generic words first.)
        assert texts == ["This app couldn't verify who you are, so it can't subscribe you to anything now."], texts
