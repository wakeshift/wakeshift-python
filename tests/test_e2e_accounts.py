"""End to end against a real core, for an app whose users sign in with
Wakeshift (accounts: "wakeshift"): run with scripts/e2e.sh, whose harness
(TestSDKHarness's accounts part) registers the app and signs agents in.
Skipped without it."""

from __future__ import annotations

import json
import os
from typing import Any

import pytest
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import Agent, Wakeshift, WakeshiftError, is_code

from .helpers import initialize, served

pytestmark = pytest.mark.e2e

HARNESS = os.environ.get("WAKESHIFT_SDK_HARNESS", "")


class Channel(BaseModel):
    channel: str


class Message(BaseModel):
    sender: str
    text: str


def accounts() -> tuple[str, dict[str, Any]]:
    if not HARNESS:
        pytest.skip("set WAKESHIFT_SDK_HARNESS (scripts/e2e.sh)")
    with open(HARNESS) as f:
        h = json.load(f)
    a = h.get("accounts")
    assert a, "the harness has no accounts part: scripts/e2e.sh sets WAKESHIFT_SDK_HARNESS_ACCOUNTS_MCP"
    return h["core"], a


async def test_events_for_an_app_whose_users_sign_in_with_wakeshift() -> None:
    """A subscription is the agent's its exchanged token names, under its
    person's consent; a personal event reaches only the agent it's to; another
    app's consent doesn't count; serve_events' tools act as the agent the agent
    option names."""
    core, a = accounts()
    wake = Wakeshift(a["key"], core=core, mcp_url=a["mcp"], on_warning=lambda _m: None)
    msg = wake.define_event(
        "chat.message",
        description="A message for you in a channel.",
        args=Channel,
        payload=Message,
        text=lambda m, x: f"{m.sender} in #{x.channel}: {m.text}",
        personal=True,
    )
    await wake.publish_manifest(
        {
            "description": "Lounge, for the SDKs' tests.",
            "accounts": "wakeshift",
            "agent": {"wake": [{"pattern": "app.lounge.*", "description": "Messages"}], "post": [], "ttl_days": 30},
        }
    )

    # verify_access_token: Wakeshift's token for this app's MCP URL; not another app's.
    claims = await wake.verify_access_token(a["ada_claude"]["token"])
    assert f"{claims.wakeshift_tenant}/{claims.wakeshift_agent}" == a["ada_claude"]["user"]
    with pytest.raises(ValueError, match="another app"):
        await wake.verify_access_token(a["other_token"])
    with pytest.raises(ValueError, match="invalid token"):
        await wake.verify_access_token("not.a.token")
    # Wakeshift's header and claims, with a signature of the wrong length.
    h, c, _ = a["ada_claude"]["token"].split(".")
    with pytest.raises(ValueError, match="invalid token"):
        await wake.verify_access_token(f"{h}.{c}.AAAA")

    claude = await wake.exchange(a["ada_claude"]["token"])
    codex = await wake.exchange(a["ada_codex"]["token"])
    bob = await wake.exchange(a["bob_claude"]["token"])
    assert claude.user() == a["ada_claude"]["user"]
    assert claude.token not in repr(claude), "an Agent's repr never shows its token"

    def as_(agent: Agent, client: str, session: str) -> dict[str, Any]:
        return {"agent": agent, "client": client, "wake_args": {"session_key": session}}

    # Each agent subscribes its own session.
    sub = await msg.subscribe(None, {"channel": "eng"}, **as_(claude, "claude-code", a["ada_claude"]["session"]))
    assert sub.created and sub.id
    await msg.subscribe(None, {"channel": "eng"}, **as_(bob, "claude-code", a["bob_claude"]["session"]))
    # Not another agent's session.
    with pytest.raises(WakeshiftError) as e:
        await msg.subscribe(None, {"channel": "eng"}, **as_(codex, "codex", a["ada_claude"]["session"]))
    assert is_code(e.value, "unknown_session")
    # Not under another app's consent (Ada's sign-in to "other").
    other = Wakeshift(a["other_key"], core=core, mcp_url=a["other_mcp"], on_warning=lambda _m: None)
    elsewhere = await other.exchange(a["other_token"])
    with pytest.raises(WakeshiftError) as e:
        await msg.subscribe(None, {"channel": "eng"}, **as_(elsewhere, "claude-code", a["ada_claude"]["session"]))
    assert is_code(e.value, "bad_agent_token")

    # A personal emit reaches only the agent it's to.
    assert (await msg.emit({"channel": "eng"}, {"sender": "bob", "text": "hi ada"}, to=claude.user())).matched == 1
    assert (await msg.emit({"channel": "eng"}, {"sender": "ada", "text": "hi bob"}, to=bob.user())).matched == 1
    assert (await msg.emit({"channel": "eng"}, {"sender": "ada", "text": "hi codex"}, to=codex.user())).matched == 0

    # serve_events with the agent option: the tools act as the signed-in agent.
    server = MCPServer("lounge")
    wake.serve_events(server, agent=lambda _ctx: codex)
    async with served(server) as (_, rpc):
        await initialize(rpc, "codex-mcp-client")

        async def said(name: str, args: dict[str, Any]) -> str:
            res = await rpc("tools/call", {"name": name, "arguments": args})
            return "\n".join(c.get("text", "") for c in res["result"]["content"])

        session = a["ada_codex"]["session"]
        out = await said("subscribe_events", {"event": "chat.message", "arguments": {"channel": "ops"}, "session_key": session})
        assert "Subscribed to chat.message" in out, out
        assert (await msg.emit({"channel": "ops"}, {"sender": "ada", "text": "now codex"}, to=codex.user())).matched == 1
        assert 'chat.message {"channel":"ops"}' in await said("list_subscriptions", {})
        out = await said("unsubscribe_events", {"event": "chat.message", "arguments": {"channel": "ops"}, "session_key": session})
        assert "Stopped chat.message" in out, out

    # A caller that isn't signed in gets no subscription.
    def signed_out(_ctx: Any) -> Agent:
        raise ValueError("not signed in")

    anon_server = MCPServer("lounge")
    wake.serve_events(anon_server, agent=signed_out)
    async with served(anon_server) as (_, anon):
        await initialize(anon, "claude-code")
        res = await anon(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "chat.message", "arguments": {"channel": "eng"}}}
        )
        assert res["result"].get("isError") is True


async def test_a_current_key_exchanges_tokens_and_signs_in() -> None:
    """The lounge app's key in the current format: Wakeshift says which app
    it is, token exchange works, and the token endpoint takes its sign-in
    (a made-up code is invalid_grant, not a refused client)."""
    core, a = accounts()
    wake = Wakeshift(a["new_key"], core=core, mcp_url=a["mcp"], on_warning=lambda _m: None)
    claude = await wake.exchange(a["ada_claude"]["token"])
    assert claude.user() == a["ada_claude"]["user"]
    assert (await wake.app()).id == "lounge"
    fresh = Wakeshift(a["new_key"], core=core, mcp_url=a["mcp"], on_warning=lambda _m: None)
    with pytest.raises(WakeshiftError) as e:
        await fresh.sign_in(code="wsc_not-a-code", verifier="v" * 43, redirect_uri="https://lounge.test/auth/wakeshift/callback")
    assert is_code(e.value, "invalid_grant")
    assert fresh.app_id == "lounge"
