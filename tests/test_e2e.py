"""End to end against a real core: run with scripts/e2e.sh in Wakeshift's own
repository, which starts one with a self-serve app and points WAKESHIFT_SDK_HARNESS at its URL and test key. Skipped without it."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import queue
import re
import secrets
import signal
import subprocess
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
import pytest
from mcp.server.mcpserver import Context, MCPServer
from pydantic import BaseModel

from wakeshift import GuestAuth, Identity, Wakeshift, WakeshiftError, guest_of, is_code
from wakeshift.key import b64url

from .helpers import Rpc, initialize, served

pytestmark = pytest.mark.e2e

HARNESS = os.environ.get("WAKESHIFT_SDK_HARNESS", "")


def harness() -> tuple[str, str]:
    if not HARNESS:
        pytest.skip("set WAKESHIFT_SDK_HARNESS (scripts/e2e.sh)")
    with open(HARNESS) as f:
        h = json.load(f)
    return h["core"], h["key"]


class Lines:
    """The command Claude Code's Monitor is given (the session's wait in a
    loop), run in sh as Monitor runs it: one JSON line per event."""

    def __init__(self, cmd: str) -> None:
        self.q: queue.Queue[dict[str, Any]] = queue.Queue()
        self.proc = subprocess.Popen(["sh", "-c", cmd], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True)
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        for raw in self.proc.stdout:
            line = raw.decode().strip()
            if not line:
                continue
            ev = json.loads(line)
            if ev.get("type") == "event":
                self.q.put(ev)

    async def next(self, timeout: float = 5.0) -> dict[str, Any]:
        return await asyncio.to_thread(self.q.get, True, timeout)

    async def expect(self, subject: str) -> dict[str, Any]:
        ev = await self.next()
        assert ev["subject"] == subject
        return ev

    async def expect_nothing(self, timeout: float = 0.7) -> None:
        with pytest.raises(queue.Empty):
            await self.next(timeout)

    def stop(self) -> None:
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)  # its curl too
        except ProcessLookupError:
            pass
        self.proc.wait()


@contextmanager
def receiver() -> Iterator[tuple[str, str, list[dict[str, Any]]]]:
    """A callback that speaks MCP Events webhooks as a client would: it checks
    the Standard Webhooks signature with its secret, echoes the verification
    challenge, and keeps each occurrence."""
    key = secrets.token_bytes(32)
    got: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_: Any) -> None:
            pass

        def do_POST(self) -> None:
            raw = self.rfile.read(int(self.headers.get("content-length") or 0))
            signed = f"{self.headers.get('webhook-id', '')}.{self.headers.get('webhook-timestamp', '')}.".encode() + raw
            want = "v1," + base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()
            if want not in (self.headers.get("webhook-signature") or "").split(" "):
                self.send_response(401)
                self.end_headers()
                return
            body = json.loads(raw)
            if body.get("type") == "verification":
                out = json.dumps({"challenge": body["challenge"]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)
                return
            if not body.get("type"):
                got.append({"name": body.get("name"), "data": body.get("data"), "meta": body.get("_meta")})
            self.send_response(202)
            self.end_headers()

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{srv.server_address[1]}/cb", "whsec_" + base64.b64encode(key).decode(), got
    finally:
        srv.shutdown()
        srv.server_close()


def text_of(r: dict[str, Any]) -> str:
    assert "result" in r, r
    return "\n".join(c.get("text", "") for c in r["result"]["content"])


MONITOR = re.compile(r"with this command: (u='https?://[^']+/v1/stream/wait\?[^']+'; c=; p=\$PPID; .*?; done) \.")


def monitor_of(r: dict[str, Any]) -> str:
    m = MONITOR.search(text_of(r))
    assert m, text_of(r)
    return m.group(1)


async def eventually(what: str, fn: Any, timeout: float = 5.0) -> Any:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        v = fn()
        if v:
            return v
        if loop.time() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.1)


class Build(BaseModel):
    conclusion: str
    url: str


class Repo(BaseModel):
    repo: str


class Table(BaseModel):
    table: str


class Turn(BaseModel):
    hole: list[str]
    toCall: float


async def test_events_defined_served_emitted_bridged_and_native() -> None:
    core, key = harness()
    wake = Wakeshift(key, core=core)
    assert wake.mode == "test"
    build = wake.define_event(
        "ci.build",
        description="A CI run on a repo finished",
        args=Repo,
        payload=Build,
        text=lambda b, a: f"CI {b.conclusion} on {a.repo}",
        authorize=lambda s, a: a.repo != "secret",
    )
    turn = wake.define_event(
        "game.turn",
        description="It's your move at a table",
        args=Table,
        payload=Turn,
        text=lambda p, a: f"Your move: {' '.join(p.hole)}, {p.toCall:g} to call",
        personal=True,
    )

    # One server; each connection's user comes from a header here (an app's own sign-in in practice).
    server = MCPServer("ci-bot")
    wake.attach(
        server, identify=lambda ctx: Identity(user=ctx.request.headers.get("x-user") if ctx.request is not None else None)
    )
    lines: list[Lines] = []
    try:
        async with served(server, stateless=False) as (client, _):

            async def connect(name: str, user: str) -> Rpc:
                rpc = Rpc(client, "/mcp", headers={"X-User": user})
                await initialize(rpc, name)
                return rpc

            async def tool(rpc: Rpc, name: str, args: dict[str, Any]) -> dict[str, Any]:
                return await rpc("tools/call", {"name": name, "arguments": args})

            # Bridged (Claude Code): subscribe_events, then a wake through the Monitor.
            claude = await connect("claude-code", "bob")
            sub = await tool(claude, "subscribe_events", {"event": "ci.build", "arguments": {"repo": "acme"}})
            assert re.match(r'^Subscribed to ci\.build \{"repo":"acme"\}\.', text_of(sub))
            bob = Lines(monitor_of(sub))
            lines.append(bob)
            refused = await tool(claude, "subscribe_events", {"event": "ci.build", "arguments": {"repo": "secret"}})
            assert refused["result"]["isError"] is True
            assert (await build.emit({"repo": "acme"}, {"conclusion": "failure", "url": "https://ci.example/1"})).matched == 1
            got = await bob.expect("CI failure on acme")
            assert got["data"]["url"] == "https://ci.example/1"
            assert got["from"] == f"app/{wake.app_id}"

            # Personal: two players at one table; each turn reaches only its player.
            p1 = await connect("claude-code", "p1")
            p2 = await connect("claude-code", "p2")
            p1_lines = Lines(monitor_of(await tool(p1, "subscribe_events", {"event": "game.turn", "arguments": {"table": "t1"}})))
            lines.append(p1_lines)
            p2_sub = await tool(p2, "subscribe_events", {"event": "game.turn", "arguments": {"table": "t1"}})
            p2_lines = Lines(monitor_of(p2_sub))
            lines.append(p2_lines)
            m = re.search(r'session_key="(wsc_[a-z2-7]+)"', text_of(p2_sub))
            assert m, text_of(p2_sub)
            p2_session = m.group(1)
            with pytest.raises(WakeshiftError, match="personal"):
                await turn.emit({"table": "t1"}, {"hole": ["As", "Kd"], "toCall": 2})
            assert (await turn.emit({"table": "t1"}, {"hole": ["As", "Kd"], "toCall": 2}, to="p1")).matched == 1
            assert (await p1_lines.expect("Your move: As Kd, 2 to call"))["data"]["hole"] == ["As", "Kd"]
            await p2_lines.expect_nothing()
            await turn.emit({"table": "t1"}, {"hole": ["7c", "7d"], "toCall": 4}, to="p2")
            await p2_lines.expect("Your move: 7c 7d, 4 to call")
            await p1_lines.expect_nothing()

            # An event's publish URL: a POST's body is the payload, checked against
            # the schema; a personal event's goes to the one subscriber it's for.
            async with httpx.AsyncClient() as web:
                build_url = await build.publish_url({"repo": "acme"})
                assert build_url.event == "ci.build" and build_url.url.startswith(core + "/v1/publish/")
                posted = await web.post(build_url.url, json={"conclusion": "success", "url": "https://ci.example/2"})
                assert posted.status_code == 202 and posted.json()["matched"] == 1
                via_url = await bob.expect('ci.build: {"conclusion":"success","url":"https://ci.example/2"}')
                assert via_url["data"]["url"] == "https://ci.example/2"
                bad = await web.post(build_url.url, json={"conclusion": 1})
                assert bad.status_code == 400 and bad.json()["error"]["code"] == "bad_payload"
                await wake.revoke_publish_url(build_url.id)
                assert (await web.post(build_url.url, json={"conclusion": "success", "url": "x"})).status_code == 410
                with pytest.raises(WakeshiftError, match="personal"):
                    await turn.publish_url({"table": "t1"})
                p2_url = await turn.publish_url({"table": "t1"}, to="p2")
                assert (await web.post(p2_url.url, json={"hole": ["8c", "8d"], "toCall": 1})).json()["matched"] == 1
                await p2_lines.expect('game.turn: {"hole":["8c","8d"],"toCall":1}')
                await p1_lines.expect_nothing()
                await wake.revoke_publish_url(p2_url.id)

            # Native (a client that speaks MCP Events): events/subscribe, a webhook.
            with receiver() as (url, secret, hook):
                chatgpt = await connect("openai-mcp", "p3")
                offered = sorted(t["name"] for t in (await chatgpt("tools/list", {}))["result"]["tools"])
                assert offered == ["list_subscriptions", "unsubscribe_events"]
                listed = (await chatgpt("events/list", {}))["result"]
                assert sorted(e["name"] for e in listed["events"]) == ["ci.build", "game.turn"]
                native = await chatgpt(
                    "events/subscribe",
                    {
                        "name": "game.turn",
                        "arguments": {"table": "t1"},
                        "delivery": {"mode": "webhook", "url": url, "secret": secret},
                        "ttlMs": 3600000,
                    },
                )
                assert "result" in native, native
                assert native["result"]["resultType"] == "complete"
                assert native["result"]["id"].startswith("sub_")
                await turn.emit({"table": "t1"}, {"hole": ["Qh", "Qs"], "toCall": 0}, to="p3")
                await eventually("p3's webhook", lambda: len(hook) == 1, 15)
                assert hook[0]["name"] == "game.turn"
                assert hook[0]["data"] == {"hole": ["Qh", "Qs"], "toCall": 0}
                assert "Your move: Qh Qs, 0 to call" in (hook[0]["meta"] or {}).get("ai.wakeshift/text", "")
                # p1's turn never reaches p3's webhook.
                await turn.emit({"table": "t1"}, {"hole": ["2c", "3c"], "toCall": 2}, to="p1")
                await p1_lines.expect("Your move: 2c 3c, 2 to call")
                await asyncio.sleep(0.7)
                assert len(hook) == 1

                # list_subscriptions and unsubscribe_events, by id (the native one) and by arguments (bridged).
                mine = text_of(await tool(chatgpt, "list_subscriptions", {}))
                found = re.search(r'- ([0-9a-f-]{36}): game\.turn \{"table":"t1"\}, delivered to 127\.0\.0\.1', mine)
                assert found, mine
                assert "Stopped subscription" in text_of(await tool(chatgpt, "unsubscribe_events", {"id": found.group(1)}))
                assert "no subscriptions" in text_of(await tool(chatgpt, "list_subscriptions", {}))
            stopped = await tool(
                p2, "unsubscribe_events", {"event": "game.turn", "arguments": {"table": "t1"}, "session_key": p2_session}
            )
            assert "Stopped game.turn" in text_of(stopped)
            assert (await turn.emit({"table": "t1"}, {"hole": ["9c", "9d"], "toCall": 2}, to="p2")).matched == 0
    finally:
        for ln in lines:
            ln.stop()


async def test_guest_sign_in_against_a_real_core() -> None:
    core, key = harness()
    wake = Wakeshift(key, core=core)
    server = MCPServer("ci-bot")

    @server.tool()
    async def watch(repo: str, ctx: Context) -> Any:
        who = guest_of(ctx)
        assert who is not None
        sub = await wake.subscribe(wake.call(ctx, client=who.client, user=who.user), f"guests/{repo}")
        return sub.result(f"Watching {repo} for {who.agent}.")

    guest = GuestAuth(wake, "http://127.0.0.1:8000/mcp")
    guest_granted = GuestAuth(wake, "http://127.0.0.1:8000/mcp", grants=True)
    # Which GuestAuth serves: the plain one, then the one with grants.
    current = [guest]

    def wrap(app: Any) -> Any:
        plain, granted = guest.wrap(app), guest_granted.wrap(app)
        return lambda scope, receive, send: (plain if current[0] is guest else granted)(scope, receive, send)

    async with served(server, wrap=wrap) as (client, rpc):
        redirect = "http://127.0.0.1:9/cb"

        async def sign_in(client_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
            reg = (await client.post("/oauth/register", json={"client_name": client_name, "redirect_uris": [redirect]})).json()
            verifier = b64url(secrets.token_bytes(32))
            challenge = b64url(hashlib.sha256(verifier.encode()).digest())
            q = {
                "response_type": "code",
                "client_id": reg["client_id"],
                "redirect_uri": redirect,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
            auth = await client.get("/oauth/authorize?" + urlencode(q))
            assert auth.status_code == 302, auth.text
            code = parse_qs(urlsplit(auth.headers["location"]).query)["code"][0]
            r = await client.post(
                "/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "code_verifier": verifier,
                    "redirect_uri": redirect,
                    "client_id": reg["client_id"],
                    "client_secret": reg["client_secret"],
                },
            )
            assert r.status_code == 200, r.text
            return reg, r.json()

        async def refresh(reg: dict[str, Any], rt: str) -> httpx.Response:
            return await client.post(
                "/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": rt,
                    "client_id": reg["client_id"],
                    "client_secret": reg["client_secret"],
                },
            )

        reg, out = await sign_in("Claude Code")
        g = guest.verify(out["access_token"])
        assert g is not None and g.agent == "claude-code"

        # Without the token, /mcp asks for one; with it, the guest's tool call subscribes in the guest's own workspace.
        assert (await client.post("/mcp", json={})).status_code == 401
        client.headers["Authorization"] = "Bearer " + out["access_token"]
        await initialize(rpc, "claude-code")
        res = await rpc("tools/call", {"name": "watch", "arguments": {"repo": "acme"}})
        assert "Watching acme for claude-code." in text_of(res)
        assert await wake.workspace(g.user, "x") == g.workspace, "topics used another workspace"
        assert (await wake.topic_status("guests/acme")).subscriptions == 1

        # A refresh works, and rotates; after forget_user it's refused, and leaves no workspace behind.
        again = await refresh(reg, out["refresh_token"])
        assert again.status_code == 200, again.text
        assert again.json()["refresh_token"] != out["refresh_token"]
        assert await wake.forget_user(g.user) is True
        gone = await refresh(reg, again.json()["refresh_token"])
        assert gone.status_code == 400
        assert gone.json()["error"] == "invalid_grant"
        assert await wake.forget_user(g.user) is False, "the refused refresh left a workspace"

        # With an agent scope in the manifest, grants: a grant per guest, checked at each refresh.
        scope = [{"pattern": "app.ci-bot.ci.*", "description": "CI runs it watches"}]
        # Wakeshift refuses a manifest that asks for Wakeshift accounts or redirect_uris: apps sign their own users in.
        for refused in (
            {"accounts": "wakeshift"},
            {"agent": {"wake": scope, "post": scope, "ttl_days": 30, "redirect_uris": ["http://127.0.0.1:8000/cb"]}},
        ):
            with pytest.raises(WakeshiftError) as e:
                await wake.publish_manifest(refused)
            assert is_code(e.value, "bad_manifest")
        m = await wake.publish_manifest({"agent": {"wake": scope, "post": scope, "ttl_days": 30}})
        assert m["agent"]["ttl_days"] == 30
        current[0] = guest_granted
        del client.headers["Authorization"]
        reg2, first = await sign_in("codex-mcp-client")
        g2 = guest_granted.verify(first["access_token"])
        assert g2 is not None and g2.agent == "codex"
        await wake.check_agent(g2.workspace, g2.agent)  # the grant is live
        assert (await refresh(reg2, first["refresh_token"])).status_code == 200


async def test_a_current_key_asks_wakeshift_which_app_it_is() -> None:
    """The harness's key in the current format (wst_ and 50 characters),
    which names no app: calls work before Wakeshift says which app it is, and
    after."""
    core, _ = harness()
    with open(HARNESS) as f:
        new_key = json.load(f)["new_key"]
    wake = Wakeshift(new_key, core=core)
    assert wake.mode is None
    with pytest.raises(RuntimeError, match="call await wake.app"):
        _ = wake.app_id
    before = await wake.workspace("new-key", "New key")
    info = await wake.app()
    assert (info.id, info.name) == ("ci-bot", "CI bot")
    assert wake.app_id == "ci-bot"
    assert await wake.workspace("new-key", "New key") == before
