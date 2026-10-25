"""GuestAuth against a fake core: the OAuth flow, and everything it refuses
(the same cases as the Go SDK's guestMisuse and the Node SDK's guest tests)."""

from __future__ import annotations

import base64
import json
import re
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, quote, urlencode, urlsplit

import httpx
import pytest
from mcp.server.mcpserver import Context, MCPServer
from starlette.responses import PlainTextResponse

from wakeshift import GuestAuth, Wakeshift, guest_of
from wakeshift.limits import RateLimiter, client_addr, limit_key
from wakeshift.sealed import seal, unseal

from .helpers import served

SEED = (
    base64.urlsafe_b64encode(bytes.fromhex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"))
    .rstrip(b"=")
    .decode()
)
KEY = f"wst_test_quiz_{SEED}"
REDIRECT = "http://127.0.0.1:9/cb"
# RFC 7636's example pair.
VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
CHALLENGE = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
BASE = "http://127.0.0.1:8000"


@dataclass
class Core:
    """Answers the headless workspace calls GuestAuth makes."""

    workspaces: dict[str, str] = field(default_factory=dict)  # user → workspace
    agents: set[str] = field(default_factory=set)  # workspace/agent with a live grant
    fail: str = ""
    limit: float = float("inf")

    def handler(self, request: httpx.Request) -> httpx.Response:
        assert re.match(r"^Bearer [\w-]+\.[\w-]+\.[\w-]+$", request.headers.get("authorization", "")), "the app's assertion"
        path = request.url.path.removeprefix("/api/apps/v1")
        body = json.loads(request.content) if request.content else {}
        if self.fail:
            return httpx.Response(500, json={"error": {"code": self.fail, "message": "boom"}})
        if path == "/workspaces":
            w = self.workspaces.get(body["external_id"])
            if not w:
                if len(self.workspaces) >= self.limit:
                    return httpx.Response(403, json={"error": {"code": "workspace_limit", "message": "full"}})
                w = str(uuid.uuid4())
                self.workspaces[body["external_id"]] = w
            return httpx.Response(200, json={"workspace": w, "created": False})
        if m := re.match(r"^/workspaces/([^/]+)/agents$", path):
            self.agents.add(m.group(1) + "/" + body["agent"])
            return httpx.Response(200, json={"agent": body["agent"], "grant": "g1"})
        if m := re.match(r"^/workspaces/([^/]+)/agents/([^/]+)/token$", path):
            if m.group(1) not in self.workspaces.values():
                return httpx.Response(404, json={"error": {"code": "unknown_workspace", "message": "no"}})
            if f"{m.group(1)}/{m.group(2)}" not in self.agents:
                return httpx.Response(403, json={"error": {"code": "no_grant", "message": "no"}})
            return httpx.Response(200, json={"access_token": "x", "expires_in": 900})
        if path == "/users/forget":
            had = self.workspaces.pop(body["user"], None) is not None
            return httpx.Response(200, json={"forgotten": had})
        return httpx.Response(404, json={"error": {"code": "not_found", "message": path}})


@dataclass
class App:
    core: Core
    guest: GuestAuth
    http: httpx.AsyncClient
    errors: list[str]


async def _not_found(scope: Any, receive: Any, send: Any) -> None:
    await PlainTextResponse("not found", status_code=404)(scope, receive, send)


@asynccontextmanager
async def start(**opts: Any) -> AsyncIterator[App]:
    core = Core()
    wake = Wakeshift(KEY, core="http://core.test", http=httpx.AsyncClient(transport=httpx.MockTransport(core.handler)))
    errors: list[str] = []
    guest = GuestAuth(wake, BASE + "/mcp", on_error=lambda m, err: errors.append(f"{m} {err}"), **opts)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=guest.wrap(_not_found)), base_url=BASE) as http:
        yield App(core, guest, http, errors)


async def register(a: App, name: str = "Codex", headers: dict[str, str] | None = None) -> tuple[str, str, int]:
    r = await a.http.post(
        "/oauth/register",
        content=json.dumps({"client_name": name, "redirect_uris": [REDIRECT]}),
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    out = r.json()
    return out.get("client_id", ""), out.get("client_secret", ""), r.status_code


async def code(
    a: App, client_id: str, redirect: str = REDIRECT, challenge: str = CHALLENGE, state: str = ""
) -> tuple[str, int, str]:
    q = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    if state:
        q["state"] = state
    r = await a.http.get("/oauth/authorize?" + urlencode(q))
    loc = r.headers.get("location", "")
    got = parse_qs(urlsplit(loc).query).get("code", [""])[0] if loc else ""
    return got, r.status_code, loc


async def token(
    a: App, form: dict[str, str], basic: tuple[str, str] | None = None, headers: dict[str, str] | None = None
) -> tuple[int, Any, httpx.Headers]:
    h = {"Content-Type": "application/x-www-form-urlencoded", **(headers or {})}
    if basic:
        h["Authorization"] = (
            "Basic " + base64.b64encode((quote(basic[0], safe="") + ":" + quote(basic[1], safe="")).encode()).decode()
        )
    r = await a.http.post("/oauth/token", content=urlencode(form), headers=h)
    return r.status_code, r.json(), r.headers


def exchange(client_id: str, c: str, secret: str | None = None) -> dict[str, str]:
    f = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "redirect_uri": REDIRECT,
        "code": c,
        "code_verifier": VERIFIER,
    }
    if secret is not None:
        f["client_secret"] = secret
    return f


async def test_metadata_points_mcp_clients_at_the_apps_own_sign_in() -> None:
    async with start() as a:
        r = await a.http.get("/.well-known/oauth-protected-resource")
        assert r.json() == {"resource": BASE + "/mcp", "authorization_servers": [BASE], "bearer_methods_supported": ["header"]}
        m = (await a.http.get("/.well-known/oauth-authorization-server")).json()
        assert m["token_endpoint"] == BASE + "/oauth/token"
        assert m["token_endpoint_auth_methods_supported"] == ["client_secret_post", "client_secret_basic"]
        assert (await a.http.post("/.well-known/oauth-authorization-server")).status_code == 405
        assert (await a.http.get("/oauth/token")).headers.get("allow") == "POST"


async def test_the_whole_flow_a_guest_per_registered_client_named_after_its_harness() -> None:
    async with start() as a:
        cid, secret, status = await register(a, "Claude Code")
        assert status == 201
        c, st, loc = await code(a, cid, state="st8")
        assert st == 302
        assert parse_qs(urlsplit(loc).query)["state"] == ["st8"]
        assert a.core.workspaces == {}, "authorize made nothing"
        status, out, _ = await token(a, exchange(cid, c, secret))
        assert status == 200, out
        assert out["token_type"] == "Bearer" and out["expires_in"] == 3600
        g = a.guest.verify(out["access_token"])
        assert g is not None
        assert g.agent == "claude-code" and g.client == "Claude Code"
        assert re.match(r"^guest:[\w-]{24}$", g.user)
        assert a.core.workspaces[g.user] == g.workspace
        assert a.core.agents == set(), "no grant without grants"
        assert a.guest.verify(out["access_token"][:-2] + "xx") is None, "a tampered token"
        assert a.guest.verify(out["refresh_token"]) is None, "a refresh token isn't an access token"
        assert a.guest.verify(c) is None, "a code isn't an access token"
        # Another install of the same harness is another guest.
        oid, osec, _ = await register(a, "Claude Code")
        oc, _, _ = await code(a, oid)
        _, o, _ = await token(a, exchange(oid, oc, osec))
        assert a.guest.verify(o["access_token"]).workspace != g.workspace


async def test_knowing_a_client_id_isnt_enough_codes_and_refresh_tokens_need_the_secret() -> None:
    async with start() as a:
        cid, secret, _ = await register(a)
        assert (await code(a, cid, "http://evil.example/cb"))[1] == 400, "an unregistered redirect"
        assert (await code(a, "not-a-client"))[1] == 400, "an unknown client"
        short_code, short_status, short_loc = await code(a, cid, REDIRECT, "short")
        assert short_code == "", "a malformed PKCE challenge got a code"
        # Answered here, never by redirecting (an open redirect otherwise).
        assert short_status == 400 and not short_loc

        c = (await code(a, cid))[0]
        status, out, headers = await token(a, exchange(cid, c))
        assert status == 401 and out["error"] == "invalid_client"
        assert headers.get("www-authenticate") == 'Basic realm="oauth"'
        assert (await token(a, exchange(cid, c, "not-the-secret")))[0] == 401, "a wrong secret"
        oid, osec, _ = await register(a)
        assert (await token(a, exchange(cid, c, osec)))[0] == 401, "another client's secret"
        assert (await token(a, exchange(cid, c, secret[:-1])))[0] == 401, "a secret cut short"
        assert a.core.workspaces == {}, "refused exchanges made a workspace"

        bad = exchange(cid, c, secret)
        bad["code_verifier"] = "the-wrong-verifier-the-wrong-verifier-123456"
        status, out, _ = await token(a, bad)
        assert status == 400 and out["error"] == "invalid_grant"
        other_code = (await code(a, oid))[0]
        assert (await token(a, exchange(cid, other_code, secret)))[0] == 400, "another client's code"
        wrong = exchange(cid, c, secret)
        wrong["redirect_uri"] = "http://127.0.0.1:9/other"
        assert (await token(a, wrong))[0] == 400, "a different redirect_uri"
        assert (await token(a, {**exchange(cid, c, secret), "code": c + "x"}))[0] == 400, "a tampered code"

        # Basic and the form secret together, or Basic with a different form client_id: refused.
        assert (await token(a, exchange(cid, c, secret), (cid, secret)))[0] == 401
        assert (await token(a, exchange(oid, c), (cid, secret)))[0] == 401

        status, out, headers = await token(a, exchange(cid, c, secret))
        assert status == 200, "the real client, its secret in the form"
        assert headers.get("cache-control") == "no-store"
        assert len(a.core.workspaces) == 1
        # Basic works too (Codex's OAuth client sends it).
        assert (await token(a, exchange(oid, other_code), (oid, osec)))[0] == 200

        # Refreshes rotate, and need the client and its secret.
        first = out["refresh_token"]
        status, again, _ = await token(
            a, {"grant_type": "refresh_token", "refresh_token": first, "client_id": cid, "client_secret": secret}
        )
        assert status == 200
        assert a.guest.verify(again["access_token"]).agent == "codex"
        assert again["refresh_token"] != first, "a refresh didn't rotate"
        assert (await token(a, {"grant_type": "refresh_token", "refresh_token": again["refresh_token"]}, (cid, secret)))[
            0
        ] == 200, "refresh with Basic"
        assert (await token(a, {"grant_type": "refresh_token", "refresh_token": first, "client_id": cid}))[0] == 401, (
            "without the secret"
        )
        assert (await token(a, {"grant_type": "refresh_token", "refresh_token": first, "client_id": oid, "client_secret": osec}))[
            0
        ] == 400, "another client's"
        assert (
            await token(
                a,
                {"grant_type": "refresh_token", "refresh_token": out["access_token"], "client_id": cid, "client_secret": secret},
            )
        )[0] == 400, "an access token as a refresh token"
        assert (await token(a, {"grant_type": "password", "client_id": cid, "client_secret": secret}))[1][
            "error"
        ] == "unsupported_grant_type"


async def test_refreshes_stop_once_the_app_revokes_the_guest_or_forgets_its_user() -> None:
    cutoff = {"t": 0.0}
    async with start(revoked=lambda g, issued: g.agent == "codex" and issued.timestamp() < cutoff["t"]) as a:
        cid, secret, _ = await register(a)
        _, out, _ = await token(a, exchange(cid, (await code(a, cid))[0], secret))

        async def refresh(rt: str) -> tuple[int, Any, httpx.Headers]:
            return await token(a, {"grant_type": "refresh_token", "refresh_token": rt, "client_id": cid, "client_secret": secret})

        cutoff["t"] = time.time() + 3600
        status, x, _ = await refresh(out["refresh_token"])
        assert status == 400 and x["error"] == "invalid_grant"
        cutoff["t"] = 0
        assert (await refresh(out["refresh_token"]))[0] == 200

        # forget_user(guest.user): the next refresh is refused, and the workspace asking made is forgotten again.
        g = a.guest.verify(out["access_token"])
        del a.core.workspaces[g.user]
        status, x, _ = await refresh(out["refresh_token"])
        assert status == 400 and x["error"] == "invalid_grant"
        assert g.user not in a.core.workspaces, "the refresh left a workspace behind"

        # Wakeshift down: try again, not signed out; logged without the token.
        cid2, sec2, _ = await register(a)
        _, out2, _ = await token(a, exchange(cid2, (await code(a, cid2))[0], sec2))
        a.core.fail = "internal"
        status, x, _ = await token(
            a, {"grant_type": "refresh_token", "refresh_token": out2["refresh_token"], "client_id": cid2, "client_secret": sec2}
        )
        assert status == 503 and x["error"] == "temporarily_unavailable"
        assert a.errors and all(out2["refresh_token"] not in e and sec2 not in e for e in a.errors)


async def test_grants_a_grant_per_guest_and_revoking_it_signs_the_guest_out() -> None:
    async with start(grants=True) as a:
        cid, secret, _ = await register(a, "copilot-cli")
        _, out, _ = await token(a, exchange(cid, (await code(a, cid))[0], secret))
        g = a.guest.verify(out["access_token"])
        assert g.agent == "copilot"
        assert g.workspace + "/copilot" in a.core.agents
        form = {"grant_type": "refresh_token", "refresh_token": out["refresh_token"], "client_id": cid, "client_secret": secret}
        assert (await token(a, form))[0] == 200
        a.core.agents.clear()
        status, x, _ = await token(a, form)
        assert status == 400 and x["error"] == "invalid_grant"


async def test_a_full_app_takes_no_more_guests_a_failing_core_is_a_503() -> None:
    async with start() as a:
        a.core.limit = 0
        cid, secret, _ = await register(a)
        status, out, _ = await token(a, exchange(cid, (await code(a, cid))[0], secret))
        assert status == 403 and out["error"] == "access_denied"
        a.core.limit = float("inf")
        a.core.fail = "internal"
        assert (await token(a, exchange(cid, (await code(a, cid))[0], secret)))[0] == 503


async def test_codes_expire_after_a_minute_access_tokens_after_access_ttl() -> None:
    now = {"t": time.time()}
    async with start(now=lambda: now["t"], access_ttl=60) as a:
        cid, secret, _ = await register(a)
        c = (await code(a, cid))[0]
        now["t"] += 61
        assert (await token(a, exchange(cid, c, secret)))[0] == 400, "an expired code"
        _, out, _ = await token(a, exchange(cid, (await code(a, cid))[0], secret))
        assert out["expires_in"] == 60
        assert a.guest.verify(out["access_token"])
        now["t"] += 61
        assert a.guest.verify(out["access_token"]) is None


async def test_registration_refuses_bad_metadata() -> None:
    async with start() as a:
        for body in [
            "not json",
            "{}",
            json.dumps({"redirect_uris": []}),
            json.dumps({"redirect_uris": ["http://h/" + x for x in "abcdef"]}),
            json.dumps({"redirect_uris": [42]}),
            json.dumps({"redirect_uris": ["relative/path"]}),
            json.dumps({"redirect_uris": ["http://h/" + "x" * 500]}),
            json.dumps({"redirect_uris": ["javascript:alert(1)"]}),
            json.dumps({"redirect_uris": ["data:text/html,x"]}),
            json.dumps({"redirect_uris": ["http://evil.example/cb"]}),
            json.dumps({"redirect_uris": ["https://ok.example/cb#frag"]}),
            "x" * (17 << 10),
        ]:
            r = await a.http.post("/oauth/register", content=body, headers={"Content-Type": "application/json"})
            assert r.status_code == 400, body[:40]
        for good in ["https://app.example/cb", "http://localhost:3000/cb", "http://127.0.0.1:9/cb", "http://[::1]:9/cb"]:
            r = await a.http.post("/oauth/register", content=json.dumps({"redirect_uris": [good]}))
            assert r.status_code == 201, good
        ok = (
            await a.http.post("/oauth/register", content=json.dumps({"client_name": "   ", "redirect_uris": ["cursor://cb"]}))
        ).json()
        assert ok["client_name"] == "MCP client"
        assert ok["token_endpoint_auth_method"] == "client_secret_post"


async def test_registering_and_token_requests_are_limited_per_address_from_the_proxys_entry() -> None:
    async with start(register_limit=2, token_limit=3, proxy_hops=1) as a:

        def xff(v: str) -> dict[str, str]:
            return {"X-Forwarded-For": v}

        assert (await register(a, "x", xff("203.0.113.7")))[2] == 201
        assert (await register(a, "x", xff("203.0.113.7")))[2] == 201
        assert (await register(a, "x", xff("203.0.113.7")))[2] == 429
        assert (await register(a, "x", xff("1.2.3.4, 203.0.113.7")))[2] == 429, "a spoofed X-Forwarded-For"
        assert (await register(a, "x", xff("203.0.113.8")))[2] == 201, "another address"
        for _ in range(3):
            await token(a, {}, headers=xff("203.0.113.9"))
        status, out, _ = await token(a, {}, headers=xff("203.0.113.9"))
        assert status == 429 and out["error"] == "slow_down"


def test_the_limiter_counts_an_ipv6_64_as_one_address_and_tracks_a_bounded_number() -> None:
    now = time.time()
    lim = RateLimiter(2, 60)
    assert lim.allow("2001:db8:1:2::1", now) and lim.allow("2001:db8:1:2::ffff", now)
    assert not lim.allow("2001:db8:1:2:abcd::9", now), "one /64 isn't one address"
    assert lim.allow("2001:db8:1:3::1", now) and lim.allow("192.0.2.1", now)
    assert limit_key("2001:db8:0:0:1::1") == limit_key("2001:db8::1:0:0:2")
    assert limit_key("::ffff:192.0.2.1") == "::ffff:192.0.2.1"
    assert limit_key("192.0.2.1") == "192.0.2.1"
    lim = RateLimiter(5, 60, 3)
    for i in range(3):
        assert lim.allow(f"192.0.2.{i}", now)
    assert not lim.allow("192.0.2.200", now), "a new address past the cap"
    assert lim.allow("192.0.2.1", now), "a tracked address past the cap"
    assert lim.allow("192.0.2.200", now + 120), "after the old ones expired"
    assert RateLimiter(-1, 1).allow("x", now), "a negative limit is off"


@pytest.mark.parametrize(
    ("xff", "want"),
    [
        ("spoofed, 203.0.113.7, 10.0.0.1", "203.0.113.7"),
        ("203.0.113.7, 10.0.0.1", "203.0.113.7"),
        ("10.0.0.1", "198.51.100.2"),
        ("   ", "198.51.100.2"),
        ("not-an-ip, 10.0.0.1", "198.51.100.2"),
    ],
)
def test_client_addr_reads_the_outer_proxys_entry_else_the_connection(xff: str, want: str) -> None:
    assert client_addr([(b"x-forwarded-for", xff.encode())], "198.51.100.2", 2) == want


def test_client_addr_ignores_the_header_without_trusted_proxies() -> None:
    assert client_addr([(b"x-forwarded-for", b"203.0.113.7")], "198.51.100.2", 0) == "198.51.100.2"
    assert client_addr([], "::ffff:192.0.2.9", 0) == "192.0.2.9"
    # Only the first X-Forwarded-For header counts (as Go's Header.Get reads it).
    assert (
        client_addr([(b"x-forwarded-for", b"203.0.113.7"), (b"x-forwarded-for", b"1.2.3.4")], "198.51.100.2", 1) == "203.0.113.7"
    )


async def test_authenticate_tools_get_the_guest_a_bad_token_gets_the_401_mcp_clients_start_from() -> None:
    core = Core()
    wake = Wakeshift(KEY, core="http://core.test", http=httpx.AsyncClient(transport=httpx.MockTransport(core.handler)))
    guest = GuestAuth(wake, BASE + "/mcp")
    seen: dict[str, Any] = {}
    server = MCPServer("quiz")

    @server.tool()
    async def whoami(ctx: Context) -> str:
        g = guest_of(ctx)
        seen["guest"] = g
        return g.user if g else "nobody"

    async with served(server, wrap=guest.wrap) as (http, rpc):
        anon = await http.post("/mcp", content="{}")
        assert anon.status_code == 401
        assert anon.headers["www-authenticate"] == f'Bearer resource_metadata="{BASE}/.well-known/oauth-protected-resource"'
        bad = await http.post("/mcp", content="{}", headers={"Authorization": "Bearer nope"})
        assert bad.status_code == 401
        assert 'error="invalid_token"' in bad.headers["www-authenticate"]

        a = App(core, guest, http, [])
        cid, secret, _ = await register(a, "Claude Code")
        _, out, _ = await token(a, exchange(cid, (await code(a, cid))[0], secret))
        auth = {"Authorization": "Bearer " + out["access_token"]}
        res = await rpc("tools/call", {"name": "whoami", "arguments": {}}, headers=auth)
        g = guest.verify(out["access_token"])
        assert res["result"]["content"][0]["text"] == g.user
        assert seen["guest"].agent == "claude-code"


async def test_verify_token_lets_mcpserver_own_bearer_auth_carry_the_guest() -> None:
    async with start() as a:
        cid, secret, _ = await register(a, "Claude Code")
        _, out, _ = await token(a, exchange(cid, (await code(a, cid))[0], secret))
        tok = await a.guest.verify_token(out["access_token"])
        assert tok is not None and tok.client_id.startswith("guest:")
        assert await a.guest.verify_token("nope") is None

        class _User:
            access_token = tok

        class _Req:
            scope = {"type": "http", "user": _User()}

        g = guest_of(_Req())
        assert g is not None and g.agent == "claude-code"


def test_sealed_values_open_only_under_their_key_unchanged_and_canonically_encoded() -> None:
    k = bytes([1]) * 32
    s = seal(k, {"a": 1})
    assert unseal(k, s) == {"a": 1}
    assert unseal(bytes([2]) * 32, s) is None, "another key"
    assert unseal(k, ("f" if s[0] == "e" else "e") + s[1:]) is None, "a changed payload"
    p, sig = s.split(".")
    digits = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    alt = sig[:-1] + digits[digits.index(sig[-1]) ^ 1]
    pad = "=" * (-len(sig) % 4)
    assert base64.urlsafe_b64decode(alt + pad) == base64.urlsafe_b64decode(sig + pad), "the test's premise"
    assert unseal(k, p + "." + alt) is None, "a non-canonical signature"
    assert unseal(k, seal(k, [1, 2])) is None, "not an object"
    assert unseal(k, seal(k, "x")) is None, "not an object"
    for bad in ["", ".", "x", "a.b.c", s + "=", 42, None]:
        assert unseal(k, bad) is None, repr(bad)
