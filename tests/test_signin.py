"""Sign in with Wakeshift: the browser flow (WakeshiftSignIn) and the token
calls (wake.sign_in, wake.refresh), against a fake core."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import httpx
import pytest
from starlette.requests import Request
from starlette.responses import Response

from wakeshift import SignInError, Wakeshift, WakeshiftError, WakeshiftSignIn
from wakeshift.key import b64url

SEED = b64url(bytes.fromhex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"))
KEY = f"wst_test_holdem_{SEED}"
CORE = "http://core.test"
CALLBACK = "https://holdem.example/auth/wakeshift/callback"

TOKENS = {
    "access_token": "at",
    "refresh_token": "rt",
    "expires_in": 900,
    "wakeshift_user": "u-123",
    "wakeshift_workspace": "w-1",
    "wakeshift_agent": "claude-code",
    "wakeshift_grant": "g-1",
}


class Seen:
    form: dict[str, str] | None = None


def setup(
    answer: Callable[[dict[str, str]], tuple[int, Any]] = lambda f: (200, TOKENS), now: Callable[[], float] | None = None
) -> tuple[Wakeshift, Seen, WakeshiftSignIn]:
    seen = Seen()

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == CORE + "/oauth/token"
        form = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
        seen.form = form
        status, body = answer(form)
        return httpx.Response(status, json=body)

    wake = Wakeshift(KEY, core=CORE, http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    return wake, seen, WakeshiftSignIn(wake, CALLBACK, now=now)


async def start_flow(login: WakeshiftSignIn, link: str = "") -> tuple[Response, dict[str, str], str, str]:
    """Runs start: the response, the redirect's query, the cookie and the state Wakeshift would echo."""
    res = await login.start(link=link, agent_hint="Claude Code")
    loc = res.headers["location"]
    q = {k: v[0] for k, v in parse_qs(urlsplit(loc).query).items()}
    cookie = res.headers["set-cookie"].split(";")[0]
    return res, q, cookie, q["state"]


def request(query: str, cookie: str = "") -> Request:
    headers = [(b"cookie", cookie.encode())] if cookie else []
    return Request({"type": "http", "method": "GET", "path": "/cb", "query_string": query.encode(), "headers": headers})


async def test_start_sends_the_person_to_wakeshift_with_pkce_and_a_signed_cookie() -> None:
    _, _, login = setup()
    res, q, _, _ = await start_flow(login, "guest:abc")
    assert res.status_code == 302
    assert res.headers["location"].startswith(CORE + "/oauth/authorize?")
    assert q["client_id"] == "app:holdem"
    assert q["redirect_uri"] == CALLBACK
    assert q["code_challenge_method"] == "S256"
    assert re.fullmatch(r"[\w-]{43}", q["code_challenge"])
    assert q["agent_hint"] == "Claude Code"
    assert re.fullmatch(
        r"wakeshift_signin=[\w-]+\.[\w-]+; Path=/auth/wakeshift/callback; Max-Age=600; HttpOnly; SameSite=Lax; Secure",
        res.headers["set-cookie"],
    )
    assert res.headers["cache-control"] == "no-store"


async def test_finish_exchanges_the_code_with_the_verifier_and_returns_who_signed_in_and_the_link() -> None:
    _, seen, login = setup()
    _, q, cookie, state = await start_flow(login, "guest:abc")
    res = Response()
    signed_in, link = await login.finish(request(f"code=c1&state={state}&iss={quote(CORE, safe='')}", f"other=1; {cookie}"), res)
    assert link == "guest:abc"
    assert (signed_in.user, signed_in.agent, signed_in.refresh_token) == ("u-123", "claude-code", "rt")
    assert "access_token=" not in repr(signed_in) and "refresh_token=" not in repr(signed_in), "tokens stay out of reprs"
    f = seen.form
    assert f is not None
    assert f["grant_type"] == "authorization_code"
    assert f["code"] == "c1"
    assert f["redirect_uri"] == CALLBACK
    assert f["client_id"] == "app:holdem"
    assert f["client_assertion_type"] == "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
    assert re.fullmatch(r"[\w-]+\.[\w-]+\.[\w-]+", f["client_assertion"])
    assert b64url(hashlib.sha256(f["code_verifier"].encode()).digest()) == q["code_challenge"]
    assert "Max-Age=0" in res.headers["set-cookie"], "the cookie is cleared"


async def test_finish_refuses_what_didnt_start_here_came_from_elsewhere_or_was_refused() -> None:
    now = [1_800_000_000.0]
    _, seen, login = setup(now=lambda: now[0])
    _, _, cookie, state = await start_flow(login)
    iss = quote(CORE, safe="")

    async def code(q: str, c: str | None = cookie) -> str:
        try:
            await login.finish(request(q, c or ""), Response())
        except SignInError as e:
            return e.code
        return "ok"

    assert await code(f"code=c&state={state}&iss={iss}", "") == "expired", "no cookie"
    name, _, value = cookie.partition("=")
    tampered = name + "=" + ("f" if value[0] == "e" else "e") + value[1:]
    assert await code(f"code=c&state={state}&iss={iss}", tampered) == "expired", "a tampered cookie"
    assert await code(f"code=c&state=other&iss={iss}") == "state"
    assert await code(f"code=c&iss={iss}") == "state"
    assert await code(f"code=c&state={state}") == "issuer"
    assert await code(f"code=c&state={state}&iss={quote('https://evil.example', safe='')}") == "issuer"
    with pytest.raises(SignInError) as denied:
        await login.finish(request(f"error=access_denied&state={state}&iss={iss}", cookie), Response())
    assert (denied.value.code, denied.value.detail) == ("denied", "access_denied")
    assert await code(f"state={state}&iss={iss}") == "failed", "no code"
    assert seen.form is None, "nothing reached Wakeshift"
    # Another app's cookie (another key) doesn't open.
    other_wake = Wakeshift(f"wst_test_other_{b64url(bytes([7]) * 32)}", core=CORE)
    _, _, other_cookie, other_state = await start_flow(WakeshiftSignIn(other_wake, CALLBACK))
    assert await code(f"code=c&state={other_state}&iss={iss}", other_cookie) == "expired"
    now[0] += 601
    assert await code(f"code=c&state={state}&iss={iss}") == "expired", "after ten minutes"


async def test_a_code_wakeshift_refuses_is_a_failed_sign_in() -> None:
    _, _, login = setup(lambda f: (400, {"error": "invalid_grant", "error_description": "the code is invalid"}))
    _, _, cookie, state = await start_flow(login)
    with pytest.raises(SignInError) as e:
        await login.finish(request(f"code=c&state={state}&iss={quote(CORE, safe='')}", cookie), Response())
    assert e.value.code == "failed"
    assert isinstance(e.value.__cause__, WakeshiftError) and e.value.__cause__.code == "invalid_grant"


async def test_refresh_renews_a_sign_in_a_revoked_one_is_invalid_grant() -> None:
    revoked = [False]
    extra = {"wakeshift_workspace_grant": "wg", "wakeshift_workspace_name": "Ada's", "wakeshift_user_email": "ada@example.com"}
    wake, seen, _ = setup(
        lambda f: (400, {"error": "invalid_grant", "error_description": "revoked"}) if revoked[0] else (200, {**TOKENS, **extra})
    )
    s = await wake.refresh("rt-old")
    assert seen.form is not None
    assert (seen.form["grant_type"], seen.form["refresh_token"]) == ("refresh_token", "rt-old")
    assert s.connection is not None
    assert (
        s.connection.workspace,
        s.connection.grant,
        s.connection.user,
        s.connection.workspace_name,
        s.connection.user_email,
    ) == (
        "w-1",
        "wg",
        "u-123",
        "Ada's",
        "ada@example.com",
    )
    revoked[0] = True
    with pytest.raises(WakeshiftError) as e:
        await wake.refresh("rt")
    assert (e.value.code, e.value.status) == ("invalid_grant", 400)


async def test_a_200_without_the_tokens_is_a_bad_response() -> None:
    wake, _, _ = setup(lambda f: (200, {"access_token": "at"}))
    with pytest.raises(WakeshiftError) as e:
        await wake.sign_in(code="c", verifier="v", redirect_uri=CALLBACK)
    assert e.value.code == "bad_response"


async def test_a_plain_http_redirect_gets_no_secure_cookie_a_bad_cookie_name_is_refused() -> None:
    wake, _, _ = setup()
    res = await WakeshiftSignIn(wake, "http://localhost:8080/cb").start()
    assert "Secure" not in res.headers["set-cookie"]
    with pytest.raises(ValueError, match="cookie name"):
        WakeshiftSignIn(wake, CALLBACK, cookie="a;b")
    with pytest.raises(ValueError, match="link"):
        await WakeshiftSignIn(wake, CALLBACK).start(link="x" * 1001)
    with pytest.raises(ValueError, match="absolute URL"):
        WakeshiftSignIn(wake, "/cb")
