"""The app key, assertions and the client's calls, against a fake core: the
Node SDK's unit tests, in Python."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from cryptography.exceptions import InvalidSignature
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import (
    CONVERSATION_ARG,
    HARNESS_ARG,
    SESSION_ARG,
    Wakeshift,
    WakeshiftError,
    is_code,
    parse_key,
    sign_assertion,
)
from wakeshift.client import WAKE_ARGS_SCHEMA, _call, _call_of
from wakeshift.key import b64url, b64url_decode, public_key_of

# RFC 8032, test 1: this seed's public key.
SEED_HEX = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
PUB_HEX = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
SEED = b64url(bytes.fromhex(SEED_HEX))
KEY = f"wst_test_ci-bot_{SEED}"


def test_parse_key_reads_the_app_id_and_key() -> None:
    k = parse_key(KEY)
    assert k.app_id == "ci-bot" and not hasattr(k, "mode")
    assert b64url_decode(public_key_of(k)).hex() == PUB_HEX
    # A seed with "_" and "-" in it: split on the first two underscores only.
    odd = parse_key("wst_live_my-app_3q2-7__7AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    assert odd.app_id == "my-app"
    assert SEED not in repr(k), "the seed stays out of reprs"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        SEED,
        f"wsk_test_ci-bot_{SEED}",
        f"wst_prod_ci-bot_{SEED}",
        f"wst_test__{SEED}",
        f"wst_test_Ci_{SEED}",
        f"wst_test_ci-bot_{SEED[:42]}",
        f"wst_test_ci-bot_{SEED}A",
        f"wst_test_ci-bot_{SEED[:42]}=",
        "wst_test_ci-bot",
    ],
)
def test_parse_key_refuses_anything_else(bad: str) -> None:
    with pytest.raises(ValueError, match="wakeshift:"):
        parse_key(bad)


def test_sign_assertion_signs_a_short_lived_eddsa_assertion_for_the_core() -> None:
    k = parse_key(KEY)
    now = datetime(2026, 10, 18, 12, tzinfo=UTC).timestamp()
    tok = sign_assertion(k, "https://app.wakeshift.ai", now)
    h, c, s = tok.split(".")
    assert json.loads(b64url_decode(h)) == {"alg": "EdDSA", "typ": "app+jwt", "kid": k.kid}
    claims = json.loads(b64url_decode(c))
    assert claims["iss"] == claims["sub"] == "ci-bot"
    assert claims["aud"] == "https://app.wakeshift.ai/api/apps"
    assert claims["iat"] == int(now)
    assert claims["exp"] == int(now) + 120
    assert re.fullmatch(r"[A-Za-z0-9_-]{22}", claims["jti"])
    assert "=" not in tok, "base64url without padding"
    k.private_key.public_key().verify(b64url_decode(s), f"{h}.{c}".encode())
    with pytest.raises(InvalidSignature):
        k.private_key.public_key().verify(b64url_decode(s), f"{h}.{c}x".encode())
    other = json.loads(b64url_decode(sign_assertion(k, "x", now).split(".")[1]))
    assert other["jti"] != claims["jti"]


class Core:
    """Answers the app API with reply(path, body), recording what was sent."""

    def __init__(self, reply: Any) -> None:
        self.reply = reply
        self.sent: list[dict[str, Any]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.sent.append(
            {"method": request.method, "url": str(request.url), "auth": request.headers.get("authorization", ""), "body": body}
        )
        status, out = self.reply(request.url.raw_path.decode().split("?")[0], body)
        return httpx.Response(status, json=out)

    def wake(self, **kw: Any) -> Wakeshift:
        return Wakeshift(KEY, core="http://core.test", http=httpx.AsyncClient(transport=httpx.MockTransport(self)), **kw)


def test_the_core_url_given_else_wakeshift_core_else_production_except_for_a_test_key(monkeypatch: pytest.MonkeyPatch) -> None:
    assert Wakeshift(KEY, core="http://127.0.0.1:9/").core == "http://127.0.0.1:9"
    monkeypatch.setenv("WAKESHIFT_CORE", "https://app.staging.wakeshift.ai")
    assert Wakeshift(KEY).core == "https://app.staging.wakeshift.ai"
    monkeypatch.delenv("WAKESHIFT_CORE")
    with pytest.raises(ValueError, match="no core URL for this test key: set WAKESHIFT_CORE"):
        Wakeshift(KEY)
    monkeypatch.setenv("WAKESHIFT_CORE", "")
    with pytest.raises(ValueError, match="no core URL"):
        Wakeshift(KEY)
    assert Wakeshift(KEY.replace("wst_test_", "wst_live_")).core == "https://app.wakeshift.ai"
    with pytest.raises(ValueError, match="no app key"):
        Wakeshift("")


class Hole(BaseModel):
    table: str


class Note(BaseModel):
    text: str


async def test_emit_merges_the_manifest_then_sends_a_signed_request() -> None:
    core = Core(
        lambda path, body: (
            200,
            body if path.endswith("/manifest/merge") else {"matched": 3, "duplicate": False},
        )
    )
    wake = core.wake()
    news = wake.define_event("table.news", description="News at a table", args=Hole, payload=Note, text=lambda n, a: n.text)
    wake.attach(MCPServer("news"))  # a process that serves tools sends the manifest
    out = await news.emit({"table": "oak"}, {"text": "dealt"}, id="d1")
    assert (out.matched, out.duplicate, out.unmatched, out.ended) == (3, False, [], [])
    assert [(r["method"], r["url"]) for r in core.sent] == [
        ("POST", "http://core.test/api/apps/v1/manifest/merge"),
        ("POST", "http://core.test/api/apps/v1/events/emit"),
    ]
    r = core.sent[1]
    assert re.fullmatch(r"Bearer [\w-]+\.[\w-]+\.[\w-]+", r["auth"])
    assert SEED not in r["auth"], "the key never goes over the wire"
    assert r["body"] == {
        "name": "table.news",
        "arguments": {"table": "oak"},
        "payload": {"text": "dealt"},
        "text": "dealt",
        "id": "d1",
    }
    # Sent once: the next emit goes straight out.
    await news.emit({"table": "oak"}, {"text": "again"})
    assert [r["method"] for r in core.sent] == ["POST", "POST", "POST"]


async def test_errors_carry_the_apis_code() -> None:
    wake = Core(
        lambda p, b: (403, {"error": {"code": "other_client", "message": "that conversation is another client's"}})
    ).wake()
    with pytest.raises(WakeshiftError) as e:
        await wake.forget_user("ada")
    assert (e.value.status, e.value.code, e.value.detail) == (403, "other_client", "that conversation is another client's")
    assert is_code(e.value, "other_client")
    assert not is_code(e.value, "bad_request")
    bare = Core(lambda p, b: (502, "bad gateway")).wake()
    with pytest.raises(WakeshiftError) as e2:
        await bare.forget_user("ada")
    assert (e2.value.status, e2.value.code) == (502, "")


@pytest.mark.parametrize("body", ["", "<html>ok</html>", "null", "42"])
async def test_a_2xx_without_a_json_object_is_a_wakeshift_error(body: str) -> None:
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, text=body, headers={"Content-Type": "text/html"}))
    )
    wake = Wakeshift(KEY, core="http://core.test", http=http)
    with pytest.raises(WakeshiftError) as e:
        await wake.workspace("ada", "Ada")
    assert (e.value.code, e.value.status) == ("bad_response", 200)
    assert "http://core.test answered 200 without a JSON object" in str(e.value)


async def test_a_core_that_doesnt_answer_the_error_names_it() -> None:
    def refused(r: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    wake = Wakeshift(KEY, core="http://127.0.0.1:9", http=httpx.AsyncClient(transport=httpx.MockTransport(refused)))
    with pytest.raises(WakeshiftError) as e:
        await wake.forget_user("ada")
    assert (e.value.code, e.value.status) == ("unreachable", 0)
    assert str(e.value).startswith(
        "wakeshift: can't reach Wakeshift at http://127.0.0.1:9 (connection refused). Check that WAKESHIFT_CORE"
    )
    assert isinstance(e.value.__cause__, httpx.ConnectError)

    def slow(r: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out")

    wake2 = Wakeshift(KEY, core="http://core.test", http=httpx.AsyncClient(transport=httpx.MockTransport(slow)), timeout=0.02)
    with pytest.raises(WakeshiftError, match=r"can't reach Wakeshift at http://core\.test \(no answer within 0\.02s\)"):
        await wake2.workspace("ada", "Ada")


async def test_the_other_calls_paths_bodies_and_results() -> None:
    replies: dict[str, Any] = {
        "/api/apps/v1/events/publish-urls/u%2F1": {"revoked": True},
        "/api/apps/v1/users/forget": {"forgotten": True},
        "/api/apps/v1/workspaces": {"workspace": "ws_1"},
        "/api/apps/v1/workspaces/ws_1/agents": {"grant": "gr_1"},
        "/api/apps/v1/workspaces/ws_1/agents/codex/token": {"ok": True},
    }
    core = Core(lambda path, body: (200, replies[path]) if path in replies else (404, {"error": {"code": "not_found"}}))
    wake = core.wake()

    await wake.revoke_publish_url("u/1")
    assert core.sent[-1]["method"] == "DELETE"
    assert core.sent[-1]["body"] is None
    assert await wake.forget_user("ada") is True
    assert core.sent[-1]["body"] == {"user": "ada"}
    assert await wake.workspace("ada", "Ada") == "ws_1"
    assert core.sent[-1]["body"] == {"external_id": "ada", "name": "Ada"}
    assert await wake.add_agent("ws_1", "codex", "Codex") == "gr_1"
    assert core.sent[-1]["body"] == {"agent": "codex", "display_name": "Codex"}
    await wake.check_agent("ws_1", "codex")
    assert core.sent[-1]["url"] == "http://core.test/api/apps/v1/workspaces/ws_1/agents/codex/token"


def test_a_call_packs_only_what_was_given() -> None:
    call = _call_of(
        client_name="Cursor",
        user="ada",
        meta={"x": 1},
        session="wsc_2",
        harness=" grok-bot ",
        conversation=" bc-1 ",
        user_agent="ua",
    )
    assert call.to_json() == {
        "client": "Cursor",
        "user": "ada",
        "meta": {"x": 1},
        "session": "wsc_2",
        "user_agent": "ua",
        "conversation": "bc-1",
        "harness": "grok-bot",
    }
    assert _call_of(client_name="x", harness="", conversation="  ").to_json() == {"client": "x"}
    assert _call_of(client_name="x", tool=" watch ").to_json() == {"client": "x", "tool": "watch"}


def test_a_call_reads_the_context_and_the_wake_arguments() -> None:
    class Info:
        name = "Cursor"

    class Params:
        client_info = Info()

    class Session:
        client_params = Params()

    class Ctx:
        session = Session()
        meta = {"x-codex-turn-metadata": {"thread_id": "t1"}}
        request = None

    args = {"repo": "x", SESSION_ARG: "wsc_2", HARNESS_ARG: " grok-bot ", CONVERSATION_ARG: " bc-1 "}
    assert _call(Ctx(), user="ada", args=args, tool="subscribe_events").to_json() == {
        "client": "Cursor",
        "user": "ada",
        "meta": {"x-codex-turn-metadata": {"thread_id": "t1"}},
        "session": "wsc_2",
        "conversation": "bc-1",
        "harness": "grok-bot",
        "tool": "subscribe_events",
    }
    assert _call(Ctx(), user=None, args={SESSION_ARG: 7, HARNESS_ARG: ""}, tool="").to_json().keys() == {"client", "meta"}, (
        "only strings, and not empty ones"
    )


def test_wake_args_session_key_harness_and_conversation_id() -> None:
    assert list(WAKE_ARGS_SCHEMA) == ["session_key", "harness", "conversation_id"]
    assert re.fullmatch(
        r"This conversation's session_key, from an earlier result .*Keep it to this conversation; don't share or log it\.",
        WAKE_ARGS_SCHEMA[SESSION_ARG]["description"],
    )
    assert re.fullmatch(
        r"The program you're running in: .*cursor \(local CLI or IDE\), cursor-cloud \(Cursor cloud agent\), grok-bot, .*or your program's name if it isn't listed",
        WAKE_ARGS_SCHEMA[HARNESS_ARG]["description"],
    )
    assert "printenv CURSOR_CONVERSATION_ID" in WAKE_ARGS_SCHEMA[CONVERSATION_ARG]["description"]


def test_the_package_no_longer_exports_guest_sign_in() -> None:
    import wakeshift

    for name in ("GuestAuth", "Guest", "guest_of"):
        assert not hasattr(wakeshift, name), name
        assert name not in wakeshift.__all__, name
    assert not hasattr(wakeshift.Wakeshift, "secret_seed")


def test_the_package_no_longer_exports_topics_or_the_old_subscribe() -> None:
    """0.4.0: one way to subscribe from your own tools (@attached.tool() and
    call.subscribe); topics are gone."""
    import wakeshift

    for name in (
        "Call",
        "CallerSubscription",
        "Addition",
        "apply_addition",
        "TopicSubscription",
        "TopicStatus",
        "SubscriptionStatus",
        "Published",
        "UNKNOWN_CLIENT_WARNING",
    ):
        assert not hasattr(wakeshift, name), name
        assert name not in wakeshift.__all__, name
    for name in (
        "call",
        "call_of",
        "subscribe",
        "unsubscribe",
        "publish",
        "send",
        "topic_status",
        "subscription_status",
        "publish_url",
        "addition",
        "annotate",
        "close_topic",
        "end_subscription",
        "unsubscribe_user",
    ):
        assert not hasattr(wakeshift.Wakeshift, name), name
    for name in ("subscribe", "unsubscribe"):
        assert not hasattr(wakeshift.WakeEvent, name), name
    for name in ("emit", "publish_url", "status", "status_of", "end"):
        assert hasattr(wakeshift.WakeEvent, name), name
    for name in ("revoke_publish_url", "forget_user", "workspace", "add_agent", "check_agent", "attach", "define_event"):
        assert hasattr(wakeshift.Wakeshift, name), name
    assert wakeshift.__version__ == "0.5.2"
