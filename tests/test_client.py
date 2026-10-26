"""The app key, assertions and the client's calls, against a fake core: the
Node SDK's unit tests, in Python."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
from cryptography.exceptions import InvalidSignature
from mcp.types import CallToolResult, TextContent

from wakeshift import (
    CONVERSATION_ARG,
    HARNESS_ARG,
    SESSION_ARG,
    WAKE_ARGS_SCHEMA,
    Wakeshift,
    WakeshiftError,
    is_code,
    parse_key,
    sign_assertion,
)
from wakeshift.key import b64url, b64url_decode, public_key_of

# RFC 8032, test 1: this seed's public key.
SEED_HEX = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
PUB_HEX = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
SEED = b64url(bytes.fromhex(SEED_HEX))
KEY = f"wst_test_ci-bot_{SEED}"


def test_parse_key_reads_the_mode_app_id_and_key() -> None:
    k = parse_key(KEY)
    assert (k.mode, k.app_id) == ("test", "ci-bot")
    assert b64url_decode(public_key_of(k)).hex() == PUB_HEX
    # A seed with "_" and "-" in it: split on the first two underscores only.
    odd = parse_key("wst_live_my-app_3q2-7__7AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    assert (odd.mode, odd.app_id) == ("live", "my-app")
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
    now = datetime(2026, 10, 18, 12, tzinfo=timezone.utc).timestamp()
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


SUBSCRIBE_REPLY = {
    "subscription": {
        "id": "ts_AAAAAAAAAAAAAAAAAAAAAA",
        "topic": "ci/acme/api",
        "filter": {"branch": "main"},
        "once": False,
        "expires_at": "2026-11-17T12:00:00Z",
    },
    "session": "claude-code#1",
    "addition": {"text": "You're claude-code#1. Start the Monitor tool now.", "meta": {"wakeshift/x": "y"}},
}


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


async def test_subscribe_sends_the_call_and_options_and_builds_the_tool_result() -> None:
    core = Core(lambda path, body: (200, SUBSCRIBE_REPLY))
    wake = core.wake()
    call = wake.call_of(client_name="claude-code", user="ada", meta={})
    sub = await wake.subscribe(call, "ci/acme/api", filter={"branch": "main"}, replay="5m", ttl=3600)
    r = core.sent[0]
    assert r["method"] == "POST"
    assert r["url"] == "http://core.test/api/apps/v1/topics/subscribe"
    assert re.fullmatch(r"Bearer [\w-]+\.[\w-]+\.[\w-]+", r["auth"])
    assert SEED not in r["auth"], "the key never goes over the wire"
    assert r["body"] == {
        "call": {"client": "claude-code", "user": "ada"},
        "topic": "ci/acme/api",
        "once": False,
        "filter": {"branch": "main"},
        "replay": "5m",
        "ttl": 3600,
    }
    assert sub.id == "ts_AAAAAAAAAAAAAAAAAAAAAA"
    assert sub.session == "claude-code#1"
    assert "session=" not in repr(sub), "the session key stays out of reprs"
    assert sub.expires_at == datetime(2026, 11, 17, 12, tzinfo=timezone.utc)

    out = sub.result("Watching CI on acme/api.")
    dumped = out.model_dump(by_alias=True, exclude_none=True)
    assert dumped["content"] == [
        {"type": "text", "text": "Watching CI on acme/api.\n\nYou're claude-code#1. Start the Monitor tool now."}
    ]
    assert dumped["_meta"] == {"wakeshift/x": "y"}
    base = CallToolResult(content=[TextContent(type="text", text="mine")], _meta={"mine": 1}, structuredContent={"ok": True})
    merged = sub.result(base).model_dump(by_alias=True, exclude_none=True)
    assert merged["content"] == [
        {"type": "text", "text": "mine"},
        {"type": "text", "text": "You're claude-code#1. Start the Monitor tool now."},
    ]
    assert merged["_meta"] == {"mine": 1, "wakeshift/x": "y"}
    assert merged["structuredContent"] == {"ok": True}
    assert len(base.content) == 1, "the app's result isn't changed"


async def test_errors_carry_the_apis_code() -> None:
    wake = Core(lambda p, b: (403, {"error": {"code": "private_topic", "message": "that topic is another user's"}})).wake()
    with pytest.raises(WakeshiftError) as e:
        await wake.subscribe(wake.call_of(client_name="x"), "u/ada/inbox")
    assert (e.value.status, e.value.code, e.value.detail) == (403, "private_topic", "that topic is another user's")
    assert is_code(e.value, "private_topic")
    assert not is_code(e.value, "bad_request")
    bare = Core(lambda p, b: (502, "bad gateway")).wake()
    with pytest.raises(WakeshiftError) as e2:
        await bare.publish("a", text="x")
    assert (e2.value.status, e2.value.code) == (502, "")


@pytest.mark.parametrize("body", ["", "<html>ok</html>", "null", "42"])
async def test_a_2xx_without_a_json_object_is_a_wakeshift_error(body: str) -> None:
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, text=body, headers={"Content-Type": "text/html"}))
    )
    wake = Wakeshift(KEY, core="http://core.test", http=http)
    with pytest.raises(WakeshiftError) as e:
        await wake.subscribe(wake.call_of(client_name="claude-code"), "ci/x")
    assert (e.value.code, e.value.status) == ("bad_response", 200)
    assert "http://core.test answered 200 without a JSON object" in str(e.value)


async def test_a_core_that_doesnt_answer_the_error_names_it() -> None:
    def refused(r: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    wake = Wakeshift(KEY, core="http://127.0.0.1:9", http=httpx.AsyncClient(transport=httpx.MockTransport(refused)))
    with pytest.raises(WakeshiftError) as e:
        await wake.publish("ci/x", text="x")
    assert (e.value.code, e.value.status) == ("unreachable", 0)
    assert str(e.value).startswith(
        "wakeshift: can't reach Wakeshift at http://127.0.0.1:9 (connection refused). Check that WAKESHIFT_CORE"
    )
    assert isinstance(e.value.__cause__, httpx.ConnectError)

    def slow(r: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out")

    wake2 = Wakeshift(KEY, core="http://core.test", http=httpx.AsyncClient(transport=httpx.MockTransport(slow)), timeout=0.02)
    with pytest.raises(WakeshiftError, match=r"can't reach Wakeshift at http://core\.test \(no answer within 0\.02s\)"):
        await wake2.topic_status("ci/x")


async def test_the_other_calls_paths_bodies_and_results() -> None:
    replies: dict[str, Any] = {
        "/api/apps/v1/topics/unsubscribe": {"unsubscribed": 2},
        "/api/apps/v1/topics/publish": {"matched": 3, "duplicate": False},
        "/api/apps/v1/subscriptions/ts_a%2Fb/send": {"status": "queued"},
        "/api/apps/v1/topics/publish-urls": {"id": "u1", "topic": "hooks/x", "url": "http://core.test/v1/publish/wsp_x"},
        "/api/apps/v1/events/publish-urls/u1": {"revoked": True},
        "/api/apps/v1/topics/annotate": {"text": "waiting: 1", "meta": {"wakeshift/connector": "beacon"}},
        "/api/apps/v1/topics/close": {"closed": 4},
        "/api/apps/v1/users/forget": {"forgotten": True},
        "/api/apps/v1/topics/status": {"topic": "ci/x", "subscriptions": 3, "connected": 2, "via": {"stream": 1, "hooks": 1}},
        "/api/apps/v1/subscriptions/ts_a%2Fb": {
            "id": "ts_a/b",
            "topic": "ci/x",
            "status": "active",
            "connected": True,
            "via": "hooks",
            "expires_at": "2026-11-17T12:00:00Z",
        },
        "/api/apps/v1/subscriptions/ts_a%2Fb/end": {"ended": True},
    }
    core = Core(lambda path, body: (200, replies[path]) if path in replies else (404, {"error": {"code": "not_found"}}))
    wake = core.wake()
    call = wake.call_of(client_name="codex-mcp-client", meta={"threadId": "t1"})

    assert await wake.unsubscribe(call) == 2
    assert core.sent[-1]["body"] == {"call": {"client": "codex-mcp-client", "meta": {"threadId": "t1"}}}
    await wake.unsubscribe(call, "ci/x")
    assert core.sent[-1]["body"]["topic"] == "ci/x"

    p = await wake.publish("ci/x", id="d1", text="CI failed", data={"branch": "main"})
    assert (p.matched, p.duplicate) == (3, False)
    assert core.sent[-1]["body"] == {"topics": ["ci/x"], "event": {"id": "d1", "text": "CI failed", "data": {"branch": "main"}}}
    await wake.publish(["a", "b"], text="x")
    assert core.sent[-1]["body"]["topics"] == ["a", "b"]

    assert await wake.send("ts_a/b", text="just you") == "queued"
    assert core.sent[-1]["body"] == {"event": {"text": "just you"}}

    u = await wake.publish_url("hooks/x")
    assert (u.id, u.topic, u.url) == ("u1", "hooks/x", "http://core.test/v1/publish/wsp_x")
    await wake.revoke_publish_url("u1")
    assert core.sent[-1]["method"] == "DELETE"
    assert core.sent[-1]["body"] is None

    annotated = await wake.annotate(call, CallToolResult(content=[TextContent(type="text", text="3 builds")]))
    dumped = annotated.model_dump(by_alias=True, exclude_none=True)
    assert dumped["content"] == [{"type": "text", "text": "3 builds"}, {"type": "text", "text": "waiting: 1"}]
    assert dumped["_meta"] == {"wakeshift/connector": "beacon"}

    st = await wake.topic_status("ci/x")
    assert (st.topic, st.subscriptions, st.connected, st.via) == (
        "ci/x",
        3,
        2,
        {"stream": 1, "poll": 0, "hooks": 1, "webhook": 0},
    )
    assert core.sent[-1]["method"] == "GET"
    assert core.sent[-1]["url"] == "http://core.test/api/apps/v1/topics/status?topic=ci%2Fx"
    ss = await wake.subscription_status("ts_a/b")
    assert (ss.id, ss.status, ss.connected, ss.via) == ("ts_a/b", "active", True, "hooks")
    assert ss.expires_at == datetime(2026, 11, 17, 12, tzinfo=timezone.utc)

    assert await wake.close_topic("ci/x") == 4
    assert await wake.end_subscription("ts_a/b") is True
    assert core.sent[-1]["url"] == "http://core.test/api/apps/v1/subscriptions/ts_a%2Fb/end"
    assert await wake.unsubscribe_user("bob", "org/org1/deploys") == 2
    assert core.sent[-1]["body"] == {"user": "bob", "topic": "org/org1/deploys"}
    await wake.unsubscribe_user("bob")
    assert core.sent[-1]["body"] == {"user": "bob"}
    assert await wake.forget_user("ada") is True
    assert core.sent[-1]["body"] == {"user": "ada"}


def test_call_of_packs_only_what_was_given() -> None:
    wake = Wakeshift(KEY, core="http://core.test")
    call = wake.call_of(
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
    assert wake.call_of(client_name="x", harness="", conversation="  ").to_json() == {"client": "x"}


def test_call_reads_the_context_and_the_wake_arguments() -> None:
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

    wake = Wakeshift(KEY, core="http://core.test")
    args = {"repo": "x", SESSION_ARG: "wsc_2", HARNESS_ARG: " grok-bot ", CONVERSATION_ARG: " bc-1 "}
    assert wake.call(Ctx(), user="ada", args=args).to_json() == {
        "client": "Cursor",
        "user": "ada",
        "meta": {"x-codex-turn-metadata": {"thread_id": "t1"}},
        "session": "wsc_2",
        "conversation": "bc-1",
        "harness": "grok-bot",
    }
    assert wake.call(Ctx(), client="cursor-nightly", args={SESSION_ARG: 7, HARNESS_ARG: ""}).client == "cursor-nightly"
    assert wake.call(Ctx(), args={SESSION_ARG: 7, HARNESS_ARG: ""}).to_json().keys() == {"client", "meta"}, (
        "only strings, and not empty ones"
    )


def test_wake_args_session_key_harness_and_conversation_id() -> None:
    assert list(WAKE_ARGS_SCHEMA) == ["session_key", "harness", "conversation_id"]
    assert "secret: keep it to this conversation; don't share or log it" in WAKE_ARGS_SCHEMA[SESSION_ARG]["description"]
    assert re.fullmatch(
        r"The program you're running in: .*cursor \(local CLI or IDE\), cursor-cloud \(Cursor cloud agent\), grok-bot, .*or your program's name if it isn't listed",
        WAKE_ARGS_SCHEMA[HARNESS_ARG]["description"],
    )
    assert "printenv CURSOR_CONVERSATION_ID" in WAKE_ARGS_SCHEMA[CONVERSATION_ARG]["description"]


async def test_addition_warns_once_when_the_core_recognised_nothing() -> None:
    warnings: list[str] = []
    wake = Core(lambda p, b: (200, {"text": "t", "agent": {}})).wake(on_warning=warnings.append)
    a = await wake.addition(wake.call_of(client_name=""))
    assert a.agent is not None and a.agent.harness is None
    await wake.addition(wake.call_of(client_name=""))
    assert len(warnings) == 1 and "couldn't tell which agent" in warnings[0]
    quiet: list[str] = []
    wake2 = Core(lambda p, b: (200, {"agent": {"harness": "claude-code", "via": "client"}})).wake(on_warning=quiet.append)
    a2 = await wake2.addition(wake2.call_of(client_name="claude-code"))
    assert a2.agent is not None and (a2.agent.harness, a2.agent.via) == ("claude-code", "client")
    assert quiet == []
