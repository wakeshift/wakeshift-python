"""Secrets and argument values (design §2.13, §4, PR 9): secret-pattern.json and
arg-values.json, SessionKey in every printer, and the canary leak test: a
canary caller token, a canary session key and the test key appear in no log
line, result, error or printed SDK object, nor in the locals Sentry captures
from the SDK's frames."""

from __future__ import annotations

import base64
import dataclasses
import io
import json
import logging
import pickle
import pprint
from typing import Any

import httpx2
import pytest
import sentry_sdk
from mcp.server.auth.provider import AccessToken
from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer as OfficialMCPServer
from pydantic import BaseModel
from rich.pretty import pretty_repr

from wakeshift import (
    Identity,
    PublishURL,
    Request,
    SessionKey,
    Subscriber,
    ToolCall,
    UsageError,
    Wakeshift,
    decode_arg_value,
    encode_arg_value,
    redact_secrets,
)

from .helpers import FakeCore, Logged, golden_file, initialize, served

PATTERN = golden_file("secret-pattern.json")
ARGS = golden_file("arg-values.json")


@pytest.mark.parametrize("v", PATTERN["vectors"], ids=lambda v: v["name"])
def test_redact_secrets(v: dict[str, str]) -> None:
    assert redact_secrets(v["input"]) == v["want"]
    assert redact_secrets(v["want"]) == v["want"]
    if v["name"].startswith("unchanged:"):
        assert v["want"] == v["input"]


@pytest.mark.parametrize("v", ARGS["encode"], ids=lambda v: v["name"])
def test_encode_arg_value(v: dict[str, str]) -> None:
    got = encode_arg_value(v["input"])
    assert got == v["want"]
    assert len(got) <= 100 and all(c in "abcdefghijklmnopqrstuvwxyz0123456789._-" for c in got)
    if not got.startswith("..h"):
        assert decode_arg_value(got) == v["input"]


@pytest.mark.parametrize("v", [v for v in ARGS["refused_encode"] if v["input"] is not None], ids=lambda v: v["name"])
def test_encode_refuses_strings_that_arent_utf8(v: dict[str, Any]) -> None:
    with pytest.raises(UsageError) as e:
        encode_arg_value(v["input"])
    assert e.value.code == "bad_arguments" and v["input"] not in str(e.value)


@pytest.mark.parametrize("v", ARGS["decode"], ids=lambda v: v["name"])
def test_decode_arg_value(v: dict[str, str]) -> None:
    assert decode_arg_value(v["input"]) == v["want"]


@pytest.mark.parametrize("v", ARGS["refused_decode"], ids=lambda v: v["name"])
def test_decode_refuses_what_isnt_canonical(v: dict[str, str]) -> None:
    with pytest.raises(UsageError) as e:
        decode_arg_value(v["input"])
    assert e.value.code == "bad_arguments" and v["input"] not in str(e.value)


CANARY_TOKEN = "canary-bearer-7f3a9c1e5b"
CANARY_SESSION = "wsc_canarysessionkey7f3a9c"
CORE_SESSION = "wsc_canaryfromcore5b2d8e"
_KEYS = golden_file("app-keys.json")
KEY = next(a for a in _KEYS["accepted"] if a["seed"] == "S" and a["key"].startswith("wst_1"))
SEED = base64.urlsafe_b64decode(next(s["seed"] for s in _KEYS["seeds"] if s["name"] == "S") + "=")


def canaries() -> list[str]:
    seed = SEED
    return [
        CANARY_TOKEN,
        CANARY_SESSION,
        CORE_SESSION,
        KEY["key"],
        KEY["key"][4:],
        seed.hex(),
        base64.b64encode(seed).decode(),
        base64.urlsafe_b64encode(seed).decode().rstrip("="),
        repr(seed),
        str(list(seed)),
    ]


def test_session_key_never_prints() -> None:
    k = SessionKey(CANARY_SESSION)
    for text in (
        repr(k),
        str(k),
        f"{k}",
        f"{k:>40}",
        "%s" % k,  # noqa: UP031  (the %-style printer too)
        json.dumps({"k": k}, default=str),
        pprint.pformat(k),
        pretty_repr(k),
    ):
        assert CANARY_SESSION not in text, text
    with pytest.raises(TypeError):
        pickle.dumps(k)
    assert k == SessionKey(CANARY_SESSION) and k.reveal() == CANARY_SESSION
    assert dataclasses.replace is not None
    assert PublishURL(id="p1", url="https://app.wakeshift.ai/p/wsp_secretpublishtoken123").url.endswith("123")
    assert "wsp_secretpublishtoken123" not in repr(
        PublishURL(id="p1", url="https://app.wakeshift.ai/p/wsp_secretpublishtoken123")
    )


class Table(BaseModel):
    table: str


class Msg(BaseModel):
    text: str


def printed(obj: Any) -> list[str]:
    """obj in every printer Python has."""
    out = [repr(obj), str(obj), pprint.pformat(obj), pretty_repr(obj), json.dumps(obj, default=str)]
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        out.append(repr(dataclasses.asdict(obj)))
        out.append(json.dumps(dataclasses.asdict(obj), default=str))
    buf = io.StringIO()
    h = logging.StreamHandler(buf)
    log = logging.getLogger("leak-test")
    log.addHandler(h)
    log.warning("%s %r", obj, obj)
    log.removeHandler(h)
    out.append(buf.getvalue())
    return out


async def test_no_canary_leaks(capsys: pytest.CaptureFixture[str]) -> None:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (
            200,
            {"event_subscription": {"id": "es_1", "user": b["call"].get("user"), "created": True}, "session": CORE_SESSION},
        ),
    )
    core.on("POST", "/calls/addition", lambda b: (200, {"text": "Wakeshift: woken."}))
    core.on(
        "POST", "/events/publish-urls", lambda b: (200, {"id": "p1", "url": "https://app.wakeshift.ai/p/wsp_canarypublish00001"})
    )
    wake = Wakeshift(
        KEY["key"], core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler))
    )
    news = wake.define_event("table.news", description="News", args=Table, payload=Msg, authorize=lambda s, a: True)

    class Verifier:
        async def verify_token(self, token: str) -> AccessToken | None:
            return AccessToken(token=token, client_id="cli", scopes=[], subject="ada") if token == CANARY_TOKEN else None

    server = OfficialMCPServer(
        "cards",
        token_verifier=Verifier(),
        auth=AuthSettings(
            issuer_url="https://auth.example", resource_server_url="https://cards.example/mcp", validate_token_resource=False
        ),
    )
    requests: list[Request] = []

    def identify(r: Request) -> Identity:
        requests.append(r)
        return Identity(user=r.token.subject if r.token else None)

    attached = wake.attach(server, identify=identify)
    logged = Logged(logging.DEBUG)
    kept: dict[str, Any] = {}
    events: list[dict[str, Any]] = []

    def before_send(event: Any, hint: Any) -> None:
        events.append(event)
        return None

    sentry_sdk.init(
        dsn="https://k@sentry.invalid/1",
        before_send=before_send,
        include_local_variables=True,
        attach_stacktrace=True,
        default_integrations=False,
    )
    try:

        @attached.tool(subscribes=[news])
        async def watch(table: str, call: ToolCall) -> str:
            sub = await call.subscribe(news, {"table": table})
            kept.update(call=call, sub=sub)
            try:
                await call.subscribe(wake.define_event("x.y", description="X", args=Table, payload=Msg), {"table": table})
            except UsageError as err:
                kept["error"] = err
            sentry_sdk.capture_message("inside a tool that subscribed")
            return "watching"

        async with served(server, stateless=False) as (_, rpc):
            rpc.headers["Authorization"] = f"Bearer {CANARY_TOKEN}"
            await initialize(rpc, "claude-code")
            res = await rpc("tools/call", {"name": "watch", "arguments": {"table": "oak", "session_key": CANARY_SESSION}})
            listed = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {"session_key": CANARY_SESSION}})
        url = await news.create_publish_url({"table": "oak"})
    finally:
        sentry_sdk.init(dsn=None)

    assert not res["result"].get("isError"), res
    texts = [json.dumps(res), json.dumps(listed)]
    texts += logged + [
        json.dumps({k: v for k, v in vars(r).items() if k.startswith("wakeshift_")}, default=str) for r in logged.records
    ]
    out = capsys.readouterr()
    texts += [out.out, out.err, str(kept["error"])]
    for obj in (
        wake,
        kept["call"],
        kept["sub"],
        requests[0],
        Subscriber(user="ada", client="c", native=False, request=requests[0]),
        url,
        kept["sub"].session,
    ):
        texts += printed(obj)
    # Sentry's captured locals, in the SDK's own frames and the tool's.
    assert events, "Sentry captured the message"
    frames = [
        f
        for e in events
        for t in (e.get("threads", {}).get("values") or [])
        for f in (t.get("stacktrace") or {}).get("frames", [])
    ]
    frames += [f for e in events for f in (e.get("stacktrace") or {}).get("frames", [])]
    ours = [f for f in frames if "wakeshift" in (f.get("module") or "") or f.get("function") == "watch"]
    assert ours, "the SDK's frames were captured"
    texts += [json.dumps(f.get("vars") or {}, default=str) for f in ours]
    for text in texts:
        for canary in canaries():
            assert canary not in text, (canary, text[:400])
    # The values went where they must.
    assert core.bodies("/event-subscriptions")[0]["call"]["session"] == CANARY_SESSION
    assert kept["sub"].session.reveal() == CORE_SESSION and requests[0].token.token == CANARY_TOKEN
