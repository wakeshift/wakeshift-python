"""errors.json: how every SDK reads Wakeshift's answers (which refusals it
retries, the error each answer becomes, and the JSON-RPC error each becomes
for an MCP Events client), the one list of codes with each code's class and
fix, and the format of every message the SDK writes itself. The Go and
TypeScript SDKs test against the same file."""

from __future__ import annotations

import typing
from typing import Any

import httpx2
import pytest

from wakeshift import (
    APIConnectionError,
    APIError,
    ErrorCode,
    IdReusedError,
    UsageError,
    Wakeshift,
    WakeshiftError,
)
from wakeshift._errors import FIXES, anchor, usage
from wakeshift._events import _rpc_error_of
from wakeshift._retry import RETRY_PAUSES, refused
from wakeshift._spec import codes_for

from .helpers import app_key, golden_file

GOLDEN = golden_file("errors.json")
CORE = "https://core.test"
CLASSES: dict[str, type[WakeshiftError]] = {
    "UsageError": UsageError,
    "APIError": APIError,
    "APIConnectionError": APIConnectionError,
    "IdReusedError": IdReusedError,
}


def response(a: dict[str, Any]) -> httpx2.Response:
    headers = {"Content-Type": a["content_type"]} if a["content_type"] else {}
    return httpx2.Response(a["status"], headers=headers, content=a["body"].encode())


async def error_for(a: dict[str, Any]) -> APIError:
    """The error a forget_user (a POST, so nothing here is retried) gets for an answer."""
    wake = Wakeshift(app_key(), core=CORE, http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(lambda r: response(a))))
    with pytest.raises(APIError) as e:
        await wake.forget_user("ada")
    return e.value


def test_the_codes_are_the_shared_list() -> None:
    codes = [c["code"] for c in GOLDEN["codes"]]
    assert list(typing.get_args(ErrorCode)) == codes
    assert list(FIXES) == codes
    for c in GOLDEN["codes"]:
        assert FIXES[c["code"]] == c.get("fix_python", c["fix"]), c["code"]
        assert anchor(c["code"]) == c["anchor"]


def test_the_classes() -> None:
    assert issubclass(APIError, WakeshiftError)
    assert issubclass(APIConnectionError, APIError)
    assert issubclass(IdReusedError, APIError)
    assert issubclass(UsageError, WakeshiftError) and issubclass(UsageError, ValueError)
    # Every class a code lists is one the SDK has.
    for c in GOLDEN["codes"]:
        for cls in c["class"].values():
            assert cls is None or cls in CLASSES, (c["code"], cls)


@pytest.mark.parametrize("c", [c for c in GOLDEN["codes"] if c["class"]["before_sending"]], ids=lambda c: c["code"])
def test_a_usage_error_follows_the_message_format(c: dict[str, Any]) -> None:
    err = usage(c["code"], "start_crawl subscribed to crawl.finished, which isn't in its subscribes")
    assert isinstance(err, CLASSES[c["class"]["before_sending"]])
    text = str(err)
    assert text.startswith("wakeshift: ")
    assert c.get("fix_python", c["fix"]) in text
    assert text.endswith(" " + c["anchor"])
    assert err.code == c["code"]


def test_the_example_message() -> None:
    m = GOLDEN["messages"]
    err = usage("undeclared_subscribe", "start_crawl subscribed to crawl.finished, which isn't in its subscribes")
    assert str(err) == m["example"]


def test_pauses() -> None:
    assert [round(p * 1000) for p in RETRY_PAUSES] == GOLDEN["retry"]["pauses_ms"]


@pytest.mark.parametrize("case", GOLDEN["retry"]["cases"], ids=lambda c: c["name"])
def test_which_refusals_are_retried(case: dict[str, Any]) -> None:
    resp = response(case)
    assert refused(case["method"], resp) is case["retried"]
    assert resp.text == case["body"]


@pytest.mark.parametrize("case", GOLDEN["responses"], ids=lambda c: c["name"])
async def test_the_error_each_answer_becomes(case: dict[str, Any]) -> None:
    err = await error_for(case)
    assert type(err) is APIError
    got = {"status": err.status, "code": err.code, "message": err.detail, "text": str(err)}
    if err.reason is not None:
        got["reason"] = err.reason
    assert got == case["want"]


async def test_the_request_id_is_kept() -> None:
    case = {"status": 500, "content_type": "application/json", "body": '{"error":{"code":"internal","message":"x"}}'}

    def answer(r: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(500, headers={"Content-Type": "application/json", "X-Request-Id": "req_1"}, content=case["body"])

    wake = Wakeshift(app_key(), core=CORE, http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(answer)))
    with pytest.raises(APIError) as e:
        await wake.forget_user("ada")
    assert e.value.request_id == "req_1"


@pytest.mark.parametrize("case", GOLDEN["bad_response"]["cases"], ids=lambda c: c["name"])
async def test_a_2xx_without_a_json_object(case: dict[str, Any]) -> None:
    err = await error_for(case)
    message = GOLDEN["bad_response"]["message"].replace("{core}", CORE).replace("{status}", str(case["status"]))
    assert (err.status, err.code, err.detail) == (case["status"], GOLDEN["bad_response"]["code"], message)


async def test_a_core_that_doesnt_answer_is_unreachable() -> None:
    def down(r: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("connection refused")

    wake = Wakeshift(app_key(), core=CORE, http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(down)))
    with pytest.raises(APIConnectionError) as e:
        await wake.forget_user("ada")
    u = GOLDEN["unreachable"]
    assert (e.value.status, e.value.code) == (None, u["code"])
    text = str(e.value)
    prefix = "wakeshift: " + u["message_prefix"].replace("{core}", CORE)
    assert text == prefix + "connection refused" + u["message_suffix"] + ". " + anchor("unreachable")
    assert FIXES["unreachable"] in text
    assert isinstance(e.value.__cause__, httpx2.ConnectError)


async def test_no_answer_in_time_is_a_timeout() -> None:
    def slow(r: httpx2.Request) -> httpx2.Response:
        raise httpx2.ReadTimeout("timed out")

    wake = Wakeshift(app_key(), core=CORE, http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(slow)), timeout=0.02)
    with pytest.raises(APIConnectionError) as e:
        await wake.forget_user("ada")
    assert (e.value.status, e.value.code) == (None, "timeout")
    assert str(e.value) == GOLDEN["messages"]["timeout"].replace("{core}", CORE)
    assert isinstance(e.value.__cause__, httpx2.ReadTimeout)


@pytest.mark.parametrize("case", GOLDEN["rpc"], ids=lambda c: c["name"])
@pytest.mark.parametrize("dialect", ["draft", "chatgpt"])
def test_the_json_rpc_error_each_becomes(case: dict[str, Any], dialect: Any) -> None:
    e = case["error"]
    err: BaseException
    if not e:
        err = ConnectionError("connection refused")
    elif e["code"] == "unreachable":
        err = APIConnectionError(e["message"])
    else:
        err = APIError(e["message"], code=e["code"], status=400, reason=e.get("reason"))
    got = _rpc_error_of(err, codes_for(dialect))
    want = case["want"]
    assert (got.code, got.message, got.data) == (want["code"][dialect], want["message"], want["data"])  # type: ignore[attr-defined]
