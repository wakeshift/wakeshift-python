"""errors.json: how every SDK reads Wakeshift's answers (which refusals it
retries, the error each answer becomes, and the JSON-RPC error each becomes
for an MCP Events client). The Go and Node SDKs test against the same file."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from wakeshift import Wakeshift, WakeshiftError
from wakeshift.events import _rpc_error_of
from wakeshift.retry import RETRY_PAUSES, refused
from wakeshift.spec import codes_for

from .helpers import app_key, golden_file

GOLDEN = golden_file("errors.json")
CORE = "http://core.test"


def response(a: dict[str, Any]) -> httpx.Response:
    headers = {"Content-Type": a["content_type"]} if a["content_type"] else {}
    return httpx.Response(a["status"], headers=headers, content=a["body"].encode())


async def error_for(a: dict[str, Any]) -> BaseException:
    """The error a forget_user (a POST, so nothing here is retried) gets for an answer."""
    wake = Wakeshift(app_key(), core=CORE, http=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: response(a))))
    with pytest.raises(WakeshiftError) as e:
        await wake.forget_user("ada")
    return e.value


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
    assert isinstance(err, WakeshiftError)
    got = {"status": err.status, "code": err.code, "message": err.detail, "text": str(err)}
    if err.reason is not None:
        got["reason"] = err.reason
    assert got == case["want"]


@pytest.mark.parametrize("case", GOLDEN["bad_response"]["cases"], ids=lambda c: c["name"])
async def test_a_2xx_without_a_json_object(case: dict[str, Any]) -> None:
    err = await error_for(case)
    assert isinstance(err, WakeshiftError)
    message = GOLDEN["bad_response"]["message"].replace("{core}", CORE).replace("{status}", str(case["status"]))
    assert (err.status, err.code, err.detail) == (case["status"], GOLDEN["bad_response"]["code"], message)


async def test_a_core_that_doesnt_answer_is_unreachable() -> None:
    def down(r: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    wake = Wakeshift(app_key(), core=CORE, http=httpx.AsyncClient(transport=httpx.MockTransport(down)))
    with pytest.raises(WakeshiftError) as e:
        await wake.forget_user("ada")
    u = GOLDEN["unreachable"]
    assert (e.value.status, e.value.code) == (u["status"], u["code"])
    assert e.value.detail.startswith(u["message_prefix"].replace("{core}", CORE))
    assert e.value.detail.endswith(u["message_suffix"])
    assert str(e.value) == "wakeshift: " + e.value.detail


@pytest.mark.parametrize("case", GOLDEN["rpc"], ids=lambda c: c["name"])
@pytest.mark.parametrize("dialect", ["draft", "chatgpt"])
def test_the_json_rpc_error_each_becomes(case: dict[str, Any], dialect: Any) -> None:
    e = case["error"]
    err: BaseException = (
        WakeshiftError(400, e["code"], e["message"], reason=e.get("reason")) if e else ConnectionError("connection refused")
    )
    got = _rpc_error_of(err, codes_for(dialect))
    want = case["want"]
    assert (got.code, got.message, got.data) == (want["code"][dialect], want["message"], want["data"])  # type: ignore[attr-defined]
