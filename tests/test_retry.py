"""Requests the load balancer refused are sent again (the Node SDK's retry.test.ts)."""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest

from wakeshift import Wakeshift, WakeshiftError
from wakeshift.retry import refused

from .helpers import app_key

# Google's front end's 500 page, as a retired Cloud Run revision answers through the load balancer.
GFE_PAGE = """<html><head>
<meta http-equiv="content-type" content="text/html;charset=utf-8">
<title>500 Server Error</title>
</head>
<body text=#000000 bgcolor=#ffffff>
<h1>Error: Server Error</h1>
<h2>The server encountered an error and could not complete your request.<p>Please try again in 30 seconds.</h2>
<h2></h2>
</body></html>"""


def gfe500() -> httpx.Response:
    return httpx.Response(500, text=GFE_PAGE, headers={"Content-Type": "text/html; charset=UTF-8"})


def as_json(status: int, body: object) -> Callable[[], httpx.Response]:
    return lambda: httpx.Response(status, content=json.dumps(body), headers={"Content-Type": "application/json"})


def scripted(answers: list[Callable[[], httpx.Response]]) -> tuple[list[bytes], Wakeshift]:
    bodies: list[bytes] = []

    def handler(r: httpx.Request) -> httpx.Response:
        bodies.append(r.content)
        return answers[min(len(bodies), len(answers)) - 1]()

    wake = Wakeshift(app_key(), core="http://core.test", http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    wake._retry_pauses = (0.001, 0.001)  # pyright: ignore[reportPrivateUsage]
    return bodies, wake


async def test_a_publish_the_load_balancer_refused_is_sent_again_body_and_all() -> None:
    bodies, wake = scripted([gfe500, as_json(200, {"matched": 1})])
    r = await wake.publish("ci/x", text="hi")
    assert r.matched == 1
    assert len(bodies) == 2
    assert bodies[0] == bodies[1]


async def test_wakeshifts_own_errors_arent_retried_and_a_refusal_that_keeps_coming_is_the_error() -> None:
    bodies, wake = scripted([as_json(500, {"error": {"code": "internal", "message": "x"}})])
    with pytest.raises(WakeshiftError) as e:
        await wake.publish("ci/x", text="x")
    assert e.value.status == 500
    assert len(bodies) == 1

    bodies, wake = scripted([gfe500])
    with pytest.raises(WakeshiftError) as e:
        await wake.publish("ci/x", text="x")
    assert e.value.status == 500
    assert len(bodies) == 3
    assert "Server Error" in e.value.detail


@pytest.mark.parametrize(
    ("method", "resp", "want"),
    [
        ("POST", httpx.Response(500, text="<p>oops</p>", headers={"Content-Type": "text/html"}), False),
        ("GET", httpx.Response(503, text="Service Unavailable", headers={"Content-Type": "text/html"}), True),
        ("POST", httpx.Response(503, text="Service Unavailable", headers={"Content-Type": "text/html"}), False),
        ("GET", httpx.Response(503, json={"error": {"code": "unavailable"}}), False),
        ("POST", gfe500(), True),
        ("GET", httpx.Response(404, text="Not Found", headers={"Content-Type": "text/html"}), False),
    ],
)
def test_which_answers_are_retried(method: str, resp: httpx.Response, want: bool) -> None:
    assert refused(method, resp) is want
