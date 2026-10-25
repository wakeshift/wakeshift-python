"""Retrying a request the load balancer refused (the Node SDK's retry.ts and
the Go SDK's retry.go, in Python).

Wakeshift runs behind Google's load balancer on Cloud Run. For some minutes
after a rollout, the load balancer sends a small share of requests to a
retired revision, which answers at once with Google's own HTML "500 Server
Error" page: the request never reached a Wakeshift instance. It happens to
every client alike (new connections included), so retrying is what gets the
request through.
"""

from __future__ import annotations

import httpx

#: Pauses (seconds) before each retry of a refused request (so at most len + 1 attempts).
RETRY_PAUSES: tuple[float, ...] = (0.25, 1.0)


def _media_type(resp: httpx.Response) -> str:
    return resp.headers.get("Content-Type", "").split(";")[0].strip().lower()


def refused(method: str, resp: httpx.Response) -> bool:
    """Whether resp is the load balancer's refusal, not Wakeshift's answer: Google's
    HTML 500 page, for any request (it never ran), and a 502, 503 or 504 not from
    Wakeshift for a GET or HEAD. Wakeshift's own errors (JSON) never are."""
    if resp.status_code == 500:
        return _media_type(resp) == "text/html" and "Server Error" in resp.text
    if resp.status_code in (502, 503, 504):
        return method.upper() in ("GET", "HEAD") and _media_type(resp) != "application/json"
    return False
