"""HTTP 400 for every HeaderMismatch Wakeshift's events/* refuse with, on
every Streamable HTTP path (the 2026-07-28 transport's Server Validation;
SEP-3415's Routing Headers).

On protocol 2026-07-28 the ``mcp`` host already answers a handler's -32020
with HTTP 400. An ``initialize`` session's request (stateful or stateless)
goes through the legacy transport, which answers every handler error with
HTTP 200, and on an SSE response has sent its status before the handler
runs. So the request is checked before the host sees it: the session
manager's request handler is wrapped, for the low-level servers attach
serves events on only, and a refused events/* request is answered with HTTP
400 and -32020 there, from the same checks the handlers run in-band. Any
other request goes on with its body as it was read.

The hook is on ``mcp``'s ``StreamableHTTPSessionManager._handle_request``
(which ``MCPServer``, the low-level ``Server`` and FastMCP 4 all serve
through), wrapped once per process; a manager it doesn't know, or one made
before attach, keeps the in-band answer (-32020 at the host's status).
"""

from __future__ import annotations

import json
import weakref
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

#: Why a request is refused (HeaderMismatch), or None: method, params, and a
#: header's fields by its lowercase name (empty when absent).
Check = Callable[[str, dict[str, Any], Callable[[str], Sequence[str]]], "str | None"]

_CHECKS: weakref.WeakKeyDictionary[Any, Check] = weakref.WeakKeyDictionary()
_WRAPPED = "_wakeshift_header_mismatch_400"
#: The most of a body read to check it: the host's own default limit.
_MAX_BODY = 4 * 1024 * 1024
_HEADER_MISMATCH = -32020


def header_mismatch_as_400(low: Any, check: Check) -> bool:
    """Has HeaderMismatch refusals of events/* on low answered with HTTP 400
    (see the module's doc). False where the host's shape is unknown."""
    try:
        from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    except ImportError:
        return False
    found = getattr(StreamableHTTPSessionManager, "_handle_request", None)
    if found is None or not callable(found):
        return False
    inner = cast("Callable[..., Awaitable[None]]", found)
    try:
        _CHECKS[low] = check
    except TypeError:  # not weakly referenceable
        return False
    if not getattr(inner, _WRAPPED, False):

        async def _handle_request(self: Any, scope: Any, receive: Any, send: Any) -> None:
            check = _CHECKS.get(getattr(self, "app", None))
            if check is None or scope.get("type") != "http" or scope.get("method") != "POST":
                await inner(self, scope, receive, send)
                return
            replay, refusal = await _refusal(scope, receive, check)
            if refusal is not None:
                await _answer_400(send, *refusal)
                return
            await inner(self, scope, replay, send)

        setattr(_handle_request, _WRAPPED, True)
        StreamableHTTPSessionManager._handle_request = _handle_request  # type: ignore[method-assign]
    return True


async def _refusal(scope: Any, receive: Any, check: Check) -> tuple[Any, tuple[Any, str] | None]:
    """The request's body read (a receive that gives it again), and its
    refusal (the JSON-RPC id and message) if it's an events/* request
    check refuses."""
    messages: list[dict[str, Any]] = []
    body = bytearray()
    while True:
        m = await receive()
        messages.append(m)
        if m.get("type") != "http.request":
            break
        body.extend(m.get("body", b""))
        if not m.get("more_body", False) or len(body) > _MAX_BODY:
            break

    async def replay() -> Any:
        if messages:
            return messages.pop(0)
        return await receive()

    if len(body) > _MAX_BODY:
        return replay, None
    try:
        msg = json.loads(bytes(body))
    except ValueError:
        return replay, None
    if not isinstance(msg, dict) or "id" not in msg:
        return replay, None
    method = msg.get("method")
    if not isinstance(method, str) or not method.startswith("events/"):
        return replay, None
    params = msg.get("params")
    if not isinstance(params, dict):
        params = {}
    fields: dict[str, list[str]] = {}
    for k, v in scope.get("headers") or []:
        try:
            fields.setdefault(k.decode("latin-1").lower(), []).append(v.decode("latin-1"))
        except (AttributeError, UnicodeDecodeError):
            continue
    try:
        refused = check(method, params, lambda name: fields.get(name, []))
    except Exception:  # a check that fails refuses nothing here; the handler answers in-band
        refused = None
    return replay, (msg["id"], refused) if refused else None


async def _answer_400(send: Any, rid: Any, message: str) -> None:
    body = json.dumps({"jsonrpc": "2.0", "id": rid, "error": {"code": _HEADER_MISMATCH, "message": message}}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 400,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})
