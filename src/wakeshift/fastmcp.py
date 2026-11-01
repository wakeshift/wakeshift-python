"""Wakeshift on FastMCP 4 (the ``fastmcp`` package, ``pip install
'wakeshift[fastmcp]'``): ``wake.attach`` takes a ``FastMCP`` server as it
takes the official SDK's ``MCPServer``, and ``@attached.tool()`` registers a
FastMCP tool. This module holds the helpers that only make sense on FastMCP::

    from fastmcp import FastMCP
    from wakeshift import ToolCall, Wakeshift
    from wakeshift.fastmcp import identify_by_claim

    mcp = FastMCP("chat", auth=my_token_verifier)
    wake = Wakeshift(os.environ["WAKESHIFT_KEY"])
    message = wake.define_event("chat.message", ...)
    attached = wake.attach(mcp, identify=identify_by_claim("sub"))

    @attached.tool()
    async def watch(channel: str, call: ToolCall) -> str:
        await call.subscribe(message, {"channel": channel})
        return f"Watching #{channel}."

    app = mcp.http_app(stateless_http=True)

FastMCP 4 sits on the official SDK 2.x (``mcp>=2.3``), as this SDK does, so
both share one install. FastMCP 2.x and 3.x sit on ``mcp`` 1.x and aren't
supported: attach refuses their servers.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .events import Identity

__all__ = ["access_token", "identify_by_claim"]


def access_token() -> Any:
    """The FastMCP access token of the request being served (None without
    one): fastmcp's get_access_token, which works in a tool, in FastMCP
    middleware and in attach's own handlers (events/*, the wait tool)."""
    from fastmcp.server.dependencies import get_access_token

    return get_access_token()


def identify_by_claim(claim: str = "sub", *, client_claim: str | None = None) -> Callable[[Any], Identity | None]:
    """An identify for attach that names the user by a claim of the
    request's FastMCP access token (its ``claims``; ``sub`` falls back to
    the token's subject), and optionally the client's name by another. A
    request without a token, or a token without the claim, has no user."""

    def identify(_ctx: Any) -> Identity | None:
        tok = access_token()
        if tok is None:
            return None
        claims = getattr(tok, "claims", None) or {}
        user = claims.get(claim)
        if user is None and claim == "sub":
            user = getattr(tok, "subject", None)
        client = claims.get(client_claim) if client_claim else None
        return Identity(
            user=user if isinstance(user, str) and user else None,
            client=client if isinstance(client, str) and client else None,
        )

    return identify
