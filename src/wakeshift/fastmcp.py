"""Wakeshift on FastMCP 4 (the ``fastmcp`` package, ``pip install
'wakeshift[fastmcp]'``): ``wake.attach`` takes a ``FastMCP`` server as it
takes the official SDK's ``MCPServer``, and ``@attached.tool()`` registers a
FastMCP tool. This module holds the helpers that only make sense on FastMCP::

    from fastmcp import FastMCP
    from wakeshift import ToolCall, Wakeshift
    from wakeshift.fastmcp import identify_by_claim

    mcp = FastMCP("chat", auth=my_token_verifier)
    wake = Wakeshift()
    message = wake.define_event("chat.message", ...)
    attached = wake.attach(mcp, identify=identify_by_claim("sub"))

    @attached.tool(subscribes=[message])
    async def watch(channel: str, call: ToolCall) -> str:
        await call.subscribe(message, {"channel": channel})
        return f"Watching #{channel}."

    app = mcp.http_app(stateless_http=True)

FastMCP 4 sits on the official SDK 2.x (``mcp>=2.3``), as this SDK does, so
both share one install. FastMCP 2.12+ and 3.x sit on ``mcp`` 1.x: install
plain ``wakeshift`` beside them (never ``[mcp]``), and attach, tools that
subscribe, ``wrap`` and the built-in tools work the same; native MCP Events
and protocol 2026-07-28 need FastMCP 4.
"""

from __future__ import annotations

from collections.abc import Callable

from ._events import Identity, Request

__all__ = ["identify_by_claim"]


def identify_by_claim(claim: str = "sub", *, client_claim: str | None = None) -> Callable[[Request], Identity | None]:
    """An identify for attach that names the user by a claim of the
    request's verified access token (``Request.token.claims``; ``sub`` falls
    back to the token's subject), and optionally the client's name by
    another. A request without a token, or a token without the claim, has no
    user."""

    def identify(r: Request) -> Identity | None:
        claims = getattr(r.token, "claims", None) or {}
        user = claims.get(claim) or (getattr(r.token, "subject", None) if claim == "sub" else None)
        client = claims.get(client_claim) if client_claim else None
        return Identity(
            user=user if isinstance(user, str) and user else None, client=client if isinstance(client, str) and client else None
        )

    return identify
