"""WAKESHIFT_TEST_SERVER=fastmcp runs the suite against FastMCP 4 instead of
the official SDK's MCPServer (docs/fastmcp.md): each test module's MCPServer
and Context become a FastMCP subclass that answers the few MCPServer calls the
tests make, and FastMCP's Context. tests/test_fastmcp.py covers what's
FastMCP's own, whichever server the rest runs on."""

from __future__ import annotations

import os
from typing import Any

import pytest

FASTMCP = os.environ.get("WAKESHIFT_TEST_SERVER") == "fastmcp"


def _compat() -> tuple[type, type]:
    from fastmcp import Context, FastMCP

    class _Tools:
        def __init__(self, server: Any) -> None:
            self._server = server

        def get_tool(self, name: str) -> Any:
            for c in self._server._local_provider._components.values():
                if getattr(c, "name", None) == name and c.key.startswith("tool:"):
                    return c
            return None

    class FastMCPServer(FastMCP):
        """A FastMCP that answers MCPServer's calls the tests make."""

        @property
        def _tool_manager(self) -> _Tools:
            return _Tools(self)

        def tool(self, name_or_fn: Any = None, **kw: Any) -> Any:  # type: ignore[override]
            # MCPServer's structured_output: FastMCP's output schema.
            so = kw.pop("structured_output", None)
            if so is False:
                kw.setdefault("output_schema", None)
            return super().tool(name_or_fn, **kw)

        def streamable_http_app(self, *, stateless_http: bool = True, json_response: bool = True) -> Any:
            return self.http_app(path="/mcp", stateless_http=stateless_http, json_response=json_response)

    return FastMCPServer, Context


#: Where FastMCP differs from MCPServer by design, so the test's expectation
#: doesn't hold on FastMCP.
_DIFFERS = {
    "test_no_session_alias": "FastMCP refuses arguments a tool doesn't declare, so the tool doesn't run",
    "test_schema_hides_call_and_adds_wake_args": "reads MCPServer's list_tools(); test_fastmcp.py checks the same over the wire",
}


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not FASTMCP:
        return
    server_cls, context_cls = _compat()
    seen: set[int] = set()
    for item in items:
        why = _DIFFERS.get(item.name)
        if why:
            item.add_marker(pytest.mark.xfail(reason=why, strict=True))
        mod = item.module  # type: ignore[attr-defined]
        if mod is None or id(mod) in seen:
            continue
        seen.add(id(mod))
        if hasattr(mod, "MCPServer"):
            mod.MCPServer = server_cls
        if hasattr(mod, "Context"):
            mod.Context = context_cls
