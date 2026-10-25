"""Shared test helpers: a fake core, an MCPServer served in-process over
Streamable HTTP, and raw JSON-RPC calls to it."""

from __future__ import annotations

import base64
import json
import secrets
import zlib
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx

from wakeshift import Wakeshift

# The golden files the Go and Node SDKs test against too: beside the Node
# SDK's tests in Wakeshift's repository, copied to tests/golden in the export.
GOLDEN = Path(__file__).resolve().parent / "golden"
if not GOLDEN.is_dir():
    GOLDEN = Path(__file__).resolve().parents[2] / "typescript" / "test"


def golden_file(name: str) -> Any:
    """One of the golden files (event-tools.json, events-list.json, agent-ids.json)."""
    return json.loads((GOLDEN / name).read_text())


def sorted_json(v: Any) -> Any:
    """v with every object's keys in order, as Go writes JSON."""
    if isinstance(v, list):
        return [sorted_json(x) for x in v]
    if isinstance(v, dict):
        return {k: sorted_json(v[k]) for k in sorted(v)}
    return v


def app_key(app: str = "sample") -> str:
    """An older key (wst_test_…), which names its app."""
    return f"wst_test_{app}_" + base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()


def format_key(seed: bytes) -> str:
    """A current key for seed: wst_ and the seed with its CRC32, in 50 base58 characters."""
    n = int.from_bytes(seed + zlib.crc32(seed).to_bytes(4, "big"), "big")
    out = ""
    for _ in range(50):
        n, d = divmod(n, 58)
        out = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"[d] + out
    return "wst_" + out


def new_app_key() -> str:
    """A current key, which doesn't name its app."""
    return format_key(secrets.token_bytes(32))


class FakeCore:
    """Answers the app API like Wakeshift would, recording each request."""

    def __init__(self, default_ok: bool = False) -> None:
        self.requests: list[tuple[str, str, Any]] = []
        self.routes: dict[tuple[str, str], Callable[[Any], tuple[int, Any]]] = {}
        #: Answer unknown paths with {} (as the Node SDK's tests do), not a 404.
        self.default_ok = default_ok

    def on(self, method: str, path: str, fn: Callable[[Any], tuple[int, Any]]) -> None:
        self.routes[(method, path)] = fn

    def bodies(self, path: str) -> list[Any]:
        return [b for _, p, b in self.requests if p == path]

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api/apps/v1")
        body = json.loads(request.content) if request.content else None
        self.requests.append((request.method, path, body))
        fn = self.routes.get((request.method, path))
        if fn is None:
            if (request.method, path) == ("PUT", "/manifest"):
                return httpx.Response(200, json=body)
            if self.default_ok:
                return httpx.Response(200, json={})
            return httpx.Response(404, json={"error": {"code": "not_found", "message": f"no route {request.method} {path}"}})
        status, out = fn(body)
        return httpx.Response(status, json=out)


def fake_wake(core: FakeCore | None = None, app: str = "sample", **kw: Any) -> tuple[Wakeshift, FakeCore]:
    core = core or FakeCore(default_ok=True)
    http = httpx.AsyncClient(transport=httpx.MockTransport(core.handler))
    return Wakeshift(app_key(app), core="http://core.test", http=http, **kw), core


class Rpc:
    """Raw JSON-RPC to the server; a stateful one keeps the session the
    initialize response named, as an MCP client would."""

    def __init__(self, client: httpx.AsyncClient, path: str, headers: dict[str, str] | None = None) -> None:
        self.client = client
        self.path = path
        self.n = 0
        self.session: str | None = None
        #: Sent with every request (an app's own sign-in, say).
        self.headers = dict(headers or {})

    async def __call__(self, method: str, params: Any = None, *, headers: dict[str, str] | None = None) -> dict[str, Any]:
        """The JSON-RPC response (result or error)."""
        self.n += 1
        h = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": "2025-06-18",
        }
        if self.session:
            h["Mcp-Session-Id"] = self.session
        h.update(self.headers)
        h.update(headers or {})
        msg: dict[str, Any] = {"jsonrpc": "2.0", "id": self.n, "method": method}
        if params is not None:
            msg["params"] = params
        r = await self.client.post(self.path, content=json.dumps(msg), headers=h)
        assert r.status_code in (200, 400, 401), (r.status_code, r.text)
        if method == "initialize" and r.headers.get("mcp-session-id"):
            self.session = r.headers["mcp-session-id"]
            await self.client.post(
                self.path,
                content=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
                headers={**h, "Mcp-Session-Id": self.session},
            )
        return r.json() if r.status_code != 401 else {"status": 401}


@asynccontextmanager
async def served(
    server: Any, *, wrap: Callable[[Any], Any] | None = None, stateless: bool = True
) -> AsyncIterator[tuple[httpx.AsyncClient, Rpc]]:
    """An MCPServer served in-process over Streamable HTTP (JSON responses):
    stateless by default; stateful keeps each client's initialize."""
    app = server.streamable_http_app(stateless_http=stateless, json_response=True)
    asgi = wrap(app) if wrap else app
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=asgi), base_url="http://127.0.0.1:8000") as client:
            yield client, Rpc(client, "/mcp")


async def initialize(
    rpc: Rpc, client_name: str, capabilities: dict[str, Any] | None = None, headers: dict[str, str] | None = None
) -> dict[str, Any]:
    return await rpc(
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": capabilities or {},
            "clientInfo": {"name": client_name, "version": "1"},
        },
        headers=headers,
    )
