"""Stdio servers: the SDK never writes to stdout, where a stdio server's
JSON-RPC goes. A stdio server with Wakeshift attached runs as a subprocess,
configuring no logging, without a key, with a key and a core that answers,
and with a key and a core that's down (so the SDK logs failures); an MCP
client drives it over stdio (initialize, tools/list, tool calls), and every
line of its stdout must be a JSON-RPC message."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from .helpers import new_app_key

SERVER = Path(__file__).parent / "stdio_server.py"


class _Core(BaseHTTPRequestHandler):
    """A core that answers what the stdio server asks."""

    def _answer(self) -> None:
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n)) if n else {}
        path = self.path.removeprefix("/api/apps/v1")
        out: Any = {}
        if path in ("/manifest", "/manifest/merge"):
            out = body
        elif path == "/app":
            out = {"id": "app_1", "name": "Stdio check"}
        elif path == "/event-subscriptions":
            out = {"event_subscription": {"id": "es_1", "user": None, "created": True}, "session": "wsc_s1"}
        elif path == "/calls/addition":
            out = {"text": "Wakeshift: woken."}
        elif path == "/events/emit":
            out = {"matched": 1}
        data = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_PUT = _answer

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass


@pytest.fixture
def core() -> Iterator[str]:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Core)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()
        httpd.server_close()


def _drive(kind: str, env: dict[str, str]) -> tuple[list[str], dict[int, Any]]:
    """Runs the stdio server and drives it as an MCP client would: its stdout
    lines, and the responses by id."""
    # stderr, where the SDK's lines go, isn't read: it only must not block.
    proc = subprocess.Popen(
        [sys.executable, str(SERVER), kind],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=env,
        text=True,
    )
    assert proc.stdin is not None and proc.stdout is not None
    lines: list[str] = []
    responses: dict[int, Any] = {}
    # Without a key, tools are listed as written: no Wakeshift arguments.
    wake_args = {"session_key": "wsc_x"} if env.get("WAKESHIFT_KEY") else {}

    def send(msg: dict[str, Any]) -> None:
        assert proc.stdin is not None
        proc.stdin.write(json.dumps({"jsonrpc": "2.0", **msg}) + "\n")
        proc.stdin.flush()

    def response(id: int) -> Any:  # noqa: A002
        assert proc.stdout is not None
        while id not in responses:
            line = proc.stdout.readline()
            assert line, "the server closed stdout"
            lines.append(line)
            msg = json.loads(line)
            if "id" in msg and ("result" in msg or "error" in msg):
                responses[msg["id"]] = msg
        return responses[id]

    try:
        send(
            {
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "claude-code", "version": "2"},
                },
            }
        )
        response(1)
        send({"method": "notifications/initialized"})
        send({"id": 2, "method": "tools/list"})
        response(2)
        send({"id": 3, "method": "tools/call", "params": {"name": "start", "arguments": {"job": "j1", **wake_args}}})
        response(3)
        send({"id": 4, "method": "tools/call", "params": {"name": "peek", "arguments": {"job": "j1"}}})
        response(4)
        send({"id": 5, "method": "tools/call", "params": {"name": "finish", "arguments": {"job": "j1"}}})
        response(5)
        proc.stdin.close()
        lines += proc.stdout.read().splitlines(keepends=True)
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    return lines, responses


def _env(**kw: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("WAKESHIFT_")}
    env.update(kw)
    return env


def _kinds() -> list[str]:
    try:
        import fastmcp  # noqa: F401
    except ImportError:
        return ["mcpserver"]
    return ["mcpserver", "fastmcp"]


def _only_json_rpc(lines: list[str]) -> None:
    for line in lines:
        msg = json.loads(line)  # a line that isn't JSON fails here
        assert isinstance(msg, dict) and msg.get("jsonrpc") == "2.0", line[:200]


def _text(r: Any) -> str:
    assert "result" in r, r
    return "\n".join(c.get("text", "") for c in r["result"]["content"])


@pytest.mark.parametrize("kind", _kinds())
def test_without_a_key_stdout_is_only_json_rpc(kind: str) -> None:
    lines, r = _drive(kind, _env())
    _only_json_rpc(lines)
    names = {t["name"] for t in r[2]["result"]["tools"]}
    assert {"start", "peek", "finish"} <= names
    assert _text(r[3]).startswith("Started j1.") and _text(r[4]) == "peek j1" and _text(r[5]) == "Emitted."


@pytest.mark.parametrize("kind", _kinds())
def test_with_a_key_stdout_is_only_json_rpc(kind: str, core: str) -> None:
    lines, r = _drive(kind, _env(WAKESHIFT_KEY=new_app_key(), WAKESHIFT_CORE=core))
    _only_json_rpc(lines)
    assert "Wakeshift: woken." in _text(r[3]) and _text(r[4]).startswith("peek j1") and _text(r[5]) == "Emitted."


@pytest.mark.parametrize("kind", _kinds())
def test_with_a_key_and_the_core_down_stdout_is_only_json_rpc(kind: str) -> None:
    # Port 9 (discard) refuses: every call to Wakeshift fails, and the SDK logs.
    lines, r = _drive(kind, _env(WAKESHIFT_KEY=new_app_key(), WAKESHIFT_CORE="http://127.0.0.1:9"))
    _only_json_rpc(lines)
    assert set(r) == {1, 2, 3, 4, 5}


@pytest.mark.parametrize("tool", ["watch", "watch_sync"])
@pytest.mark.parametrize("kind", _kinds())
def test_a_running_watch_never_keeps_a_stdio_server_alive(kind: str, tool: str, core: str) -> None:
    """stdin ends while a watch runs (an async check that keeps waiting, or a
    sync check blocking its thread): the server exits at once, as without it."""
    import time

    env = _env(WAKESHIFT_KEY=new_app_key(), WAKESHIFT_CORE=core)
    proc = subprocess.Popen(
        [sys.executable, str(SERVER), kind],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=env,
        text=True,
    )
    assert proc.stdin is not None and proc.stdout is not None
    try:

        def send(msg: dict[str, Any]) -> None:
            assert proc.stdin is not None
            proc.stdin.write(json.dumps({"jsonrpc": "2.0", **msg}) + "\n")
            proc.stdin.flush()

        def response(id: int) -> Any:  # noqa: A002
            assert proc.stdout is not None
            while True:
                line = proc.stdout.readline()
                assert line, "the server closed stdout"
                msg = json.loads(line)
                if msg.get("id") == id:
                    return msg

        init = {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "claude-code", "version": "2"}}
        send({"id": 1, "method": "initialize", "params": init})
        response(1)
        send({"method": "notifications/initialized"})
        send({"id": 2, "method": "tools/call", "params": {"name": tool, "arguments": {"job": "j1", "session_key": "wsc_x"}}})
        assert _text(response(2)).startswith("Watching j1.")
        time.sleep(0.3)  # the watch's first check is running
        started = time.monotonic()
        proc.stdin.close()
        proc.wait(timeout=10)
        assert time.monotonic() - started < 5
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
