"""MCP Tasks as a wake path (tasks.py): the wait tool, tasks/get and
tasks/cancel for a client on protocol 2026-07-28 that takes tasks, and the
wire format every SDK shares (mcp-tasks-wire.json)."""

from __future__ import annotations

import contextlib
from typing import Any

from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import Identity, ToolCall
from wakeshift.client import _takes_tasks
from wakeshift.tasks import TASK_HOLD_SECONDS, TASKS_EXTENSION, WAIT_TOOL, WAIT_TOOL_DEFINITION

from .helpers import FakeCore, fake_wake, golden_file, served

G = golden_file("mcp-tasks-wire.json")


def modern(method: str, params: Any = None) -> dict[str, str]:
    """The headers a 2026-07-28 request carries, as Claude Code 2.1.314 sends
    them (docs/harnesses.md): its version, method, and Mcp-Name with a tool
    call's name or a task request's ID."""
    h = {"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": method}
    if method == "tools/call":
        h["Mcp-Name"] = params["name"]
    elif method in ("tasks/get", "tasks/cancel") and isinstance(params, dict) and "taskId" in params:
        h["Mcp-Name"] = params["taskId"]
    return h


class _Chat(BaseModel):
    channel: str


class _Msg(BaseModel):
    text: str


def _app(core: FakeCore) -> MCPServer:
    """The golden file's app: one event, identify naming u1, and a wrapped
    tool, join, that subscribes nothing and returns "joined"."""
    wake, _ = fake_wake(core)
    wake.define_event("chat.message", description="A new message", args=_Chat, payload=_Msg, text=lambda m: m.text)
    server = MCPServer("chat")
    attached = wake.attach(server, identify=lambda _ctx: Identity(user="u1"))

    @attached.tool()
    async def join(call: ToolCall) -> str:
        """Join."""
        return "joined"

    return server


def _without_meta(r: Any) -> Any:
    """A result without the _meta the MCP SDK stamps on 2026-07-28 results (its serverInfo)."""
    if isinstance(r, dict):
        return {k: v for k, v in r.items() if k != "_meta"}
    return r


async def test_wire_format_matches_golden() -> None:
    assert G["extension"] == TASKS_EXTENSION
    assert G["hold_seconds"] == TASK_HOLD_SECONDS
    assert G["wait_tool"] == WAIT_TOOL_DEFINITION

    answers: dict[str, Any] = {}
    core = FakeCore(default_ok=True)
    for path in ("/calls/task", "/calls/task/get", "/calls/task/cancel", "/calls/addition"):
        core.on("POST", path, lambda _b, p=path: (200, answers.get(p, {})))
    server = _app(core)
    async with served(server) as (_, rpc):
        d = await rpc("server/discover", {"_meta": G["lists"][0]["meta"]}, headers=modern("server/discover"))
        assert "result" in d, d
        assert d["result"]["capabilities"]["extensions"][TASKS_EXTENSION] == {}, d
        for entry in G["lists"]:
            h = modern("tools/list") if entry["meta"].get("io.modelcontextprotocol/protocolVersion") == "2026-07-28" else {}
            res = await rpc("tools/list", {"_meta": entry["meta"]}, headers=h)
            tools = res["result"]["tools"]
            names = [t["name"] for t in tools]
            assert (WAIT_TOOL in names) == entry["wait_tool"], (entry["name"], names)
            if entry["wait_tool"]:
                assert next(t for t in tools if t["name"] == WAIT_TOOL) == G["wait_tool"]
        for c in G["cases"]:
            core.requests.clear()
            answers.clear()
            answers[c["core_path"]] = c["core_response"]
            res = await rpc(c["method"], c["params"], headers=modern(c["method"], c["params"]))
            sent = core.bodies(c["core_path"])
            assert sent, (c["name"], core.requests, res)
            body = dict(sent[0])
            body["call"] = {k: v for k, v in body["call"].items() if k != "user_agent"}
            assert body == c["core_request"], c["name"]
            if "error" in c:
                assert res["error"]["code"] == c["error"]["code"], (c["name"], res)
                assert c["error"]["message"] in res["error"]["message"], (c["name"], res)
            elif "result" in c:
                assert _without_meta(res.get("result")) == c["result"], (c["name"], res)


def test_takes_tasks() -> None:
    def meta(v: str, caps: Any) -> dict[str, Any]:
        return {"io.modelcontextprotocol/protocolVersion": v, "io.modelcontextprotocol/clientCapabilities": caps}

    assert _takes_tasks(meta("2026-07-28", {"extensions": {TASKS_EXTENSION: {}}}))
    assert not _takes_tasks(meta("2026-07-28", {"extensions": {}}))
    assert not _takes_tasks(meta("2026-07-28", {"extensions": [TASKS_EXTENSION]}))
    assert not _takes_tasks(meta("2025-11-25", {"extensions": {TASKS_EXTENSION: {}}}))
    assert not _takes_tasks(None)
    caps_only = {"io.modelcontextprotocol/clientCapabilities": {"extensions": {TASKS_EXTENSION: {}}}}
    assert _takes_tasks(caps_only, {"mcp-protocol-version": "2026-07-28"})


async def test_core_failure_is_an_rpc_error() -> None:
    core = FakeCore(default_ok=True)
    core.on("POST", "/calls/task/get", lambda _b: (503, {"error": {"code": "unavailable", "message": "down"}}))
    server = _app(core)
    async with served(server) as (_, rpc):
        res = await rpc("tasks/get", {"taskId": "wkt1_x", "_meta": G["lists"][0]["meta"]}, headers=modern("tasks/get"))
    assert res["error"]["code"] == -32603, res


async def test_other_task_ids_are_not_wakeshifts() -> None:
    """tasks/get and tasks/cancel for an ID that isn't Wakeshift's go on to the MCP SDK, never to Wakeshift."""
    core = FakeCore(default_ok=True)
    server = _app(core)
    async with served(server) as (_, rpc):
        for method in ("tasks/get", "tasks/cancel"):
            # This app has no tasks of its own: the MCP SDK answers "Method not found" (a 404).
            with contextlib.suppress(AssertionError):
                await rpc(method, {"taskId": "app-own-task", "_meta": G["lists"][0]["meta"]}, headers=modern(method))
    assert not any("/calls/task" in path for _, path, _ in core.requests), core.requests
