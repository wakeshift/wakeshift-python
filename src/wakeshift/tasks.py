"""MCP Tasks as a wake path (docs/harnesses.md, "Claude Code: MCP tasks as a
wake path"): for a client that takes MCP tasks on protocol 2026-07-28, attach
serves Wakeshift's wait tool, wakeshift_wait. Its call is answered with a task
that completes when the conversation's next Wakeshift messages arrive; the
client follows it in the background (tasks/get), and a completed task wakes
an idle conversation. Wakeshift decides everything: the SDK passes the tool's
call, tasks/get and tasks/cancel to it and answers with the MCP result it
returns, so every SDK says the same (mcp-tasks-wire.json). A task is only its
ID, which any process answers for, so nothing is kept here.

Only a request that declares the tasks extension in its own capabilities
sees the wait tool, and a wrapped tool's call from it tells Wakeshift so
(call.tasks), which then gives the wait tool's instructions instead of its
harness's zero-install path (Claude Code's Monitor). Everything else is
unchanged. The SDK's middleware answers these itself, before the MCP SDK's
own dispatch: tasks/get and tasks/cancel aren't methods it serves, and the
MCP SDK's own tasks support (an older revision's) is never advertised.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from .client import (
    CONVERSATION_DESCRIPTION,
    HARNESS_DESCRIPTION,
    SESSION_DESCRIPTION,
    TASKS_EXTENSION,
    _call,
    _request_info,
    _takes_tasks,
)
from .errors import WakeshiftError

if TYPE_CHECKING:
    from .client import Wakeshift

__all__ = ["TASKS_EXTENSION", "TASK_HOLD_SECONDS", "WAIT_TOOL", "WAIT_TOOL_DEFINITION", "WAIT_TOOL_DESCRIPTION"]

#: The wait tool's name.
WAIT_TOOL = "wakeshift_wait"

#: The wait tool's description: the core's harnesses.WaitToolDescription.
WAIT_TOOL_DESCRIPTION = (
    "Wait for this conversation's next Wakeshift messages from this app. Returns at once and keeps waiting in the "
    "background: when messages arrive, its result reaches you as a task notification. Call it again after each one, "
    "with the session_key Wakeshift gave you."
)

#: What every Wakeshift task ID starts with: tasks/get and tasks/cancel for
#: any other ID aren't Wakeshift's.
TASK_ID_PREFIX = "wkt1_"

#: How long Wakeshift holds a tasks/get for a working task, in seconds.
TASK_HOLD_SECONDS = 20

#: The wait tool as tools/list lists it.
WAIT_TOOL_DEFINITION: dict[str, Any] = {
    "name": WAIT_TOOL,
    "description": WAIT_TOOL_DESCRIPTION,
    "inputSchema": {
        "type": "object",
        "properties": {
            "session_key": {"type": "string", "description": SESSION_DESCRIPTION},
            "harness": {"type": "string", "description": HARNESS_DESCRIPTION},
            "conversation_id": {"type": "string", "description": CONVERSATION_DESCRIPTION},
        },
        "additionalProperties": False,
    },
}


def takes_tasks(ctx: Any) -> bool:
    """Whether the request is protocol 2026-07-28 and declares the tasks extension."""
    _, meta, headers = _request_info(ctx)
    return _takes_tasks(meta, headers)


def _detail(err: Exception) -> str:
    return err.detail if isinstance(err, WakeshiftError) else str(err)


class TaskServer:
    """Answers the wait tool's call, tasks/get and tasks/cancel (from attach's
    middleware), and adds the wait tool and the extension to tools/list and
    server/discover."""

    def __init__(self, wake: Wakeshift, caller_of: Callable[[Any], tuple[str | None, str | None]]) -> None:
        self._wake = wake
        self._caller_of = caller_of

    def _wire(self, ctx: Any, args: Mapping[str, Any]) -> dict[str, Any]:
        user, client = self._caller_of(ctx)
        return _call(ctx, user=user, client=client, args=args, tool=WAIT_TOOL).to_json()

    def handles(self, ctx: Any) -> bool:
        method = getattr(ctx, "method", "")
        params = getattr(ctx, "params", None)
        if method in ("tasks/get", "tasks/cancel"):
            # Only Wakeshift's own tasks: an app's own (the MCP SDK's task
            # support) go on to the MCP SDK.
            task = params.get("taskId") if isinstance(params, Mapping) else None
            return isinstance(task, str) and task.startswith(TASK_ID_PREFIX)
        return method == "tools/call" and isinstance(params, Mapping) and params.get("name") == WAIT_TOOL

    async def answer(self, ctx: Any) -> dict[str, Any]:
        from mcp.shared.exceptions import MCPError

        method = getattr(ctx, "method", "")
        params = getattr(ctx, "params", None)
        params = params if isinstance(params, Mapping) else {}
        if method == "tools/call":
            args = params.get("arguments")
            try:
                out = await self._wake._request_app(
                    "POST", "/calls/task", {"call": self._wire(ctx, args if isinstance(args, Mapping) else {})}
                )
            except Exception as err:
                return _tool_error(f"Wakeshift couldn't start the wait ({_detail(err)}). Call {WAIT_TOOL} again.")
            result = out.get("result")
            if isinstance(result, dict):
                return result
            return _tool_error(f"Wakeshift didn't answer the wait. Call {WAIT_TOOL} again.")
        task = params.get("taskId")
        body: dict[str, Any] = {"task": task if isinstance(task, str) else "", "call": self._wire(ctx, {})}
        path = "/calls/task/cancel"
        if method == "tasks/get":
            body["hold_seconds"] = TASK_HOLD_SECONDS
            path = "/calls/task/get"
        try:
            out = await self._wake._request_app("POST", path, body, timeout=TASK_HOLD_SECONDS + 15)
        except Exception as err:
            raise MCPError(-32603, f"Wakeshift: {_detail(err)}") from err
        rpc = out.get("rpc_error")
        if isinstance(rpc, Mapping):
            raise MCPError(int(rpc.get("code", -32603)), str(rpc.get("message", "")))
        result = out.get("result")
        if not isinstance(result, dict):
            raise MCPError(-32603, "Wakeshift didn't answer for this task")
        return result

    @staticmethod
    def amend(ctx: Any, method: str, result: Any) -> None:
        """server/discover gets the tasks extension; tools/list, for a request
        that takes tasks, the wait tool."""
        if not isinstance(result, dict):
            return
        if method == "server/discover":
            caps = result.setdefault("capabilities", {})
            if isinstance(caps, dict):
                caps.setdefault("extensions", {})[TASKS_EXTENSION] = {}
        elif method == "tools/list" and isinstance(result.get("tools"), list) and takes_tasks(ctx):
            if not any(isinstance(t, Mapping) and t.get("name") == WAIT_TOOL for t in result["tools"]):
                result["tools"] = [*result["tools"], dict(WAIT_TOOL_DEFINITION)]


def _tool_error(text: str) -> dict[str, Any]:
    return {"resultType": "complete", "content": [{"type": "text", "text": text}], "isError": True}
