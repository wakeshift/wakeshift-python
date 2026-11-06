"""A stdio MCP server with Wakeshift attached, for test_stdio.py:
``python stdio_server.py mcpserver|fastmcp``. It reads WAKESHIFT_KEY and
WAKESHIFT_CORE like any app, and configures no logging."""

from __future__ import annotations

import sys
import time

from pydantic import BaseModel

from wakeshift import ToolCall, Wakeshift


class Job(BaseModel):
    job: str


class Done(BaseModel):
    status: str


def main(kind: str) -> None:
    if kind == "fastmcp":
        from fastmcp import FastMCP

        server = FastMCP("stdio-check")
    else:
        from mcp.server.mcpserver import MCPServer

        server = MCPServer("stdio-check")
    wake = Wakeshift()
    finished = wake.define_event("job.finished", description="A job finished", args=Job, payload=Done)
    attached = wake.attach(server)

    @attached.tool(subscribes=[finished])
    async def start(job: str, call: ToolCall) -> str:
        """Start a job."""
        await call.subscribe(finished, {"job": job})
        return f"Started {job}."

    @attached.tool
    def peek(job: str, call: ToolCall) -> str:
        """A sync tool."""
        return f"{call.tool} {job}"

    @attached.tool(subscribes=[finished])
    async def watch(job: str, call: ToolCall) -> str:
        """Watch a job that never finishes: a watch whose async check keeps waiting."""
        await call.subscribe(finished, {"job": job})

        async def check() -> Done | None:
            return None

        finished.watch({"job": job}, check, every=1, timeout="1h")
        return f"Watching {job}."

    @attached.tool(subscribes=[finished])
    async def watch_sync(job: str, call: ToolCall) -> str:
        """Watch a job whose sync check blocks a worker thread for a while."""
        await call.subscribe(finished, {"job": job})

        def check() -> Done | None:
            time.sleep(20)
            return None

        finished.watch({"job": job}, check, every=1, timeout="1h")
        return f"Watching {job}."

    @server.tool()
    async def finish(job: str) -> str:
        """Finish a job."""
        await finished.emit(Job(job=job), Done(status="ok"))
        return "Emitted."

    if kind == "fastmcp":
        server.run("stdio", show_banner=False)
    else:
        server.run("stdio")


if __name__ == "__main__":
    main(sys.argv[1])
