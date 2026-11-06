"""Mistakes the typing gate must catch: each line marked type: ignore is an
error under pyright --strict and mypy --strict, and both report an ignore
that's no longer needed, so a mistake that stops being caught fails the gate."""

from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import BuiltinTools, Subscription, ToolCall, Wakeshift

wake = Wakeshift()


class Job(BaseModel):
    job: str


class Crawl(BaseModel):
    status: str
    pages: int


crawl_finished = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
attached = wake.attach(MCPServer("crawler"))


async def misuse(call: ToolCall) -> None:
    # A payload of the wrong model.
    await crawl_finished.emit(Job(job="j1"), Job(job="j1"))  # type: ignore
    # Arguments of the wrong model.
    await call.subscribe(crawl_finished, Crawl(status="ok", pages=1))  # type: ignore
    # text reads a field the payload doesn't have.
    wake.define_event("crawl.done", description="d", args=Job, payload=Crawl, text=lambda p, a: p.missing)  # type: ignore
    # personal is a bool.
    wake.define_event("crawl.mine", description="d", args=Job, payload=Crawl, personal="yes")  # type: ignore
    # replay is a duration, not a list.
    await call.subscribe(crawl_finished, {"job": "j1"}, replay=[60])  # type: ignore
    # timeout is seconds, not a string.
    Wakeshift(timeout="20")  # type: ignore
    # builtin_tools takes only its four keys.
    choice: BuiltinTools = {"subcribe": False}  # type: ignore
    # A subscription's state is one of five.
    sub: Subscription = await call.subscribe(crawl_finished, {"job": "j1"})
    sub.state = "done"  # type: ignore
    print(choice)
