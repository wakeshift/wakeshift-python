"""The quickstart (design, "The end state"): a crawler that wakes the
conversation that started a crawl. tests/test_warnings.py runs it, and the
typing gate checks it under pyright --strict and mypy --strict."""

from uuid import uuid4

from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import ToolCall, Wakeshift

wake = Wakeshift()  # reads WAKESHIFT_KEY; without one, Wakeshift is off


class Job(BaseModel):
    job: str


class Crawl(BaseModel):
    status: str
    pages: int


crawl_finished = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)

server = MCPServer("crawler")
attached = wake.attach(server)


def start(url: str, job: str) -> None:
    """Starts the crawl (your own code)."""


@attached.tool(subscribes=[crawl_finished])
async def start_crawl(url: str, call: ToolCall) -> str:
    """Crawl a site."""
    job = str(uuid4())  # your tool minted the ID, so only this conversation can know it
    await call.subscribe(crawl_finished, {"job": job}, once=True)
    start(url, job)
    return f"Crawl {job} started."


app = server.streamable_http_app()  # run with uvicorn


# When the crawl finishes, in your worker or webhook handler:
async def on_crawl_finished(job: str, pages: int) -> None:
    await crawl_finished.emit(Job(job=job), Crawl(status="completed", pages=pages))
