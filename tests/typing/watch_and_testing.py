"""event.watch, on_subscribe and wakeshift.testing as a vendor writes them,
under pyright --strict and mypy --strict (the typing gate)."""

import asyncio

from pydantic import BaseModel

from wakeshift import Subscribed, ToolCall, Wakeshift, WatchResult
from wakeshift.testing import TestClient, tool_call, with_call


class Job(BaseModel):
    job: str


class Crawl(BaseModel):
    status: str
    pages: int


wake: Wakeshift = TestClient()


async def status(job: str) -> Crawl | None:
    """The vendor's own status check: the payload once the job is done."""
    return Crawl(status="completed", pages=3)


def on_subscribe(s: Subscribed[Job]) -> None:
    job: str = s.args.job
    crawl_finished.watch(s.args, lambda: status(job), every="10s", timeout="1h", is_final=lambda e: isinstance(e, KeyError))


crawl_finished = wake.define_event(
    "crawl.finished", description="A crawl finished", args=Job, payload=Crawl, on_subscribe=on_subscribe
)


async def start_crawl(url: str, call: ToolCall) -> str:
    await call.subscribe(crawl_finished, {"job": "j1"}, once=True)
    return "started"


async def unit_test() -> None:
    call = tool_call(tool="start_crawl", user="u1", events=[crawl_finished])
    assert await start_crawl("https://example.com", call) == "started"
    assert call.subscriptions[0].event == "crawl.finished"
    with with_call(wake, call):
        await wake.current_call().subscribe(crawl_finished, Job(job="j2"))
    task: asyncio.Task[WatchResult] = crawl_finished.watch(Job(job="j3"), lambda: status("j3"), id="job-j3")
    result = await task
    if result.outcome == "emitted" and result.emitted is not None:
        print(result.emitted.matched)
