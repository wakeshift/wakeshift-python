"""A process that only emits needs no MCP SDK: plain wakeshift installs beside
FastMCP 2 or 3 (which sit on mcp 1.x) and emits, mints publish URLs and reads
status (design §5, #63/#64). CI also runs this file in environments with
fastmcp 2.x or 3.x and plain wakeshift, where attach serves on FastMCP's
own mcp 1.x."""

from __future__ import annotations

import subprocess
import sys
from importlib.metadata import version

import pytest
from pydantic import BaseModel

from wakeshift import Wakeshift

from .helpers import FakeCore, fake_wake


class Job(BaseModel):
    job: str


class Crawl(BaseModel):
    status: str
    pages: int


def test_importing_wakeshift_imports_no_mcp_sdk() -> None:
    code = "import sys, wakeshift; print(sorted(m for m in sys.modules if m == 'mcp' or m.startswith(('mcp.', 'fastmcp'))))"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout.strip()
    assert out == "[]", out


async def test_emit_publish_urls_and_status_without_attach() -> None:
    core = FakeCore(default_ok=True)
    core.on("POST", "/events/emit", lambda b: (200, {"matched": 2}))
    core.on("POST", "/events/publish-urls", lambda b: (200, {"id": "p1", "url": "https://app.wakeshift.ai/p/wsp_token123456"}))
    core.on("POST", "/events/status", lambda b: (200, {"subscriptions": 3, "connected": 1}))
    wake, _ = fake_wake(core)
    crawl = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    assert (await crawl.emit(Job(job="j1"), Crawl(status="completed", pages=3))).matched == 2
    url = await crawl.create_publish_url({"job": "j1"})
    assert url.id == "p1" and "wsp_token123456" not in repr(url)
    assert (await crawl.status({"job": "j1"})).subscriptions == 3


def test_attach_on_fastmcp_2_or_3_serves_without_the_mcp_extra() -> None:
    """Beside FastMCP 2 or 3, attach serves through FastMCP's own mcp 1.x
    (test_fastmcp23.py), with nothing more installed."""
    try:
        installed = version("fastmcp")
    except Exception:  # noqa: BLE001
        pytest.skip("no fastmcp here")
    if not installed.startswith(("2.", "3.")):
        pytest.skip("this environment has FastMCP 4; CI runs this beside FastMCP 2 and 3")
    from fastmcp import FastMCP

    from wakeshift import Attachment

    wake = Wakeshift("", core="https://core.test")
    assert isinstance(wake.attach(FastMCP("crawler")), Attachment)
    assert version("mcp").startswith("1.")
