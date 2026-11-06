"""subscribing-tools-list.json's plumbing, shared by test_tools_list.py and
test_fastmcp23.py (which runs beside FastMCP 2/3, on mcp 1.x, so nothing
here imports the MCP SDK): the events and vendor tools each scenario
names, registered through the attachment or with the host SDK (wrap names
them), and the shape each listed tool is compared in."""

from __future__ import annotations

from typing import Annotated, Any

import httpx2
from pydantic import BaseModel, Field

from wakeshift import ToolCall, Wakeshift
from wakeshift._toolcall import BUILTIN_NAMES

from .helpers import FakeCore, app_key, golden_file, input_shape, served

FIXTURE = golden_file("subscribing-tools-list.json")


class StartOut(BaseModel):
    job: str = Field(description="The crawl's job ID")


URL = Annotated[str, Field(description="The site's URL")]
REPO = Annotated[str, Field(description="The repository, owner/name")]
JOB = Annotated[str, Field(description="The crawl's job ID")]
PAGE = Annotated[str, Field(description="The page's URL")]


def shape(t: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"name": t["name"], "description": t.get("description"), "inputSchema": input_shape(t["inputSchema"])}
    if t.get("title") and t["name"] in BUILTIN_NAMES:  # FastMCP titles the vendor's tools itself
        out["title"] = t["title"]
    if t.get("outputSchema") is not None:
        out["outputSchema"] = input_shape(t["outputSchema"])
    if t.get("annotations"):
        out["annotations"] = t["annotations"]
    return out


def client(enabled: bool) -> Wakeshift:
    core = FakeCore(default_ok=True)
    core.on("GET", "/app", lambda b: (200, {"id": "crawler", "name": FIXTURE["app"], "key": {"key_id": "k", "name": "t"}}))
    return Wakeshift(
        app_key("crawler") if enabled else "",
        core="https://core.test",
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(core.handler)),
    )


def define(wake: Wakeshift, names: list[str]) -> dict[str, Any]:
    events = {}
    for name in names:
        e = FIXTURE["events"][name]
        kw: dict[str, Any] = (
            {"authorize": lambda s, a: True}
            if e["access"] == "authorize"
            else {e["access"]: True}
            if e["access"] != "tools"
            else {}
        )
        events[name] = wake.define_event(name, description=e["description"], args=e["args"], payload=e["payload"], **kw)
    return events


def register(server: Any, attached: Any, events: dict[str, Any], tools: list[str], *, via_wrap: bool) -> None:
    """The fixture's vendor tools: through the attachment, or with the host SDK (wrap names them)."""
    vt = FIXTURE["vendor_tools"]

    def reg(name: str, fn: Any, structured: bool) -> None:
        subs = vt[name]["subscribes"]
        opts: dict[str, Any] = {} if structured else {"structured_output": False}
        if subs is None or via_wrap:
            server.tool(name=name, description=vt[name]["description"], **opts)(fn)
        else:
            attached.tool(name, vt[name]["description"], subscribes=[events[e] for e in subs], **opts)(fn)

    if via_wrap:

        async def start_crawl(url: URL) -> StartOut:
            return StartOut(job="j1")

        async def watch_builds(repo: REPO) -> str:
            return "watching"

        async def check_crawl(job: JOB) -> str:
            return "running"

    else:

        async def start_crawl(url: URL, call: ToolCall) -> StartOut:  # type: ignore[misc]
            return StartOut(job="j1")

        async def watch_builds(repo: REPO, call: ToolCall) -> str:  # type: ignore[misc]
            return "watching"

        async def check_crawl(job: JOB, call: ToolCall) -> str:  # type: ignore[misc]
            return "running"

    async def get_page(url: PAGE) -> str:
        return "<html>"

    fns = {
        "start_crawl": (start_crawl, True),
        "watch_builds": (watch_builds, False),
        "check_crawl": (check_crawl, False),
        "get_page": (get_page, False),
    }
    for name in tools:
        reg(name, *fns[name])


def wrap_of(events: dict[str, Any], tools: list[str]) -> dict[str, list[Any]]:
    vt = FIXTURE["vendor_tools"]
    return {n: [events[e] for e in vt[n]["subscribes"]] for n in tools if vt[n]["subscribes"] is not None}


async def listed(server: Any) -> list[dict[str, Any]]:
    async with served(server) as (_, rpc):
        res = await rpc("tools/list", {})
    return res["result"]["tools"]


def check(got: list[dict[str, Any]], want: list[dict[str, Any]], name: str) -> None:
    g = {t["name"]: shape(t) for t in got}
    w = {t["name"]: t for t in want}
    assert sorted(g) == sorted(w), (name, sorted(g))
    for n in w:
        assert g[n] == {
            k: w[n][k] for k in ("name", "title", "description", "inputSchema", "outputSchema", "annotations") if k in w[n]
        }, (
            name,
            n,
            g[n],
        )
