"""attach on FastMCP 2.x (2.12+) and 3.x, which sit on mcp 1.x
(_fastmcp23.py). Runs only beside FastMCP 2 or 3 (CI installs each with
plain wakeshift); beside FastMCP 4 it's skipped.

The security property is FastMCP 4's: Wakeshift's three arguments are gone
before any vendor middleware (added before attach or after, anywhere in the
list), any log line (FastMCP's own, at debug) or any handler sees them.
Native MCP Events and protocol 2026-07-28 aren't served: every client gets
the built-in tools and the block."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from importlib.metadata import version
from typing import Any

import httpx2
import pytest

fastmcp = pytest.importorskip("fastmcp")
if not version("fastmcp").startswith(("2.", "3.")):
    pytest.skip("FastMCP 2/3 only: CI runs this beside each", allow_module_level=True)

from fastmcp import Client, Context, FastMCP  # noqa: E402
from fastmcp.server.middleware import Middleware  # noqa: E402
from pydantic import BaseModel  # noqa: E402

from wakeshift import Identity, ToolCall, UsageError, Wakeshift  # noqa: E402
from wakeshift._fastmcp23 import FastMCP23Host  # noqa: E402
from wakeshift._hosts import host_of  # noqa: E402

from .helpers import FakeCore, Logged, Rpc, fake_wake  # noqa: E402
from .tools_list_fixture import FIXTURE, check, client, define, register, wrap_of  # noqa: E402

FM3 = version("fastmcp").startswith("3.")
SECRET = "wsc_canary_secret_0123456789"
WAKE = {"session_key": SECRET, "harness": "claude-code", "conversation_id": "conv-1"}
WOKEN = "Wakeshift: this conversation is woken when it happens."


class Job(BaseModel):
    job: str


class Done(BaseModel):
    status: str


class Out(BaseModel):
    job: str
    queued: bool


class Server(FastMCP):
    """A FastMCP that answers the calls the shared fixtures make of an
    MCPServer: tool(structured_output=…) and streamable_http_app()."""

    def tool(self, name_or_fn: Any = None, **kw: Any) -> Any:  # type: ignore[override]
        if kw.pop("structured_output", None) is False:
            kw.setdefault("output_schema", None)
        return super().tool(name_or_fn, **kw)

    def streamable_http_app(self, *, stateless_http: bool = True, json_response: bool = True) -> Any:
        return self.http_app(path="/mcp", stateless_http=stateless_http, json_response=json_response)


@asynccontextmanager
async def served(server: FastMCP, headers: dict[str, str] | None = None) -> AsyncIterator[Rpc]:
    """The server over stateless Streamable HTTP (JSON responses), as a client
    on protocol 2025-06-18 sees it."""
    app = server.http_app(path="/mcp", stateless_http=True, json_response=True)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000") as c:
            yield Rpc(c, "/mcp", headers)


def wake_core() -> tuple[Wakeshift, FakeCore, Any]:
    core = FakeCore(default_ok=True)
    core.on(
        "POST",
        "/event-subscriptions",
        lambda b: (200, {"event_subscription": {"id": "es_1", "user": "u1", "created": True}, "session": "wsc_s1"}),
    )
    core.on("POST", "/calls/addition", lambda b: (200, {"text": WOKEN}))
    wake, _ = fake_wake(core, app="crawl")
    done = wake.define_event("crawl.done", description="A crawl finished", args=Job, payload=Done, public=True)
    return wake, core, done


def texts(res: dict[str, Any]) -> list[str]:
    return [c.get("text", "") for c in res["result"]["content"]]


class Recorder(Middleware):
    """A vendor's logging middleware: everything it could log, as text."""

    def __init__(self, seen: list[str]) -> None:
        self.seen = seen

    async def on_message(self, context: Any, call_next: Any) -> Any:
        self.seen.append(repr(context.message))
        return await call_next(context)

    async def on_call_tool(self, context: Any, call_next: Any) -> Any:
        self.seen.append(json.dumps(dict(context.message.arguments or {}), default=str))
        return await call_next(context)


@pytest.fixture
def every_log_line() -> Iterator[list[str]]:
    """Every log line from every logger FastMCP and the MCP SDK write to, at debug."""
    lines: list[str] = []

    class Grab(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            lines.append(f"{record.name}: {record.getMessage()} {record.args!r}")

    grab = Grab(logging.DEBUG)
    saved = []
    for name in ("", "fastmcp", "mcp", "FastMCP", "uvicorn", "httpx"):
        lg = logging.getLogger(name)
        saved.append((lg, lg.level))
        lg.addHandler(grab)
        lg.setLevel(logging.DEBUG)
    yield lines
    for lg, level in saved:
        lg.removeHandler(grab)
        lg.setLevel(level)


# ── The security property ─────────────────────────────────────────────────


@pytest.mark.parametrize("strict", [False, True] if FM3 else [False], ids=lambda s: f"strict={s}")
async def test_no_middleware_log_or_handler_ever_sees_the_session_key(strict: bool, every_log_line: list[str]) -> None:
    """A logging middleware added after attach (and one before, and one put
    first in the list after attach) never sees session_key: not in a
    wrapped tool's call, an @attached.tool's, a plain tool's, or a built-in
    tool's. Nor does any log line at debug, or any tool."""
    wake, core, done = wake_core()
    before: list[str] = []
    after: list[str] = []
    first: list[str] = []
    kw: dict[str, Any] = {"strict_input_validation": True} if strict else {}
    mcp = FastMCP("crawler", middleware=[Recorder(before)], **kw)
    got: list[dict[str, Any]] = []

    @mcp.tool
    async def start_crawl(url: str) -> Out:
        """Start a crawl."""
        got.append({"url": url})
        await wake.current_call().subscribe(done, {"job": "j1"})
        return Out(job="j1", queued=True)

    @mcp.tool
    def ping(text: str) -> str:
        """Echo."""
        got.append({"text": text})
        return f"pong {text}"

    attached = wake.attach(mcp, identify=lambda r: Identity(user="u1"), wrap={"start_crawl": [done]})
    mcp.add_middleware(Recorder(after))
    mcp.middleware.insert(0, Recorder(first))

    @attached.tool(subscribes=[done])
    async def watch(job: str, call: ToolCall) -> str:
        """Watch a job."""
        got.append({"job": job})
        await call.subscribe(done, {"job": job})
        return f"watching {job}"

    async with served(mcp) as rpc:
        assert texts(await rpc("tools/call", {"name": "start_crawl", "arguments": {"url": "https://x", **WAKE}}))[-1] == WOKEN
        assert texts(await rpc("tools/call", {"name": "watch", "arguments": {"job": "j9", **WAKE}})) == [
            f"watching j9\n\n{WOKEN}"
        ]
        assert texts(await rpc("tools/call", {"name": "ping", "arguments": {"text": "hi", **WAKE}})) == ["pong hi"]
        sub = await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "crawl.done", "arguments": {"job": "j2"}, **WAKE}}
        )
        assert not sub["result"].get("isError"), sub
        unsub = await rpc(
            "tools/call", {"name": "unsubscribe_events", "arguments": {"event": "crawl.done", "arguments": {"job": "j2"}, **WAKE}}
        )
        assert not unsub["result"].get("isError"), unsub
    for seen in (before, after, first):
        assert seen and not any(SECRET in s or "session_key" in s for s in seen), seen
    assert got == [{"url": "https://x"}, {"job": "j9"}, {"text": "hi"}]
    assert every_log_line and not [ln for ln in every_log_line if SECRET in ln]
    # Wakeshift had it: each call that subscribes names it to the core.
    subs = core.bodies("/event-subscriptions")
    assert [(b["call"]["tool"], b["call"]["session"]) for b in subs] == [
        ("start_crawl", SECRET),
        ("watch", SECRET),
        ("subscribe_events", SECRET),
    ]


async def test_a_tools_own_session_key_is_its_own() -> None:
    """A tool whose own schema names session_key keeps it, as on every host."""
    wake, _, _ = wake_core()
    mcp = FastMCP("a")

    @mcp.tool
    def legacy(session_key: str) -> str:
        return f"own {session_key[:3]}"

    wake.attach(mcp)
    async with served(mcp) as rpc:
        assert texts(await rpc("tools/call", {"name": "legacy", "arguments": {"session_key": "abc123"}})) == ["own abc"]


# ── What attach serves ────────────────────────────────────────────────────


async def test_attach_picks_the_fastmcp_2_3_host() -> None:
    assert isinstance(host_of(FastMCP("a")), FastMCP23Host)


async def test_wrap_and_current_call() -> None:
    """A tool registered with @mcp.tool and named in wrap finds its call with
    wake.current_call(), or with its Context; its result gets the block, in
    structuredContent too."""
    wake, core, done = wake_core()
    mcp = FastMCP("crawler")

    @mcp.tool
    async def start_crawl(url: str, ctx: Context) -> Out:
        """Start a crawl."""
        assert wake.current_call(ctx) is wake.current_call()
        await wake.current_call().subscribe(done, {"job": "j1"})
        return Out(job="j1", queued=True)

    wake.attach(mcp, identify=lambda r: Identity(user="u1"), wrap={"start_crawl": [done]})
    async with served(mcp) as rpc:
        tools = {t["name"]: t for t in (await rpc("tools/list", {}))["result"]["tools"]}
        sc = tools["start_crawl"]
        assert sorted(sc["inputSchema"]["properties"]) == ["conversation_id", "harness", "session_key", "url"]
        assert "This tool is how" in sc["description"]
        assert "wakeshift" in sc["outputSchema"]["properties"]
        res = (await rpc("tools/call", {"name": "start_crawl", "arguments": {"url": "u", **WAKE}}))["result"]
    assert res["structuredContent"] == {"job": "j1", "queued": True, "wakeshift": WOKEN}
    assert [c["text"] for c in res["content"]][-1] == WOKEN
    assert core.bodies("/event-subscriptions")[0]["call"]["tool"] == "start_crawl"


async def test_the_shared_subscribing_tools_list() -> None:
    """subscribing-tools-list.json, through @attached.tool and through wrap."""
    for via_wrap in (False, True):
        for scenario in FIXTURE["scenarios"]:
            wake = client(scenario["enabled"])
            events = define(wake, scenario["events"])
            server = Server("crawler")
            attached = wake.attach(
                server,
                identify=lambda r: None,
                builtin_tools=scenario["builtin_tools"],
                wrap=wrap_of(events, scenario["tools"]) if via_wrap else None,
            )
            register(server, attached, events, scenario["tools"], via_wrap=via_wrap)
            async with served(server) as rpc:
                got = (await rpc("tools/list", {}))["result"]["tools"]
            check(got, scenario["tools_list"], f"{scenario['name']} via_wrap={via_wrap}")


async def test_the_built_in_tools_and_identify() -> None:
    """subscribe_events, list_subscriptions and unsubscribe_events, with
    identify reading the request's headers; require_user refuses an
    anonymous caller's part, never the tool."""
    wake, core, done = wake_core()
    core.on("POST", "/event-subscriptions/list", lambda b: (200, {"subscriptions": []}))
    mcp = FastMCP("crawler")
    seen: list[Any] = []

    def identify(r: Any) -> Identity | None:
        seen.append(r.headers.get("x-user"))
        return Identity(user=r.headers["x-user"]) if r.headers.get("x-user") else None

    wake.attach(mcp, identify=identify, require_user=True)
    async with served(mcp, {"x-user": "ada"}) as rpc:
        res = await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "crawl.done", "arguments": {"job": "j1"}, **WAKE}}
        )
        assert texts(res)[0].startswith("Subscribed to crawl.done"), res
        res = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {}})
        assert not res["result"].get("isError"), res
        res = await rpc(
            "tools/call",
            {"name": "subscribe_events", "arguments": {"event": "crawl.done", "arguments": {"job": "j1"}, **WAKE}},
            headers={"x-user": ""},
        )
        assert res["result"]["isError"] or "error" in res
    assert core.bodies("/event-subscriptions")[0]["call"]["user"] == "ada"
    assert core.bodies("/event-subscriptions/list") == [{"user": "ada"}]
    assert "ada" in seen


async def test_a_token_verified_by_fastmcp_reaches_identify() -> None:
    from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

    wake, core, done = wake_core()
    mcp = FastMCP("crawler", auth=StaticTokenVerifier({"tok-ada": {"client_id": "ada", "scopes": []}}))
    wake.attach(mcp, identify=lambda r: Identity(user=getattr(r.token, "client_id", None)), require_user=True)
    async with served(mcp, {"Authorization": "Bearer tok-ada"}) as rpc:
        res = await rpc(
            "tools/call", {"name": "subscribe_events", "arguments": {"event": "crawl.done", "arguments": {"job": "j1"}, **WAKE}}
        )
    assert not res["result"].get("isError"), res
    assert core.bodies("/event-subscriptions")[0]["call"]["user"] == "ada"


async def test_no_native_events_every_client_gets_the_tools_and_the_block() -> None:
    """mcp 1.x has no events/*: initialize advertises no MCP Events, and a
    client that declares them still gets subscribe_events and the block."""
    wake, _, done = wake_core()
    mcp = FastMCP("crawler")
    attached = wake.attach(mcp, identify=lambda r: Identity(user="u1"))

    @attached.tool(subscribes=[done])
    async def watch(job: str, call: ToolCall) -> str:
        await call.subscribe(done, {"job": job})
        return "ok"

    caps = {"extensions": {"io.modelcontextprotocol/events": {}}}
    async with served(mcp) as rpc:
        init = await rpc(
            "initialize",
            {"protocolVersion": "2025-06-18", "capabilities": caps, "clientInfo": {"name": "claude-code", "version": "2"}},
        )
        assert "events" not in json.dumps(init["result"]["capabilities"])
        names = [t["name"] for t in (await rpc("tools/list", {}))["result"]["tools"]]
        assert "subscribe_events" in names
        assert (await rpc("events/list", {})).get("error")
        res = await rpc("tools/call", {"name": "watch", "arguments": {"job": "j1", **WAKE}, "_meta": {"openai/session": "c1"}})
    assert texts(res) == [f"ok\n\n{WOKEN}"]


async def test_the_wait_tool_is_refused() -> None:
    wake, _, _ = wake_core()
    with pytest.raises(UsageError, match="needs MCP tasks"):
        wake.attach(FastMCP("a"), builtin_tools={"wait": True})


async def test_a_failed_tool_rolls_back_what_it_subscribed() -> None:
    wake, core, done = wake_core()
    mcp = FastMCP("a")

    @mcp.tool
    async def boom(url: str) -> str:
        await wake.current_call().subscribe(done, {"job": "j1"})
        raise RuntimeError("tool failed")

    wake.attach(mcp, identify=lambda r: Identity(user="u1"), wrap={"boom": [done]})
    async with Client(mcp) as c:
        r = await c.call_tool("boom", {"url": "u", **WAKE}, raise_on_error=False)
    assert r.is_error
    assert [(b["name"], b["reason"]) for b in core.bodies("/event-subscriptions/unsubscribe")] == [("crawl.done", "rolled_back")]


async def test_a_mounted_attached_server_refuses_its_wrapped_tools() -> None:
    """As on FastMCP 4: attach the server you run. Served through a parent,
    the parent's middleware has seen the call as sent, so a wrapped tool
    refuses to run, and attach warns once."""
    wake, core, done = wake_core()
    child = FastMCP("child")

    @child.tool
    async def crawl(url: str) -> str:
        await wake.current_call().subscribe(done, {"job": "j3"})
        return "ok"

    @child.tool
    def plain(text: str) -> str:
        return f"plain {text}"

    wake.attach(child, identify=lambda r: Identity(user="u1"), wrap={"crawl": [done]})
    parent = FastMCP("parent")
    try:
        parent.mount(child, namespace="kid")
    except TypeError:  # FastMCP 2: prefix
        parent.mount(child, prefix="kid")
    logged = Logged(logging.WARNING)
    async with Client(parent) as c:
        tools = {t.name: t for t in await c.list_tools()}
        crawl_name = next(n for n in tools if n.endswith("crawl"))
        assert "session_key" not in tools[crawl_name].inputSchema["properties"]
        assert not [n for n in tools if n.endswith("subscribe_events")]
        r = await c.call_tool(crawl_name, {"url": "u"}, raise_on_error=False)
        assert r.is_error and "attach on a mounted server isn't served" in r.content[0].text
        r = await c.call_tool(next(n for n in tools if n.endswith("plain")), {"text": "t"})
        assert r.content[0].text == "plain t"
    assert not core.bodies("/event-subscriptions")
    assert len([m for m in logged if "mounted" in m]) == 1


async def test_fastmcp_before_2_12_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    import wakeshift._hosts as hosts

    monkeypatch.setattr(hosts, "_version", lambda dist: (2, 11, 4) if dist == "fastmcp" else None)
    wake, _, _ = wake_core()
    with pytest.raises(ImportError, match="FastMCP 2.12 and later 2.x, 3.x"):
        wake.attach(FastMCP("a"))


async def test_a_disabled_client_registers_tools_as_written() -> None:
    wake = Wakeshift("")
    done = wake.define_event("crawl.done", description="A crawl finished", args=Job, payload=Done)
    mcp = FastMCP("a")
    attached = wake.attach(mcp)

    @attached.tool(subscribes=[done])
    async def watch(job: str, call: ToolCall) -> str:
        await call.subscribe(done, {"job": job})
        return "ok"

    async with served(mcp) as rpc:
        tools = (await rpc("tools/list", {}))["result"]["tools"]
        assert [t["name"] for t in tools] == ["watch"]
        assert sorted(tools[0]["inputSchema"]["properties"]) == ["job"]
        assert texts(await rpc("tools/call", {"name": "watch", "arguments": {"job": "j1"}})) == ["ok"]


async def test_a_tools_call_handler_set_after_attach_gets_the_strip_too() -> None:
    """attach can come first: a vendor that replaces (or wraps) the low-level
    tools/call handler after attach (Magic Hour's validator refusing unknown
    arguments) never sees session_key, and the wrapped tool still gets its block."""
    import mcp.types as mt

    wake, core, done = wake_core()
    mcp = FastMCP("crawler")
    attached = wake.attach(mcp, identify=lambda r: Identity(user="u1"))

    @attached.tool(subscribes=[done])
    async def watch(job: str, call: ToolCall) -> str:
        """Watch a job."""
        await call.subscribe(done, {"job": job})
        return f"watching {job}"

    seen: list[dict[str, Any]] = []
    handlers = mcp._mcp_server.request_handlers
    fastmcps = handlers[mt.CallToolRequest]

    async def validator(req: Any) -> Any:
        args = dict(req.params.arguments or {})
        seen.append(args)
        if set(args) - {"job"}:
            raise ValueError(f"unexpected arguments {sorted(set(args) - {'job'})}")
        return await fastmcps(req)

    handlers[mt.CallToolRequest] = validator
    async with served(mcp) as rpc:
        res = await rpc("tools/call", {"name": "watch", "arguments": {"job": "j1", **WAKE}})
    assert texts(res) == [f"watching j1\n\n{WOKEN}"], res
    assert seen == [{"job": "j1"}]
    assert [b["call"]["session"] for b in core.bodies("/event-subscriptions")] == [SECRET]


async def test_on_list_tools_middleware_sees_the_builtins() -> None:
    """A vendor's on_list_tools (Magic Hour's securitySchemes) amends the
    built-ins too: they're in FastMCP's registry, listed as any tool is."""
    schemes = [{"type": "oauth2"}]

    class Stamp(Middleware):
        async def on_list_tools(self, context: Any, call_next: Any) -> Any:
            return [
                t.model_copy(update={"meta": {**(getattr(t, "meta", None) or {}), "securitySchemes": schemes}})
                for t in await call_next(context)
            ]

    wake, core, done = wake_core()
    mcp = FastMCP("crawler")
    attached = wake.attach(mcp, identify=lambda r: Identity(user="u1"))
    mcp.add_middleware(Stamp())

    @attached.tool(subscribes=[done])
    async def watch(job: str, call: ToolCall) -> str:
        """Watch a job."""
        return "ok"

    async with served(mcp) as rpc:
        tools = (await rpc("tools/list", {}))["result"]["tools"]
        res = await rpc("tools/call", {"name": "list_subscriptions", "arguments": {}})
    assert {t["name"] for t in tools} == {"watch", "subscribe_events", "unsubscribe_events", "list_subscriptions"}
    for t in tools:
        assert (t.get("_meta") or {}).get("securitySchemes") == schemes, t["name"]
    assert not res["result"].get("isError"), res
