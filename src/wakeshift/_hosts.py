"""The server frameworks attach serves on, behind one small interface: the
official SDK's MCPServer, its low-level Server, and FastMCP 4 (the
``fastmcp`` package). All three are, or sit on, the official SDK's low-level
``Server`` (``mcp`` 2.x), so the native ``events/*`` handlers, the wait tool
and attach's middleware are the same for each; only registering a tool, the
Context type a tool asks for and where the middleware goes differ
(docs/fastmcp.md).
"""

from __future__ import annotations

import contextvars
import sys
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

Middleware = Callable[[Any, Callable[[Any], Awaitable[Any]]], Awaitable[Any]]

#: The methods whose results attach amends with capabilities: on FastMCP its
#: own middleware turns them into typed models, which drop the top-level
#: ``events`` key, so attach amends them outside it.
_NEGOTIATION = ("initialize", "server/discover")

#: The Host whose attach middleware is serving the current request: a FastMCP
#: server reached through another one (mounted) sees another, or none.
_SERVING: contextvars.ContextVar[Host | None] = contextvars.ContextVar("wakeshift_serving", default=None)


class Host:
    """What attach needs of a server framework."""

    #: The official SDK's low-level Server (mcp.server.lowlevel.Server).
    lowlevel: Any
    #: The class a tool's Context parameter is annotated with, which the
    #: framework fills.
    context_type: type
    #: The framework's name, for messages.
    framework: str
    #: Whether @attached.tool() can register a tool on it.
    registers_tools = True

    def add_tool(
        self,
        fn: Callable[..., Any],
        *,
        name: str,
        description: str | None,
        annotations: Any = None,
        input_schema: dict[str, Any] | None = None,
        text: bool = False,
        options: Mapping[str, Any] | None = None,
    ) -> Callable[[Any], str | None] | None:
        """Registers fn as a tool. input_schema replaces the schema the
        framework derives; text: the tool replies with text only (no output
        schema or structuredContent), unless options say otherwise. Returns a
        check of a successful wire result against the tool's output schema
        (the reason it fails, or None), for a framework that doesn't check
        one itself."""
        raise NotImplementedError

    def install(self, middleware: Middleware, warn: Callable[[str], None]) -> None:
        """Puts attach's middleware on every inbound request (warn: the
        app's on_warning)."""
        raise NotImplementedError

    def misplaced(self, tool: str) -> str:
        """Why a wrapped tool ran without its attach's middleware in front."""
        return (
            f"wakeshift: {tool} ran without its attach's middleware in front: register it with the Attached of the server it's on"
        )


class MCPServerHost(Host):
    framework = "MCPServer"

    def __init__(self, server: Any) -> None:
        from mcp.server.mcpserver import Context

        self.server = server
        self.lowlevel = server._lowlevel_server
        self.context_type = Context

    def add_tool(self, fn, *, name, description, annotations=None, input_schema=None, text=False, options=None):
        opts = dict(options or {})
        if text and "structured_output" not in opts:
            # MCPServer would give a str tool an output schema and
            # structuredContent {"result": …}, which some hosts show the
            # model instead of the text.
            opts["structured_output"] = False
        if annotations is not None:
            opts["annotations"] = annotations
        self.server.add_tool(fn, name=name, description=description, **opts)
        if input_schema is not None:
            self.server._tool_manager.get_tool(name).parameters = input_schema
        return None  # MCPServer checks a result against its output schema itself

    def install(self, middleware: Middleware, warn: Callable[[str], None]) -> None:
        self.server.middleware.append(middleware)


class FastMCPHost(Host):
    framework = "FastMCP"

    def __init__(self, server: Any) -> None:
        from fastmcp import Context
        from fastmcp.server.low_level import FastMCPServerMiddleware

        low = getattr(server, "_mcp_server", None)
        if low is None or not any(isinstance(m, FastMCPServerMiddleware) for m in getattr(low, "middleware", ())):
            # attach relies on FastMCP's middleware sitting in the official
            # SDK's list (docs/fastmcp.md, "What stays fragile").
            raise TypeError(
                f"wakeshift: this FastMCP ({_fastmcp_version()}) doesn't serve requests the way attach expects: "
                "attach supports FastMCP 4.1 and later 4.x"
            )
        self.server = server
        self.lowlevel = low
        self.context_type = Context
        self._warned_mount = False
        self._warn: Callable[[str], None] = lambda _m: None

    def add_tool(self, fn, *, name, description, annotations=None, input_schema=None, text=False, options=None):
        from fastmcp.tools import Tool

        from .toolcall import with_wakeshift_field

        opts = dict(options or {})
        if opts.get("task"):
            # FastMCP runs a task-enabled tool in the background, after the
            # request that started it, so it couldn't subscribe that request's
            # conversation or carry its block.
            raise TypeError(f"wakeshift: {name}: a wrapped tool can't be a FastMCP background task (task=True)")
        # MCPServer's structured_output, for the same attached.tool() options:
        # True keeps FastMCP's own output schema, False drops it.
        structured = opts.pop("structured_output", None)
        if structured is False:
            opts.setdefault("output_schema", None)
        if text and structured is None and "output_schema" not in opts:
            # FastMCP wraps a str result as structuredContent {"result": …}
            # under an output schema, as MCPServer does.
            opts["output_schema"] = None
        # Tool.from_function, not server.tool: that returns the function, and
        # the schemas are replaced on the Tool.
        if annotations is not None:
            opts["annotations"] = annotations
        tool = Tool.from_function(fn, name=name, description=description, **opts)
        if input_schema is not None:
            tool.parameters = input_schema
        schema = tool.output_schema
        # The block goes in structuredContent's wakeshift field, so the output
        # schema names it here, on the tool: a FastMCP transform that renames
        # the tool keeps it.
        if isinstance(schema, Mapping):
            tool.output_schema = with_wakeshift_field(schema)
        self.server.add_tool(tool)
        return _output_check(schema) if isinstance(schema, Mapping) else None

    def install(self, middleware: Middleware, warn: Callable[[str], None]) -> None:
        low = self.lowlevel
        host = self
        self._warn = warn

        async def negotiation(ctx: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
            if getattr(ctx, "method", "") not in _NEGOTIATION:
                return await call_next(ctx)

            async def as_wire(c: Any) -> Any:
                # FastMCP's middleware returns these results as typed models:
                # the wire form, as the official SDK's runner writes it.
                out = await call_next(c)
                dump = getattr(out, "model_dump", None)
                return dump(by_alias=True, mode="json", exclude_none=True) if dump else out

            return await middleware(ctx, as_wire)

        async def requests(ctx: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
            if getattr(ctx, "method", "") in _NEGOTIATION:
                return await call_next(ctx)
            token = _SERVING.set(host)
            try:
                return await middleware(ctx, call_next)
            finally:
                _SERVING.reset(token)

        # Outermost for initialize and server/discover, so the capabilities
        # attach adds reach the wire as they are; innermost for everything
        # else, inside FastMCP's own middleware: its request context is bound
        # (identify can call fastmcp's get_access_token()), and FastMCP's
        # middleware sees a tool's result before Wakeshift's block is added.
        low.middleware.insert(0, negotiation)
        low.middleware.append(requests)
        self.server.add_middleware(_mount_guard(self))

    def misplaced(self, tool: str) -> str:
        return (
            f"wakeshift: {tool} ran without its attach's middleware in front. On FastMCP, attach the server you run, "
            "and mount other servers on it: attach on a mounted server isn't served"
        )

    def mounted(self) -> None:
        if self._warned_mount:
            return
        self._warned_mount = True
        self._warn(
            f"wakeshift: the FastMCP server {getattr(self.server, 'name', '')!r} that attach was given is being served "
            "through another server (mounted), so its wrapped tools refuse to run and its events aren't served: "
            "attach the server you run, and mount other servers on it"
        )


def _mount_guard(host: FastMCPHost) -> Any:
    """FastMCP middleware on the attached server that notices it's served
    through another one (``parent.mount(server)``): FastMCP runs a mounted
    server's own middleware, but not the low-level middleware attach put on
    it, so its wrapped tools would run without their block."""
    from fastmcp.exceptions import ToolError
    from fastmcp.server.dependencies import fastmcp_request_ctx
    from fastmcp.server.middleware import Middleware

    def through_another() -> bool:
        # A request is being served, but not behind this server's attach (a
        # direct call such as server.list_tools() serves no request).
        return fastmcp_request_ctx.get() is not None and _SERVING.get() is not host

    class MountGuard(Middleware):
        async def on_list_tools(self, context: Any, call_next: Any) -> Any:
            if through_another():
                host.mounted()
            return await call_next(context)

        async def on_call_tool(self, context: Any, call_next: Any) -> Any:
            if through_another():
                host.mounted()
                name = getattr(getattr(context, "message", None), "name", "") or ""
                raise ToolError(host.misplaced(name or "this tool"))
            return await call_next(context)

    return MountGuard()


def _output_check(schema: Mapping[str, Any]) -> Callable[[Any], str | None]:
    """Checks a wire result's structuredContent against a tool's output
    schema, as MCPServer does before it answers: FastMCP doesn't, and a
    client that does would refuse the result after the conversation was
    subscribed."""
    from jsonschema.exceptions import best_match
    from jsonschema.validators import validator_for

    validator_cls = validator_for(schema)
    validator = validator_cls(dict(schema))

    def check(result: Any) -> str | None:
        if not isinstance(result, Mapping):
            return None
        sc = result.get("structuredContent")
        if sc is None:
            return "the tool has an output schema but returned no structured content"
        err = best_match(validator.iter_errors(sc))
        return None if err is None else f"its result doesn't match its output schema: {err.message}"

    return check


def _fastmcp_version() -> str:
    mod = sys.modules.get("fastmcp")
    v = getattr(mod, "__version__", None)
    return f"fastmcp {v}" if v else "fastmcp"


def _is_fastmcp(server: Any) -> bool:
    """Whether server is a FastMCP server of any version, without importing
    fastmcp (whose 2.x and 3.x can't import beside mcp 2.x)."""
    for cls in type(server).__mro__:
        mod = getattr(cls, "__module__", "") or ""
        if cls.__name__ == "FastMCP" and (mod == "fastmcp" or mod.startswith("fastmcp.")):
            return True
    return False


def _major(version: str) -> int | None:
    head = version.split(".", 1)[0]
    return int(head) if head.isdigit() else None


class LowLevelHost(Host):
    """The official SDK's low-level Server (mcp.server.lowlevel.Server): it has
    no tool registry, so attach's own tools are served by wrapping its
    tools/list and tools/call handlers, and an app's on_call_tool subscribes
    with attached.call(ctx) rather than through @attached.tool()."""

    framework = "low-level Server"
    registers_tools = False

    def __init__(self, server: Any) -> None:
        from mcp.server.context import ServerRequestContext

        self.server = server
        self.lowlevel = server
        self.context_type = ServerRequestContext
        self._tools: dict[str, tuple[Callable[..., Awaitable[Any]], Any]] = {}

    def add_tool(self, fn, *, name, description, annotations=None, input_schema=None, text=False, options=None):
        from mcp_types import Tool

        tool = Tool(name=name, description=description, input_schema=input_schema or {"type": "object"}, annotations=annotations)
        self._tools[name] = (fn, tool)
        return None

    def install(self, middleware: Middleware, warn: Callable[[str], None]) -> None:
        from mcp.shared.exceptions import MCPError
        from mcp_types import CallToolRequestParams, ListToolsResult, PaginatedRequestParams

        low = self.server
        own = self._tools
        handlers = getattr(low, "_request_handlers", {})
        listing = handlers.get("tools/list")
        calling = handlers.get("tools/call")

        async def list_tools(ctx: Any, params: Any) -> Any:
            out = await listing.handler(ctx, params) if listing is not None else ListToolsResult(tools=[])
            # attach's own tools after the app's, on its first page only.
            if own and getattr(params, "cursor", None) is None:
                if isinstance(out, Mapping):
                    tools = [
                        *out.get("tools", []),
                        *(t.model_dump(by_alias=True, mode="json", exclude_none=True) for _, t in own.values()),
                    ]
                    return {**out, "tools": tools}
                return out.model_copy(update={"tools": [*out.tools, *(t for _, t in own.values())]})
            return out

        async def call_tool(ctx: Any, params: Any) -> Any:
            entry = own.get(params.name)
            if entry is None:
                if calling is None:
                    raise MCPError(-32602, f"Unknown tool: {params.name}")
                return await calling.handler(ctx, params)
            # The arguments its advertised schema names; others are ignored,
            # as MCPServer ignores them.
            fn, tool = entry
            named = tool.input_schema.get("properties") or {}
            return await fn(**{k: v for k, v in (params.arguments or {}).items() if k in named}, ctx=ctx)

        if own or listing is not None:
            low.add_request_handler(
                "tools/list", listing.params_type if listing is not None else PaginatedRequestParams, list_tools
            )
        if own or calling is not None:
            low.add_request_handler(
                "tools/call", calling.params_type if calling is not None else CallToolRequestParams, call_tool
            )
        low.middleware.append(middleware)

    def misplaced(self, tool: str) -> str:
        return (
            f"wakeshift: attached.call() for {tool or 'a tool'} ran without its attach's middleware in front: call it "
            "from the on_call_tool handler of the Server that attach was given, with that handler's ctx"
        )


def _is_fastmcp_lowlevel(server: Any) -> bool:
    return any((getattr(c, "__module__", "") or "").startswith("fastmcp.") for c in type(server).__mro__)


def host_of(server: Any) -> Host:
    """The Host for server: an MCPServer, a FastMCP 4 or a low-level Server."""
    if hasattr(server, "_lowlevel_server") and hasattr(server, "add_tool"):
        return MCPServerHost(server)
    from mcp.server.lowlevel import Server

    if isinstance(server, Server):
        if _is_fastmcp_lowlevel(server):
            raise TypeError("wakeshift: pass attach the FastMCP server itself, not its low-level server")
        return LowLevelHost(server)
    if _is_fastmcp(server):
        version = str(getattr(sys.modules.get("fastmcp"), "__version__", "") or "")
        major = _major(version)
        if major is not None and major < 4:
            raise TypeError(
                f"wakeshift: attach supports FastMCP 4 (fastmcp>=4.1,<5), and this server is FastMCP {version}: "
                "FastMCP 2.x and 3.x are built on mcp 1.x, which this SDK can't share an install with. "
                "Upgrade with pip install 'wakeshift[fastmcp]'"
            )
        if major is not None and major > 4:
            raise TypeError(f"wakeshift: attach supports FastMCP 4 (fastmcp>=4.1,<5), and this server is FastMCP {version}")
        return FastMCPHost(server)
    raise TypeError(
        "wakeshift: attach takes an MCPServer (mcp.server.mcpserver.MCPServer), a low-level Server "
        "(mcp.server.lowlevel.Server) or a FastMCP 4 server (fastmcp.FastMCP), "
        f"not {type(server).__module__}.{type(server).__qualname__}"
    )
