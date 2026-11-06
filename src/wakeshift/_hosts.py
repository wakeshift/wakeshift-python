"""The server frameworks attach serves on, behind one small interface: the
official SDK's MCPServer, its low-level Server, and FastMCP 4 (the
``fastmcp`` package). All three are, or sit on, the official SDK's low-level
``Server`` (``mcp`` 2.x), so the native ``events/*`` handlers, the wait tool
and attach's middleware are the same for each; only registering a tool, the
Context type a tool asks for and where the middleware goes differ
(docs/fastmcp.md). FastMCP 2.12+ and 3.x sit on ``mcp`` 1.x: their host is
in _fastmcp23.py, with the same middleware and no MCP Events.
"""

from __future__ import annotations

import contextvars
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from ._log import warn

Middleware = Callable[[Any, Callable[[Any], Awaitable[Any]]], Awaitable[Any]]

#: The methods whose results attach amends with capabilities: on FastMCP its
#: own middleware turns them into typed models, which drop the top-level
#: ``events`` key, so attach amends them outside it.
_NEGOTIATION = ("initialize", "server/discover")

#: The Host whose attach middleware is serving the current request: a FastMCP
#: server reached through another one (mounted) sees another, or none.
_SERVING: contextvars.ContextVar[Host | None] = contextvars.ContextVar("wakeshift_serving", default=None)


@dataclass(frozen=True)
class Added:
    """What registering a tool gave: a check of a successful wire result
    against its output schema, for a framework that doesn't check one itself;
    and whether that schema names wakeshift (the tool is then refused)."""

    check: Callable[[Any], str | None] | None = None
    output_names_wakeshift: bool = False


def _props(schema: Any) -> set[str]:
    p = schema.get("properties") if isinstance(schema, Mapping) else None
    return set(p) if isinstance(p, Mapping) else set()


def _insert_after_sdk(chain: list[Any], middleware: Middleware) -> None:
    """Puts middleware just inside the MCP SDK's own (OpenTelemetry, request
    state), ahead of every middleware the app added, before attach or after."""
    i = 0
    while i < len(chain) and (getattr(type(chain[i]), "__module__", "") or "").startswith("mcp."):
        i += 1
    chain.insert(i, middleware)


def _inject_into_handler(low: Any, attached: Any) -> str:
    """Wraps an mcp 2.x low-level Server's tools/list handler so it adds
    attach's own tools to its first page, inside every middleware."""
    entry = low.get_request_handler("tools/list")
    if entry is None:
        return ""
    orig = entry.handler

    async def list_tools(ctx: Any, params: Any) -> Any:
        result = await orig(ctx, params)
        if getattr(params, "cursor", None) is None:
            result = attached._inject_builtins(result)
        return result

    low.add_request_handler("tools/list", entry.params_type, list_tools)
    return "handler"


def _register_with_fastmcp(server: Any, attached: Any, tool_cls: Any) -> str:
    """Puts attach's own tools in a FastMCP server's registry, listed as any
    tool is (its calls never reach FastMCP: attach answers them first), where
    no tool of the server's has the name."""
    from mcp.types import ToolAnnotations

    taken = set()
    try:
        for c in server._local_provider._components.values():
            if str(getattr(c, "key", "")).startswith("tool:"):
                taken.add(getattr(c, "name", None))
    except Exception:
        tools = getattr(getattr(server, "_tool_manager", None), "_tools", None)
        if not isinstance(tools, dict):
            return ""
        taken = set(tools)
    for name, (_, e) in attached._builtins.items():
        if name in taken:
            continue  # the server's own tool keeps the name in its registry; attach's is served
        server.add_tool(
            tool_cls(
                name=name,
                title=e.get("title"),
                description=e["description"],
                parameters=e["inputSchema"],
                output_schema=e.get("outputSchema"),
                annotations=ToolAnnotations(**e["annotations"]),
            )
        )
        attached._registered_builtins.add(name)
    return "registry"


def _without_builtins(tools: Any, attached: Any) -> Any:
    """A mounted server's listing without the built-ins attach registered:
    served through another server, they'd be listed but couldn't be called."""
    if attached is None or not attached._registered_builtins:
        return tools
    return [t for t in tools if getattr(t, "name", None) not in attached._registered_builtins]


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
    #: Whether it serves MCP Events (events/*), server/discover and MCP
    #: tasks: not on mcp 1.x (FastMCP 2/3).
    native_events = True

    def add_tool(
        self,
        fn: Callable[..., Any],
        *,
        name: str,
        description: str | None,
        text: bool = False,
        options: Mapping[str, Any] | None = None,
        wake_args: bool | None = None,
    ) -> Added:
        """Registers fn as a tool: text, the tool replies with text only (no
        output schema or structuredContent) unless options say otherwise;
        wake_args, whether its listed input schema gains Wakeshift's
        arguments for a tool that subscribes to something (True) or nothing
        (False), or none (None)."""
        raise NotImplementedError

    def remove_tool(self, name: str) -> None:
        """Removes a tool add_tool registered."""

    def own_input_props(self, name: str) -> set[str] | None:
        """The properties a tool's own input schema names, where the framework
        keeps a record of its tools (None: unknown)."""
        return None

    def ensure_tool_handlers(self) -> None:
        """Gives a server without tool handlers empty ones (the low-level Server)."""

    def list_builtins(self, attached: Any) -> str:
        """Makes the server list attach's own tools itself, innermost, so
        the app's own tools/list middleware sees them as it sees its tools:
        "handler" (its tools/list handler adds them on each request), or
        "registry" (they're in its tool registry); "" where it can't, and
        attach's middleware adds them."""
        return ""

    def install(self, middleware: Middleware) -> None:
        """Puts attach's middleware on every inbound request."""
        raise NotImplementedError

    def install_attachment(self, attached: Any) -> None:
        """Puts the attachment on the server: its middleware, by default."""
        self.install(attached._middleware)

    def misplaced(self, tool: str) -> str:
        """Why a wrapped tool ran without its attach's middleware in front."""
        return f"wakeshift: {tool} ran without its attach's middleware in front: register it with the Attachment (wake.attach) of the server it's on"


class MCPServerHost(Host):
    framework = "MCPServer"

    def __init__(self, server: Any) -> None:
        from mcp.server.mcpserver import Context

        self.server = server
        self.lowlevel = server._lowlevel_server
        self.context_type = Context

    def add_tool(self, fn, *, name, description, text=False, options=None, wake_args=None):
        from ._toolcall import _with_wake_args

        opts = dict(options or {})
        if text and "structured_output" not in opts:
            # MCPServer would give a str tool an output schema and
            # structuredContent {"result": …}, which some hosts show the
            # model instead of the text.
            opts["structured_output"] = False
        self.server.add_tool(fn, name=name, description=description, **opts)
        tool = self.server._tool_manager.get_tool(name)
        if wake_args is not None:
            tool.parameters = _with_wake_args(tool.parameters, wake_args)
        # MCPServer checks a result against its output schema itself.
        return Added(None, "wakeshift" in _props(tool.output_schema))

    def remove_tool(self, name: str) -> None:
        self.server._tool_manager.remove_tool(name)

    def own_input_props(self, name: str) -> set[str] | None:
        tool = self.server._tool_manager.get_tool(name)
        return None if tool is None else _props(tool.parameters)

    def install(self, middleware: Middleware) -> None:
        _insert_after_sdk(self.server.middleware, middleware)

    def list_builtins(self, attached: Any) -> str:
        return _inject_into_handler(self.lowlevel, attached)


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
        self.attached: Any = None

    def add_tool(self, fn, *, name, description, text=False, options=None, wake_args=None):
        from fastmcp.tools import Tool

        from ._toolcall import _with_wake_args, with_wakeshift_field

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
        # the schemas are amended on the Tool, so a FastMCP transform that
        # renames the tool keeps them.
        tool = Tool.from_function(fn, name=name, description=description, **opts)
        schema = tool.output_schema
        if "wakeshift" in _props(schema):
            return Added(None, True)  # refused before it's registered
        if wake_args is not None:
            tool.parameters = _with_wake_args(tool.parameters, wake_args)
        if wake_args is None:
            self.server.add_tool(tool)  # a disabled client's: as written
            return Added()
        # The block goes in structuredContent's wakeshift field, so the output
        # schema names it.
        if isinstance(schema, Mapping):
            tool.output_schema = with_wakeshift_field(schema)
        self.server.add_tool(tool)
        return Added(_output_check(schema) if isinstance(schema, Mapping) else None)

    def install(self, middleware: Middleware) -> None:
        low = self.lowlevel
        host = self

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

    def list_builtins(self, attached: Any) -> str:
        from fastmcp.tools import Tool

        self.attached = attached
        return _register_with_fastmcp(self.server, attached, Tool)

    def own_input_props(self, name: str) -> set[str] | None:
        # FastMCP's registry is async to read; its local provider's isn't.
        try:
            for c in self.server._local_provider._components.values():
                if getattr(c, "name", None) == name and str(getattr(c, "key", "")).startswith("tool:"):
                    return _props(c.parameters)
        except Exception:  # a FastMCP whose internals moved: unknown
            return None
        return None

    def misplaced(self, tool: str) -> str:
        return (
            f"wakeshift: {tool} ran without its attach's middleware in front. On FastMCP, attach the server you run, "
            "and mount other servers on it: attach on a mounted server isn't served"
        )

    def mounted(self) -> None:
        if self._warned_mount:
            return
        self._warned_mount = True
        warn(
            f"the FastMCP server {getattr(self.server, 'name', '')!r} that attach was given is being served "
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
                return _without_builtins(await call_next(context), host.attached)
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


def _version(dist: str) -> tuple[int, ...] | None:
    """An installed distribution's version, as numbers (None: not installed)."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        v = version(dist)
    except PackageNotFoundError:
        return None
    out: list[int] = []
    for part in v.split("."):
        digits = "".join(c for c in part if c.isdigit()) if part[:1].isdigit() else ""
        if not digits:
            break
        out.append(int(digits))
    return tuple(out)


def check_sdks(server: Any) -> None:
    """attach's runtime check of the MCP SDK it serves on, since the extras'
    bounds are a resolver's only when installed: mcp 2.3 to below 3, and for
    a FastMCP server, FastMCP 2.12 to below 4 (on whatever mcp 1.x it
    brings: never [mcp], which can't share its install) or 4.1 to below 5.
    Raises ImportError naming what to install."""
    if _is_fastmcp(server):
        fv = _version("fastmcp")
        shown = ".".join(map(str, fv or ()))
        if fv is not None and (2, 12) <= fv < (4,):
            # FastMCP 2/3 on mcp 1.x: attach through FastMCP's middleware
            # (_fastmcp23), without MCP Events. Never [mcp] beside it (#64).
            return
        if fv is None or not ((4, 1) <= fv < (5,)):
            raise ImportError(
                f"wakeshift: attach supports FastMCP 2.12 and later 2.x, 3.x, and 4.1 and later 4.x, not {shown or 'this one'}: "
                "upgrade fastmcp (pip install -U 'fastmcp>=2.12'), keeping plain wakeshift on FastMCP 2 or 3"
            )
    mv = _version("mcp")
    if mv is None:
        raise ImportError("wakeshift: attach needs the MCP Python SDK: pip install 'wakeshift[mcp]'")
    if not ((2, 3) <= mv < (3,)):
        raise ImportError(
            f"wakeshift: attach needs mcp 2.3 or later, below 3, and this is mcp {'.'.join(map(str, mv))}: "
            "pip install 'wakeshift[mcp]'"
        )


class LowLevelHost(Host):
    """The official SDK's low-level Server (mcp.server.lowlevel.Server): it has
    no tool registry, so its tools that subscribe are named in attach's wrap
    and its on_call_tool handler finds their call with wake.current_call().
    attach's own tools are served by attach's middleware, so a handler the
    app sets again after attach changes nothing."""

    framework = "low-level Server"
    registers_tools = False

    def __init__(self, server: Any) -> None:
        from mcp.server.context import ServerRequestContext

        self.server = server
        self.lowlevel = server
        self.context_type = ServerRequestContext

    def install(self, middleware: Middleware) -> None:
        _insert_after_sdk(self.server.middleware, middleware)

    def ensure_tool_handlers(self) -> None:
        """A server without tools gets empty tool handlers, so it advertises
        tools and attach's own can be listed and called."""
        from mcp.shared.exceptions import MCPError
        from mcp_types import CallToolRequestParams, ListToolsResult, PaginatedRequestParams

        low = self.server
        if low.get_request_handler("tools/list") is None:

            async def list_tools(ctx: Any, params: Any) -> Any:
                return ListToolsResult(tools=[])

            low.add_request_handler("tools/list", PaginatedRequestParams, list_tools)
        if low.get_request_handler("tools/call") is None:

            async def call_tool(ctx: Any, params: Any) -> Any:
                raise MCPError(-32602, f"Unknown tool: {params.name}")

            low.add_request_handler("tools/call", CallToolRequestParams, call_tool)

    def list_builtins(self, attached: Any) -> str:
        return _inject_into_handler(self.server, attached)

    def misplaced(self, tool: str) -> str:
        return (
            f"wakeshift: {tool or 'a tool'} ran without its attach's middleware in front: call wake.current_call() "
            "from the on_call_tool handler of the Server that attach was given"
        )


def _is_fastmcp_lowlevel(server: Any) -> bool:
    return any((getattr(c, "__module__", "") or "").startswith("fastmcp.") for c in type(server).__mro__)


def host_of(server: Any) -> Host:
    """The Host for server: an MCPServer, a FastMCP server (2.12+, 3.x or 4)
    or a low-level Server."""
    if hasattr(server, "_lowlevel_server") and hasattr(server, "add_tool"):
        return MCPServerHost(server)
    if _is_fastmcp(server) and (_version("fastmcp") or (4,)) < (4,):
        from ._fastmcp23 import FastMCP23Host

        return FastMCP23Host(server)
    from mcp.server.lowlevel import Server

    if isinstance(server, Server):
        if _is_fastmcp_lowlevel(server):
            raise TypeError("wakeshift: pass attach the FastMCP server itself, not its low-level server")
        return LowLevelHost(server)
    if _is_fastmcp(server):
        return FastMCPHost(server)
    raise TypeError(
        "wakeshift: attach takes an MCPServer (mcp.server.mcpserver.MCPServer), a low-level Server "
        "(mcp.server.lowlevel.Server) or a FastMCP server (fastmcp.FastMCP), "
        f"not {type(server).__module__}.{type(server).__qualname__}"
    )
