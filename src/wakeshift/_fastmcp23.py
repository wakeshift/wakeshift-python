"""attach on FastMCP 2.x (2.12 and later) and 3.x, which sit on ``mcp`` 1.x.

Two layers, so the security property is FastMCP 4's: Wakeshift's three
arguments are gone before any vendor middleware, log line or handler sees
them.

1. **The strip**, below FastMCP: the ``mcp`` 1.x low-level Server's
   ``tools/call`` handler (the one FastMCP registers, and any set after
   attach: the handlers dict wraps each one it's given) is wrapped, so
   Wakeshift's arguments are deleted from the request's own argument dict
   before FastMCP has the request: before its middleware (added before
   attach or after, anywhere in the list), its debug log of a call's
   arguments, and its input validation (``strict_input_validation`` refuses
   an extra key). The session key is a ``SessionKey`` from there on.
2. **The bridge**, FastMCP middleware first in the server's list
   (outermost): ``tools/list`` and ``tools/call`` go through the same
   wire-form middleware every other host runs (``Attachment._middleware``),
   with the arguments the strip took handed over in a context variable, so
   tools that subscribe get their call and block, ``current_call()`` works,
   the built-in tools are served, and ``identify`` reads the request.

Not served here, since ``mcp`` 1.x has neither: native MCP Events
(``events/*``) and protocol 2026-07-28 (``server/discover``), and so the wait
tool. Every client gets the bridged tools and the block.
"""

from __future__ import annotations

import contextvars
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ._hosts import _SERVING, Added, Host, Middleware, _output_check, _props, _register_with_fastmcp, _without_builtins
from ._log import warn

if TYPE_CHECKING:
    from ._toolcall import Attachment


@dataclass
class _Req:
    """A request as attach's middleware reads it: the method, the wire params
    and the ``mcp`` 1.x request context (its _meta, the HTTP request, the
    session), which _request_info and identify's Request read through
    ``request_context``."""

    method: str
    params: dict[str, Any]
    request_context: Any = None
    protocol_version: str = ""

    @property
    def request(self) -> Any:
        """The HTTP request (Starlette's), where the transport has one."""
        return getattr(self.request_context, "request", None)


#: What the strip took out of the current tools/call, for the bridge to hand
#: to attach's middleware (taken once).
_TAKEN: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar("wakeshift_fastmcp23_taken", default=None)


class _NoLowLevel:
    """What attach_events reads of a low-level server: no events/* handlers,
    no extensions, nowhere to add them."""

    _request_handlers: dict[str, Any] = {}
    extensions: dict[str, Any] = {}


def _request_ctx() -> Any:
    """The ``mcp`` 1.x request context being served, or None outside a request."""
    try:
        from mcp.server.lowlevel.server import request_ctx  # pyright: ignore[reportAttributeAccessIssue]  # mcp 1.x
    except ImportError:  # pragma: no cover
        return None
    try:
        return request_ctx.get()
    except LookupError:
        return None


def _rc_of(context: Any) -> Any:
    """The ``mcp`` 1.x RequestContext of a FastMCP middleware context."""
    fc = getattr(context, "fastmcp_context", None)
    try:
        rc = getattr(fc, "request_context", None) if fc is not None else None
    except Exception:  # FastMCP raises outside a request
        rc = None
    return rc if rc is not None else _request_ctx()


def _dump(v: Any) -> Any:
    d = getattr(v, "model_dump", None)
    return d(by_alias=True, mode="json", exclude_none=True) if d else v


def _wire_of(res: Any) -> dict[str, Any]:
    """A FastMCP ToolResult (or what a middleware returned) as the wire result."""
    if isinstance(res, Mapping):
        return dict(res)
    out: dict[str, Any] = {"content": [_dump(c) for c in (getattr(res, "content", None) or [])]}
    sc = getattr(res, "structured_content", None)
    if sc is not None:
        out["structuredContent"] = sc
    meta = getattr(res, "meta", None)
    if meta:
        out["_meta"] = dict(meta)
    return out


def _result_of(out: Mapping[str, Any]) -> Any:
    """The wire result back as a FastMCP ToolResult; an error result raises
    ToolError with its text, as a FastMCP tool reports one."""
    import mcp.types as mt
    from fastmcp.exceptions import ToolError
    from fastmcp.tools.tool import ToolResult  # pyright: ignore[reportMissingImports]  # FastMCP 2/3
    from mcp.types import TextContent
    from pydantic import TypeAdapter

    block: Any = mt.ContentBlock  # every mcp 1.x FastMCP 2.12+ installs (1.10 and later) has it
    content = TypeAdapter(list[block]).validate_python(  # pyright: ignore[reportInvalidTypeForm]
        list(out.get("content") or [])
    )
    if out.get("isError"):
        raise ToolError("\n\n".join(c.text for c in content if isinstance(c, TextContent)))
    kw: dict[str, Any] = {"content": content, "structured_content": out.get("structuredContent")}
    if out.get("_meta"):
        try:
            return ToolResult(**kw, meta=dict(out["_meta"]))
        except TypeError:  # FastMCP 2.12: a result has no _meta (the beacon's text form still goes)
            pass
    return ToolResult(**kw)


def _tool_of(entry: Mapping[str, Any], original: Any) -> Any:
    """A listed tool's wire entry, as attach amended it, back as a FastMCP Tool."""
    update = {
        "parameters": entry.get("inputSchema") or {"type": "object"},
        "description": entry.get("description"),
        "output_schema": entry.get("outputSchema"),
    }
    if original is not None:
        return original.model_copy(update=update)
    # One of attach's own tools: listed here only; its calls never reach FastMCP.
    from fastmcp.tools.tool import Tool  # pyright: ignore[reportMissingImports]  # FastMCP 2/3
    from mcp.types import ToolAnnotations

    ann = entry.get("annotations")
    return Tool(
        name=entry["name"],
        title=entry.get("title"),
        annotations=ToolAnnotations(**ann) if isinstance(ann, Mapping) else None,
        **update,
    )


class FastMCP23Host(Host):
    """A FastMCP 2.x or 3.x server (on ``mcp`` 1.x)."""

    framework = "FastMCP 2/3"
    #: No events/* methods, server/discover or MCP tasks on mcp 1.x.
    native_events = False

    def __init__(self, server: Any) -> None:
        from fastmcp import Context

        self.server = server
        self.lowlevel = _NoLowLevel()
        self.context_type = Context
        self._warned_mount = False

    def add_tool(self, fn, *, name, description, text=False, options=None, wake_args=None):
        from fastmcp.tools import Tool

        from ._toolcall import _with_wake_args, with_wakeshift_field

        opts = dict(options or {})
        if opts.get("task"):
            raise TypeError(f"wakeshift: {name}: a wrapped tool can't be a FastMCP background task (task=True)")
        # MCPServer's structured_output, for the same attached.tool() options.
        structured = opts.pop("structured_output", None)
        if structured is False or (text and structured is None and "output_schema" not in opts):
            opts.setdefault("output_schema", None)
        tool = Tool.from_function(fn, name=name, description=description, **opts)
        schema = tool.output_schema
        if "wakeshift" in _props(schema):
            return Added(None, True)  # refused before it's registered
        if wake_args is None:
            self.server.add_tool(tool)  # a disabled client's: as written
            return Added()
        tool.parameters = _with_wake_args(tool.parameters, wake_args)
        if isinstance(schema, Mapping):
            tool.output_schema = with_wakeshift_field(schema)
        self.server.add_tool(tool)
        # FastMCP doesn't check a result against its output schema.
        return Added(_output_check(schema) if isinstance(schema, Mapping) else None)

    def remove_tool(self, name: str) -> None:
        self.server.remove_tool(name)

    def own_input_props(self, name: str) -> set[str] | None:
        # The registry is async to read; its dicts aren't (2.x: the tool
        # manager's; 3.x: the local provider's, as on FastMCP 4).
        try:
            tools = getattr(getattr(self.server, "_tool_manager", None), "_tools", None)
            if isinstance(tools, dict):
                t = tools.get(name)
                return None if t is None else _props(t.parameters)
            for c in self.server._local_provider._components.values():
                if getattr(c, "name", None) == name and str(getattr(c, "key", "")).startswith("tool:"):
                    return _props(c.parameters)
        except Exception:  # internals moved: unknown
            return None
        return None

    def list_builtins(self, attached: Any) -> str:
        from fastmcp.tools.tool import Tool  # pyright: ignore[reportMissingImports]  # FastMCP 2/3

        return _register_with_fastmcp(self.server, attached, Tool)

    def install(self, middleware: Middleware) -> None:
        raise TypeError("wakeshift: FastMCP 2/3 attaches with install_attachment")  # pragma: no cover

    def install_attachment(self, attached: Attachment) -> None:
        """The strip below FastMCP, then the bridge first in its middleware."""
        self._install_strip(attached)
        self.server.middleware.insert(0, self._bridge(attached))

    def _install_strip(self, attached: Attachment) -> None:
        import mcp.types as mt

        low = getattr(self.server, "_mcp_server", None)
        handlers = getattr(low, "request_handlers", None)
        call = handlers.get(mt.CallToolRequest) if isinstance(handlers, dict) else None
        listing = handlers.get(mt.ListToolsRequest) if isinstance(handlers, dict) else None
        if not isinstance(handlers, dict) or call is None or listing is None:
            raise TypeError(
                f"wakeshift: this FastMCP ({_version_shown()}) doesn't serve tools the way attach expects: "
                "attach supports FastMCP 2.12 and later 2.x, 3.x, and 4.1 and later 4.x"
            )
        host = self

        def strip_call(inner: Any) -> Any:
            async def call_tool(req: Any) -> Any:
                # Deleted from the request's own argument dict: nothing after
                # this (FastMCP's log, middleware, validation, the tool) finds
                # the raw value.
                args = getattr(req.params, "arguments", None)
                taken: dict[str, Any] = {}
                if isinstance(args, dict):
                    _, _, taken = attached._strip(_Req("tools/call", {"name": req.params.name, "arguments": args}))
                serving = _SERVING.set(host)
                # A strip outside this one (a handler that wraps the one
                # attach installed) already took them: keep what it took.
                given = _TAKEN.set(taken or _TAKEN.get())
                try:
                    return await inner(req)
                finally:
                    _TAKEN.reset(given)
                    _SERVING.reset(serving)

            call_tool._wakeshift = True  # type: ignore[attr-defined]
            return call_tool

        def mark_list(inner: Any) -> Any:
            async def list_tools(req: Any) -> Any:
                serving = _SERVING.set(host)
                try:
                    return await inner(req)
                finally:
                    _SERVING.reset(serving)

            list_tools._wakeshift = True  # type: ignore[attr-defined]
            return list_tools

        def guarded(key: Any, handler: Any) -> Any:
            if getattr(handler, "_wakeshift", False):
                return handler
            if key is mt.CallToolRequest:
                return strip_call(handler)
            if key is mt.ListToolsRequest:
                return mark_list(handler)
            return handler

        class Handlers(dict):  # type: ignore[type-arg]
            """The low-level server's handlers: a tools/call or tools/list
            handler set after attach (a vendor's own validator, an
            instrumentation library's wrapper) gets the strip too, so attach
            can come first or last."""

            def __setitem__(self, key: Any, value: Any) -> None:
                super().__setitem__(key, guarded(key, value))

            def update(self, *args: Any, **kw: Any) -> None:
                for k, v in dict(*args, **kw).items():
                    self[k] = v

            def setdefault(self, key: Any, default: Any = None) -> Any:
                if key not in self:
                    self[key] = default
                return self[key]

        handlers[mt.CallToolRequest] = strip_call(call)
        handlers[mt.ListToolsRequest] = mark_list(listing)
        try:
            low.request_handlers = Handlers(handlers)  # type: ignore[union-attr]
        except (AttributeError, TypeError):  # a server whose handlers can't be replaced: attach must come last
            warn("this FastMCP's request handlers can't be watched: attach after anything that replaces its tools/call handler")

    def _through_another(self) -> bool:
        """A request is being served, but not through this server's own
        handlers: it's mounted on another server, whose middleware has seen
        the call as the client sent it."""
        return _request_ctx() is not None and _SERVING.get() is not self

    def _bridge(self, attached: Attachment) -> Any:
        from fastmcp.exceptions import ToolError
        from fastmcp.server.middleware import Middleware as FastMCPMiddleware

        host = self
        middleware = attached._middleware

        class Wakeshift(FastMCPMiddleware):
            async def on_list_tools(self, context: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
                if host._through_another():
                    host.mounted()
                    return _without_builtins(await call_next(context), attached)
                tools = list(await call_next(context))
                by_name = {t.name: t for t in tools}

                async def listed(_: Any) -> dict[str, Any]:
                    return {"tools": [_dump(t.to_mcp_tool(name=t.name)) for t in tools]}

                out = await middleware(_Req("tools/list", {}, _rc_of(context)), listed)
                return [_tool_of(e, by_name.get(e.get("name"))) for e in out["tools"]]

            async def on_call_tool(self, context: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
                msg = context.message
                if host._through_another():
                    host.mounted()
                    if msg.name in attached._builtins or attached._declared_for(msg.name) is not None:
                        raise ToolError(host.misplaced(msg.name))
                    return await call_next(context)
                args = dict(msg.arguments or {})
                taken = _TAKEN.get()
                if taken:
                    # Back in for attach's own strip, on this copy only (as
                    # SessionKeys: the raw value is in no dict anyone sees).
                    _TAKEN.set(None)
                    args.update(taken)
                req = _Req("tools/call", {"name": msg.name, "arguments": args}, _rc_of(context))

                async def called(r: _Req) -> dict[str, Any]:
                    stripped = context.copy(message=msg.model_copy(update={"arguments": r.params["arguments"]}))
                    return _wire_of(await call_next(stripped))

                return _result_of(await middleware(req, called))

        return Wakeshift()

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
            "through another server (mounted), so its wrapped tools refuse to run and its built-in tools aren't "
            "listed: attach the server you run, and mount other servers on it"
        )


def _version_shown() -> str:
    from ._hosts import _fastmcp_version

    return _fastmcp_version()
