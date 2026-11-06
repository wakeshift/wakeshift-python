"""Wakeshift's SDK for MCP servers in Python: define your app's events, let the
tools that start work subscribe the conversation calling them, and emit;
Wakeshift wakes every subscribed agent (Claude Code, Codex, ChatGPT…) the best
way its client allows::

    from wakeshift import ToolCall, Wakeshift

    wake = Wakeshift()  # reads WAKESHIFT_KEY; without one, Wakeshift is off
    crawl_finished = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)
    attached = wake.attach(server)

    @attached.tool(subscribes=[crawl_finished])
    async def start_crawl(url: str, call: ToolCall) -> str:
        await call.subscribe(crawl_finished, {"job": job}, once=True)
        ...

    await crawl_finished.emit(Job(job=job), Crawl(status="completed", pages=3))
"""

from importlib.metadata import PackageNotFoundError, version

from ._client import (
    AgentScope,
    AppInfo,
    Emitted,
    Manifest,
    PublishURL,
    PublishURLSigning,
    Recipient,
    Recognized,
    WakeLimit,
    Wakeshift,
)
from ._errors import APIConnectionError, APIError, ErrorCode, IdReusedError, UsageError, WakeshiftError
from ._events import BuiltinTools, EventStatus, Identity, Request, Subscriber, SubscriptionStatus, WakeEvent
from ._secrets import SessionKey, decode_arg_value, encode_arg_value, redact_secrets
from ._toolcall import BUILTIN_NAMES as BUILTIN_TOOL_NAMES
from ._toolcall import Attachment, Subscription, SubscriptionState, ToolCall
from ._watch import Subscribed, WatchResult

try:
    __version__ = version("wakeshift")
except PackageNotFoundError:  # pragma: no cover - a source tree that isn't installed
    __version__ = "0"

__all__ = [
    "APIConnectionError",
    "APIError",
    "Attachment",
    "BuiltinTools",
    "Emitted",
    "ErrorCode",
    "EventStatus",
    "IdReusedError",
    "Identity",
    "PublishURL",
    "PublishURLSigning",
    "Recipient",
    "Recognized",
    "Request",
    "SessionKey",
    "Subscriber",
    "Subscription",
    "SubscriptionState",
    "SubscriptionStatus",
    "ToolCall",
    "UsageError",
    "WakeEvent",
    "Wakeshift",
    "WakeshiftError",
    "decode_arg_value",
    "encode_arg_value",
    "redact_secrets",
    # Advanced: apps that act for their users' agents.
    "AgentScope",
    "AppInfo",
    "Manifest",
    "WakeLimit",
    # Added after the design: the built-in tools' names, on_subscribe's
    # argument and event.watch's result.
    "BUILTIN_TOOL_NAMES",
    "Subscribed",
    "WatchResult",
]

for _name in __all__:
    _obj = globals()[_name]
    if isinstance(_obj, type):
        _obj.__module__ = "wakeshift"
del _name, _obj
