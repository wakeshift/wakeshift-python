"""Wakeshift's SDK for MCP servers in Python: define your app's events, emit
them, and Wakeshift wakes every subscribed agent (Claude Code, Codex, ChatGPT…)
the best way its client allows.

    from wakeshift import Wakeshift

    wake = Wakeshift(os.environ["WAKESHIFT_KEY"])
    message = wake.define_event("chat.message", description=..., args=..., payload=..., text=...)
    attached = wake.attach(server, identify=lambda ctx: Identity(user=user_of(ctx)))

    @attached.tool()
    async def watch(channel: str, call: ToolCall) -> str:
        await call.subscribe(message, {"channel": channel})
        return f"Watching #{channel}."

    await message.emit({"channel": "eng"}, {"sender": "ada", "text": "shipped it"})
"""

from .client import (
    CONVERSATION_ARG,
    DEFAULT_CORE,
    HARNESS_ARG,
    SESSION_ARG,
    AgentScope,
    AppInfo,
    Emitted,
    EventTypeManifest,
    Manifest,
    PublishUrl,
    Recipient,
    Recognized,
    ScopeEvent,
    WakeLimit,
    Wakeshift,
)
from .errors import WakeshiftError, is_code
from .events import (
    BUILTIN_TOOLS,
    EVENT_TOOL_TEXT,
    LIST_TOOL_DESCRIPTION,
    MAX_STATUS_SUBSCRIPTIONS,
    SUBSCRIBE_TOOL_DESCRIPTION,
    UNSUBSCRIBE_TOOL_DESCRIPTION,
    ArgsError,
    EventStatus,
    EventSubscriptionStatus,
    Identity,
    Subscriber,
    WakeEvent,
)
from .harness import harness_agent
from .key import AppKey, parse_key, sign_assertion
from .spec import EXTENSION_KEY, TEXT_META
from .spec import REVISION as EVENTS_REVISION
from .toolcall import Attached, CallEnded, EventsOptions, Subscription, ToolCall, WakeOutput

__version__ = "0.5.2"

__all__ = [
    "BUILTIN_TOOLS",
    "CONVERSATION_ARG",
    "DEFAULT_CORE",
    "EVENTS_REVISION",
    "EVENT_TOOL_TEXT",
    "EXTENSION_KEY",
    "HARNESS_ARG",
    "LIST_TOOL_DESCRIPTION",
    "SESSION_ARG",
    "SUBSCRIBE_TOOL_DESCRIPTION",
    "TEXT_META",
    "UNSUBSCRIBE_TOOL_DESCRIPTION",
    "AgentScope",
    "AppInfo",
    "Attached",
    "CallEnded",
    "AppKey",
    "ArgsError",
    "EventStatus",
    "EventTypeManifest",
    "EventSubscriptionStatus",
    "MAX_STATUS_SUBSCRIPTIONS",
    "Emitted",
    "EventsOptions",
    "Identity",
    "Manifest",
    "PublishUrl",
    "Recipient",
    "Recognized",
    "ScopeEvent",
    "Subscriber",
    "Subscription",
    "ToolCall",
    "WakeEvent",
    "WakeLimit",
    "WakeOutput",
    "Wakeshift",
    "WakeshiftError",
    "harness_agent",
    "is_code",
    "parse_key",
    "sign_assertion",
]
