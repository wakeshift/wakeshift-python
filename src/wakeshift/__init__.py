"""Wakeshift's SDK for MCP servers in Python: define your app's events, emit
them, and Wakeshift wakes every subscribed agent (Claude Code, Codex, ChatGPT…)
the best way its client allows.

    from wakeshift import Wakeshift

    wake = Wakeshift(key=os.environ["WAKESHIFT_KEY"])
    message = wake.define_event("chat.message", description=..., args=..., payload=..., text=...)
    wake.attach(server)
    await message.emit({"channel": "eng"}, {"from": "ada", "text": "shipped it"})
"""

from .client import (
    CONVERSATION_ARG,
    DEFAULT_CORE,
    HARNESS_ARG,
    SESSION_ARG,
    UNKNOWN_CLIENT_WARNING,
    WAKE_ARGS_SCHEMA,
    Addition,
    AppInfo,
    Call,
    ConversationId,
    Harness,
    Published,
    PublishUrl,
    Recognised,
    SessionKey,
    Subscription,
    SubscriptionStatus,
    TopicStatus,
    Wakeshift,
    apply_addition,
)
from .errors import WakeshiftError, is_code
from .events import (
    EVENT_TOOL_TEXT,
    LIST_TOOL_DESCRIPTION,
    SUBSCRIBE_TOOL_DESCRIPTION,
    UNSUBSCRIBE_TOOL_DESCRIPTION,
    ArgsError,
    CallerSubscription,
    Identity,
    Subscriber,
    WakeEvent,
    speaks_events,
)
from .harness import harness_agent
from .key import AppKey, parse_key, sign_assertion
from .spec import EXTENSION_KEY, LEGACY_KEY, TEXT_META
from .spec import REVISION as EVENTS_REVISION

__version__ = "0.3.0"

__all__ = [
    "CONVERSATION_ARG",
    "DEFAULT_CORE",
    "EVENTS_REVISION",
    "EVENT_TOOL_TEXT",
    "EXTENSION_KEY",
    "HARNESS_ARG",
    "LEGACY_KEY",
    "LIST_TOOL_DESCRIPTION",
    "SESSION_ARG",
    "SUBSCRIBE_TOOL_DESCRIPTION",
    "TEXT_META",
    "UNKNOWN_CLIENT_WARNING",
    "UNSUBSCRIBE_TOOL_DESCRIPTION",
    "WAKE_ARGS_SCHEMA",
    "Addition",
    "AppInfo",
    "AppKey",
    "ArgsError",
    "Call",
    "CallerSubscription",
    "ConversationId",
    "Harness",
    "Identity",
    "PublishUrl",
    "Published",
    "Recognised",
    "SessionKey",
    "Subscriber",
    "Subscription",
    "SubscriptionStatus",
    "TopicStatus",
    "WakeEvent",
    "Wakeshift",
    "WakeshiftError",
    "apply_addition",
    "harness_agent",
    "is_code",
    "parse_key",
    "sign_assertion",
    "speaks_events",
]
