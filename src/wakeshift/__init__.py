"""Wakeshift's SDK for MCP servers in Python: define your app's events, emit
them, and Wakeshift wakes every subscribed agent (Claude Code, Codex, ChatGPT…)
the best way its client allows.

    from wakeshift import Wakeshift

    wake = Wakeshift(key=os.environ["WAKESHIFT_KEY"])
    message = wake.define_event("chat.message", description=..., args=..., payload=..., text=...)
    wake.serve_events(server)
    await message.emit({"channel": "eng"}, {"from": "ada", "text": "shipped it"})
"""

from .agent import Agent, Claims
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
    Connection,
    ConversationId,
    Harness,
    Published,
    PublishUrl,
    Recognised,
    SessionKey,
    SignedIn,
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
    Subscriber,
    WakeEvent,
    speaks_events,
)
from .guest import Guest, GuestAuth, guest_of
from .harness import harness_agent
from .key import AppKey, parse_key, sign_assertion
from .signin import SignInError, WakeshiftSignIn
from .spec import EXTENSION_KEY, LEGACY_KEY, TEXT_META
from .spec import REVISION as EVENTS_REVISION

__version__ = "0.2.0"

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
    "Agent",
    "AppInfo",
    "AppKey",
    "ArgsError",
    "Call",
    "CallerSubscription",
    "Claims",
    "Connection",
    "ConversationId",
    "Guest",
    "GuestAuth",
    "Harness",
    "PublishUrl",
    "Published",
    "Recognised",
    "SessionKey",
    "SignInError",
    "SignedIn",
    "Subscriber",
    "Subscription",
    "SubscriptionStatus",
    "TopicStatus",
    "WakeEvent",
    "Wakeshift",
    "WakeshiftError",
    "WakeshiftSignIn",
    "apply_addition",
    "guest_of",
    "harness_agent",
    "is_code",
    "parse_key",
    "sign_assertion",
    "speaks_events",
]
