"""The public surface (design §5, sdk/typescript/test/SURFACE.md): __all__ is
exactly the design's list, every public class says it's wakeshift's, the
modules behind it are private, and what the release deletes is gone."""

from __future__ import annotations

import inspect
import io
import re
import tokenize
import typing
from importlib.metadata import version
from pathlib import Path

import wakeshift

SURFACE = Path(__file__).resolve().parents[2] / "typescript" / "test" / "SURFACE.md"

#: Design §5's __all__, sorted, then the advanced names.
ALL = [
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
]
ADVANCED = ["AgentScope", "AppInfo", "Manifest", "WakeLimit"]
#: Added after the design (release-gate round 2): the built-in tools' names,
#: on_subscribe's argument and event.watch's result.
ADDED = ["BUILTIN_TOOL_NAMES", "Subscribed", "WatchResult"]

DELETED = [
    "wrapped_tool",
    "notifies",
    "is_code",
    "ArgsError",
    "CallEnded",
    "UndeclaredSubscribe",
    "EventsOptions",
    "AttachTools",
    "ToolOptions",
    "WaitToolOptions",
    "WakeOutput",
    "Attached",
    "AppKey",
    "parse_key",
    "sign_assertion",
    "harness_agent",
    "WAKE_ARGS_SCHEMA",
    "BUILTIN_TOOLS",
    "EVENT_TOOL_TEXT",
    "TOOL_OUTPUT_SCHEMAS",
    "SESSION_ARG",
    "HARNESS_ARG",
    "CONVERSATION_ARG",
    "DEFAULT_CORE",
    "EXTENSION_KEY",
    "TEXT_META",
    "EVENTS_REVISION",
    "MAX_STATUS_SUBSCRIPTIONS",
    "PublishUrl",
    "PublishUrlSigning",
    "EventSubscriptionStatus",
    "EventTypeManifest",
    "ScopeEvent",
]


def test_all_is_the_designs_list() -> None:
    assert sorted(wakeshift.__all__[: len(ALL)]) == ALL == wakeshift.__all__[: len(ALL)]
    assert wakeshift.__all__[len(ALL) :] == ADVANCED + ADDED
    assert wakeshift.BUILTIN_TOOL_NAMES == ("subscribe_events", "unsubscribe_events", "list_subscriptions", "wakeshift_wait")
    for name in wakeshift.__all__:
        assert hasattr(wakeshift, name), name


def test_the_surface_checklist_lists_the_same_names() -> None:
    if not SURFACE.is_file():
        return  # the published copy has no checklist
    text = SURFACE.read_text()
    section = text[text.index("### `__all__`, sorted") : text.index("### Signatures (the stub)")]
    listed = re.findall(r"^- \[ \] `(\w+)`$", section, re.M)
    advanced = re.findall(r"Advanced: (.*)$", section, re.M)[0]
    assert listed == ALL
    assert re.findall(r"`(\w+)`", advanced) == ["AppInfo", "Manifest", "AgentScope", "WakeLimit"]


def test_public_classes_are_wakeshifts_and_their_modules_private() -> None:
    for name in wakeshift.__all__:
        obj = getattr(wakeshift, name)
        if inspect.isclass(obj):
            assert obj.__module__ == "wakeshift", name
    public_modules = {p.stem for p in Path(wakeshift.__file__).parent.glob("*.py") if not p.stem.startswith("_")}
    assert public_modules == {"fastmcp", "testing"}, public_modules


def test_what_the_release_deletes_is_gone() -> None:
    for name in DELETED:
        assert not hasattr(wakeshift, name), name
    for name in ("optional",):
        assert not hasattr(wakeshift.Wakeshift, name), name
    for name in ("call",):
        assert not hasattr(wakeshift.Attachment, name), name
    for name in ("publish_url",):
        assert not hasattr(wakeshift.WakeEvent, name), name
    import wakeshift.fastmcp as fm

    assert fm.__all__ == ["identify_by_claim"] and not hasattr(fm, "access_token")


#: Names the release deletes, as the SDK's own docstrings, comments and
#: messages might still mention them (notifies only as the old option: the
#: word is English too).
STALE = re.compile(
    r"\b(wrapped_tool|is_code|ArgsError|CallEnded|UndeclaredSubscribe|EventsOptions|AttachTools|ToolOptions|"
    r"WaitToolOptions|WakeOutput|Attached|AppKey|parse_key|sign_assertion|harness_agent|WAKE_ARGS_SCHEMA|on_warning|"
    r"serve_events|serveEvents)\b|\bnotifies\s*[=(]|\bhttp=|fastmcp\.access_token|\b(Wakeshift|wake)\.optional\b|"
    r"\battached\.(events|inputs)\b|\battached\.call\("
)


def test_the_sources_text_names_nothing_the_release_deletes() -> None:
    stale: list[str] = []
    for f in sorted(Path(wakeshift.__file__).parent.rglob("*.py")):
        for tok in tokenize.generate_tokens(io.StringIO(f.read_text()).readline):
            if tok.type in (tokenize.COMMENT, tokenize.STRING) or tok.type == getattr(tokenize, "FSTRING_MIDDLE", -1):
                stale += [f"{f.name}:{tok.start[0]}: {m.group(0)}" for m in STALE.finditer(tok.string)]
    assert stale == []


def test_signatures_follow_the_stub() -> None:
    init = inspect.signature(wakeshift.Wakeshift.__init__)
    assert list(init.parameters) == ["self", "key", "core", "required", "timeout", "max_retries", "http_client"]
    assert init.parameters["timeout"].default == 20.0 and init.parameters["max_retries"].default == 2
    attach = inspect.signature(wakeshift.Wakeshift.attach)
    assert list(attach.parameters) == ["self", "server", "identify", "require_user", "wrap", "builtin_tools", "is_native_client"]
    define = inspect.signature(wakeshift.Wakeshift.define_event)
    assert list(define.parameters) == [
        "self",
        "name",
        "description",
        "args",
        "payload",
        "text",
        "personal",
        "authorize",
        "public",
        "delete_on_delivery",
        "title",
        "topic",
        "filter",
        "on_subscribe",
    ]
    assert all(p.kind is p.KEYWORD_ONLY for n, p in define.parameters.items() if n not in ("self", "name"))
    subscribe = inspect.signature(wakeshift.ToolCall.subscribe)
    assert list(subscribe.parameters) == ["self", "event", "args", "once", "replay", "ttl", "reason", "label", "context"]
    assert typing.get_args(wakeshift.SubscriptionState) == ("created", "renewed", "native", "unavailable", "disabled")
    assert set(wakeshift.BuiltinTools.__annotations__) == {"subscribe", "list", "unsubscribe", "wait"}
    assert wakeshift.BuiltinTools.__total__ is False
    assert issubclass(wakeshift.UsageError, ValueError)


def test_version_is_the_packages() -> None:
    assert wakeshift.__version__ == version("wakeshift")
