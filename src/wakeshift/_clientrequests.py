"""Client requests: the core's /calls/addition
answer can carry one request for the SDK to make of the MCP client during a
Wakeshift subscribing call. The first kind is an elicitation form (Codex's
install offer, in Codex's own dialog). The SDK knows no wording and no
policy: it offers the kinds it can run in this call (call.client_requests),
runs a request only within fixed bounds it enforces itself, whatever the core
sends, uses the core's text for the outcome, and reports the outcome.
sdk/typescript/test/client-requests.json pins the bounds, the outcome keys
and the offer for every SDK."""

from __future__ import annotations

import time
import weakref
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ._secrets import redact_secrets

#: The kind of request this SDK can run.
ELICIT_FORM = "elicit.form"


@dataclass(frozen=True)
class _Bounds:
    message_max: int = 1200
    message_lines_max: int = 20
    title_max: int = 80
    fields_max: int = 2
    options_max: int = 4
    outcome_text_max: int = 4000
    timeout_ms: int = 120_000
    report_wait_ms: int = 2_000
    session_window_ms: int = 600_000


#: The bounds (client-requests.json bounds).
BOUNDS = _Bounds()

#: The outcome keys besides an accepted answer's <field>=<value>.
OUTCOME_KEYS = ("decline", "cancel", "timeout", "unsupported")

# The protocols a request pushed during a call works on: an allowlist, so a
# revision this SDK doesn't know (2026-07-28 replaced the push with
# input_required) gets none. "": none on the request (stdio).
_PUSH_PROTOCOLS = frozenset({"", "2025-06-18", "2025-11-25"})
_VERSION_META = "io.modelcontextprotocol/protocolVersion"


def request_prefix(app: str) -> str:
    """The message's first line, which the SDK writes itself: Wakeshift and the app, always."""
    name = app.strip()[: BOUNDS.title_max] or "an app"
    return f"Wakeshift, for {name}:"


def request_message(message: str, app: str) -> str:
    """The message the client is sent: the SDK's line, then the core's message."""
    return request_prefix(app) + "\n" + message


def _obj(v: Any) -> bool:
    return isinstance(v, Mapping)


def _answer_keys(r: Mapping[str, Any]) -> list[str]:
    """The values an answer to the request can have, as outcome keys (<field>=<value>)."""
    out: list[str] = []
    for name, f in r["schema"]["properties"].items():
        if f.get("type") == "boolean":
            out += [f"{name}=true", f"{name}=false"]
        else:
            out += [f"{name}={o['const']}" for o in f.get("oneOf") or []]
    return out


def validate_request(raw: Any, app: str) -> str:
    """The empty string if the SDK may run the request for app, else the first bound it
    breaks (client-requests.json validate: kind, message, lines, secret,
    schema, fields, field, options, title, outcomes, outcome_text, placement,
    beacon)."""
    b = BOUNDS
    if not _obj(raw) or raw.get("kind") != ELICIT_FORM or not isinstance(raw.get("token"), str) or not raw["token"]:
        return "kind"
    if not isinstance(raw.get("message"), str):
        return "message"
    message = request_message(raw["message"], app)
    if len(message) > b.message_max:
        return "message"
    if len(message.split("\n")) > b.message_lines_max:
        return "lines"
    if redact_secrets(message) != message:
        return "secret"
    schema = raw.get("schema")
    if not _obj(schema) or schema.get("type") != "object" or not _obj(schema.get("properties")):
        return "schema"
    fields = list(schema["properties"].items())
    if not fields or len(fields) > b.fields_max:
        return "fields"
    for _, f in fields:
        if not _obj(f):
            return "field"
        if f.get("type") == "boolean":
            if "oneOf" in f or "enum" in f:
                return "field"
        elif f.get("type") == "string" and isinstance(f.get("oneOf"), list) and "enum" not in f and "format" not in f:
            options = f["oneOf"]
            if not options or len(options) > b.options_max:
                return "options"
            for o in options:
                if not _obj(o) or not isinstance(o.get("const"), str) or not o["const"] or not isinstance(o.get("title"), str):
                    return "field"
                if len(o["title"]) > b.title_max:
                    return "title"
        else:
            return "field"
        if not isinstance(f.get("title"), str) or len(f["title"]) > b.title_max:
            return "title"
    outcomes = raw.get("outcomes")
    if not _obj(outcomes):
        return "outcomes"
    for k in [*_answer_keys(raw), *OUTCOME_KEYS]:
        if not _obj(outcomes.get(k)):
            return "outcomes"
    for o in outcomes.values():
        if not _obj(o) or not isinstance(o.get("text"), str) or len(o["text"]) > b.outcome_text_max:
            return "outcome_text"
        if o.get("placement") not in ("before", "after"):
            return "placement"
        if o.get("beacon") not in ("keep", "drop"):
            return "beacon"
    return ""


def outcome_key(result: Any, r: Mapping[str, Any]) -> str:
    """The outcome key for the client's elicitation result to a valid request
    (client-requests.json outcome)."""
    if not _obj(result):
        return "unsupported"
    action = result.get("action")
    if action in ("decline", "cancel"):
        return action
    content = result.get("content")
    if action != "accept" or not _obj(content):
        return "unsupported"
    # The first field answered with one of its values names the outcome.
    for name, f in r["schema"]["properties"].items():
        v = content.get(name)
        if f.get("type") == "boolean" and isinstance(v, bool):
            return f"{name}={'true' if v else 'false'}"
        if f.get("type") == "string" and isinstance(v, str) and any(o.get("const") == v for o in f.get("oneOf") or []):
            return f"{name}={v}"
    return "unsupported"


def offers_form(
    capabilities: Mapping[str, Any] | None,
    protocol: str,
    http: bool,
    session: str,
    *,
    off: bool = False,
    recent: bool = False,
) -> bool:
    """Whether the SDK offers an elicitation form for a call (client-requests.json offer)."""
    if off or recent:
        return False
    e = capabilities.get("elicitation") if capabilities is not None else None
    # 2025-06-18 declared elicitation as {}: forms. Later revisions say form.
    if not isinstance(e, Mapping) or (len(e) > 0 and "form" not in e):
        return False
    if protocol not in _PUSH_PROTOCOLS:
        return False
    # Over HTTP, only a stateful session carries the client's answer back to this process.
    return not http or bool(session)


# The MCP sessions a form was shown in, by the call's own session object
# (one per connection, so stdio sessions and other clients in the process
# never share one), with when: at most one per session per window. Weakly
# held: a session's entry goes with it.
_recent_forms: weakref.WeakKeyDictionary[Any, float] = weakref.WeakKeyDictionary()


def _window_key(ctx: Any) -> Any:
    """The object a session's form window is kept on: its connection (mcp
    2.x builds a ServerSession per request, around the connection's), else
    the session itself."""
    session = ctx.session
    return getattr(session, "_connection", None) or session


def _headers(ctx: Any) -> Mapping[str, str]:
    req = getattr(ctx, "request", None)
    h = getattr(req, "headers", None) if req is not None else None
    return h if h is not None else {}


def _session_of(ctx: Any) -> str:
    """The MCP session a form counts against: its Mcp-Session-Id over HTTP
    ("" without one), "stdio" otherwise."""
    if getattr(ctx, "request", None) is None:
        return "stdio"
    return (_headers(ctx).get("mcp-session-id") or "").strip()


def _protocol_of(ctx: Any) -> str:
    meta = getattr(ctx, "meta", None)
    if meta is not None and not isinstance(meta, Mapping):
        dump = getattr(meta, "model_dump", None)
        meta = dump(by_alias=True, exclude_none=True) if dump else None
    v = meta.get(_VERSION_META) if isinstance(meta, Mapping) else None
    return str(v or _headers(ctx).get("mcp-protocol-version") or getattr(ctx, "protocol_version", "") or "")


def _can_elicit(ctx: Any) -> bool:
    """Whether the mcp package can send a request to the client during this
    call: an mcp 2.x request context whose session's request channel takes
    server-to-client requests. mcp 2.x says no itself on stateless HTTP,
    JSON-response mode and protocol 2026-07-28 (NoBackChannelError);
    FastMCP 2/3 on mcp 1.x gets none in this release."""
    session = getattr(ctx, "session", None)
    return (
        session is not None
        and getattr(ctx, "request_id", None) is not None
        and callable(getattr(session, "elicit_form", None))
        and getattr(session, "can_send_request", False) is True
    )


def offered_kinds(ctx: Any, capabilities: Mapping[str, Any] | None, off: bool) -> list[str]:
    """The kinds this call can run, for call.client_requests: [ELICIT_FORM] or []."""
    if off or not _can_elicit(ctx):
        return []
    session = _session_of(ctx)
    at = _recent_forms.get(_window_key(ctx))
    recent = at is not None and time.monotonic() - at < BOUNDS.session_window_ms / 1000
    http = getattr(ctx, "request", None) is not None
    ok = offers_form(capabilities, _protocol_of(ctx), http, session, recent=recent)
    return [ELICIT_FORM] if ok else []


@dataclass(frozen=True)
class Ran:
    """What running a request gives: the outcome key, and its result."""

    outcome: str
    result: Mapping[str, Any]


async def run_form(ctx: Any, r: Mapping[str, Any], app: str) -> Ran:
    """Runs a request (already validated) for the call: shows the form
    through the call's own session and maps the answer. Never raises but for
    the call's own cancellation (the client cancelled the call, or its
    connection closed), which the caller reports as cancel: an error is
    unsupported, no answer in time timeout."""
    import anyio

    try:
        # The mcp package sends notifications/cancelled for a request whose
        # wait is cancelled, by the timeout or by the call's own end.
        with anyio.fail_after(BOUNDS.timeout_ms / 1000):
            result = await ctx.session.elicit_form(
                request_message(r["message"], app), dict(r["schema"]), related_request_id=ctx.request_id
            )
        dump = getattr(result, "model_dump", None)
        outcome = outcome_key(dump(by_alias=True, mode="json", exclude_none=True) if dump else result, r)
    except TimeoutError:
        outcome = "timeout"
    except Exception:
        outcome = "unsupported"
    return Ran(outcome, r["outcomes"].get(outcome) or r["outcomes"]["unsupported"])


def claim_session(ctx: Any) -> bool:
    """Claims the call's session's one form per window, just before showing
    it (the offer checked it too, but the call may have waited): False if a
    form was shown in it within the window."""
    key = _window_key(ctx)
    at = _recent_forms.get(key)
    now = time.monotonic()
    if at is not None and now - at < BOUNDS.session_window_ms / 1000:
        return False
    _recent_forms[key] = now
    return True


def reset_recent_forms() -> None:
    """Forgets the sessions forms were shown in (tests)."""
    _recent_forms.clear()
