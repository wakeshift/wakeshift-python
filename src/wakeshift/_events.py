"""Events: what an app offers to be subscribed to, the way MCP Events
(SEP-3415) lists them. An app defines its event types (define_event), serves
them (attach) and emits occurrences (emit). Wakeshift delivers each
occurrence to every subscriber the best way its client allows: natively (MCP
Events, a webhook) for a client that speaks the extension, and through
Wakeshift's wake paths for everyone else, who subscribe with the
subscribe_events tool attach adds. A port of the Node SDK's events.ts;
the tools' wording and events/list output are the same, checked against the
shared golden files."""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, timedelta
from typing import TYPE_CHECKING, Any, Generic, Literal, TypedDict

from mcp_types import CallToolResult, TextContent
from pydantic import BaseModel, ConfigDict, ValidationError
from typing_extensions import TypeVar

from ._client import (
    Emitted,
    PublishURL,
    PublishURLSigning,
    Recipient,
    _Addition,
    _apply_addition,
    _call,
    _request_info,
    _transport_session,
)
from ._errors import APIConnectionError, APIError, UsageError, usage, what_of
from ._spec import (
    DELIVERY,
    EXTENSION_KEY,
    HEADER_MISMATCH,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    NAME_HEADER,
    RESULT_TYPE,
    Codes,
    codes_for,
    dialect_of,
    header_value,
)
from ._tasks import TOOL_ANNOTATIONS, TaskServer
from ._text import OUTPUT_SCHEMAS, TEXT, wake_args

if TYPE_CHECKING:
    from mcp.server.auth.provider import AccessToken

    from ._client import Wakeshift
    from ._toolcall import Attachment
    from ._watch import Subscribed, WatchResult

Schema = type[BaseModel] | Mapping[str, Any]


@dataclass(frozen=True)
class Identity:
    """Who is behind a request to the app's MCP server, as the app knows them
    (attach's identify)."""

    #: The app's ID for the person: what personal events and
    #: list_subscriptions need, and native subscriptions belong to.
    user: str | None = None
    #: The MCP client's name, for a request that doesn't carry one: a
    #: stateless server never sees initialize, so the app passes the name it
    #: knows from its own sign-in (an OAuth registration's client_name, say).
    #: Wakeshift reads it to tell which program the agent runs in, and
    #: whether the client subscribes to events itself. A name the request
    #: carries (initialize's, or its _meta's) wins. Leave it None for a
    #: caller that must have no name.
    client: str | None = None


class Request:
    """The request behind a call, as identify, authorize and is_native_client
    see it. ``token`` is the access token the server's own auth verified for
    this HTTP request, None without one: an
    ``mcp.server.auth.provider.AccessToken`` (``token``, ``client_id``,
    ``scopes``, ``expires_at``, ``resource``, ``subject``, ``claims``), or
    FastMCP's subclass of it (``fastmcp.server.auth.AccessToken``) on a
    FastMCP server; ``headers`` are the HTTP
    request's, as the client sent them, and aren't verified. ``raw`` is the
    MCP SDK's request context. It never prints any of them (its repr shows
    only the client's name and whether there's a token), and copying or
    pickling it doesn't copy them out: it's immutable, and not a dataclass,
    so ``dataclasses.asdict`` of a Subscriber keeps it whole."""

    __slots__ = ("client_name", "headers", "raw", "token")

    headers: Mapping[str, str]
    token: AccessToken | None
    #: The MCP client's name (initialize's, or the request's _meta's).
    client_name: str | None
    raw: Any

    def __init__(
        self,
        headers: Mapping[str, str] | None = None,
        token: AccessToken | None = None,
        client_name: str | None = None,
        raw: Any = None,
    ) -> None:
        object.__setattr__(self, "headers", dict(headers or {}))
        object.__setattr__(self, "token", token)
        object.__setattr__(self, "client_name", client_name)
        object.__setattr__(self, "raw", raw)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("wakeshift: a Request can't be changed")

    def __repr__(self) -> str:
        return f"Request(client_name={self.client_name!r}, token={self.token is not None})"

    def __copy__(self) -> Request:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> Request:
        return self

    def __reduce__(self) -> Any:
        raise TypeError("wakeshift: a Request isn't pickled: it holds the caller's credential")


@dataclass(frozen=True)
class Subscriber:
    """Who is subscribing: what authorize decides on. user comes from
    identify; client and native are what the client says, never a reason to
    allow. request is the request behind it (its verified token)."""

    #: The app's ID for the person (attach's identify), if it has one.
    user: str | None
    #: The MCP client's name, if known.
    client: str
    #: Subscribing natively (MCP Events), or through subscribe_events.
    native: bool
    request: Request | None = field(default=None, repr=False)


#: How long identify and authorize may take before Wakeshift's part of the
#: request is refused, in seconds (tests shorten them).
LIMITS: dict[str, float] = {"identify": 10.0, "authorize": 10.0}


class BuiltinTools(TypedDict, total=False):
    """Which of attach's own tools it serves (attach's ``builtin_tools``):
    ``subscribe`` (subscribe_events), ``list`` (list_subscriptions) and
    ``unsubscribe`` (unsubscribe_events) are on unless turned off; ``wait``
    (wakeshift_wait, Claude Code's wait tool) is off unless turned on. With
    the wait tool on, a waiting conversation's Claude Code holds a request to
    your server (about 20 seconds) and sends the next about a second later:
    on a serverless host, give the function a timeout of at least 30 seconds.
    Turn list or unsubscribe off only if your own tools let a person see and
    stop their subscriptions."""

    subscribe: bool
    list: bool
    unsubscribe: bool
    wait: bool


_TOOL_KEYS = {"subscribe": "subscribe_events", "list": "list_subscriptions", "unsubscribe": "unsubscribe_events"}


def builtin_choice(builtin_tools: BuiltinTools | Mapping[str, Any] | Literal[False] | None) -> tuple[set[str], bool]:
    """attach's builtin_tools as the tools to serve, and whether to serve the
    wait tool (False: none of them). A key or value it doesn't know raises UsageError config, so a
    typo never leaves a tool on that the app believes is off."""
    if builtin_tools is None:
        return set(_TOOL_KEYS.values()), False
    if builtin_tools is False:
        return set(), False
    if not isinstance(builtin_tools, Mapping):
        raise usage("config", 'attach\'s builtin_tools is a mapping, {"subscribe": False, "wait": True}, or False for none')
    for key, value in builtin_tools.items():
        if key != "wait" and key not in _TOOL_KEYS:
            raise usage("config", f"attach's builtin_tools has {key!r}: its keys are subscribe, list, unsubscribe and wait")
        if not isinstance(value, bool):
            raise usage("config", f"attach's builtin_tools[{key!r}] is True or False")
    wanted = {name for key, name in _TOOL_KEYS.items() if builtin_tools.get(key, True)}
    return wanted, bool(builtin_tools.get("wait", False))


_PLACEHOLDER = re.compile(r"\{(\w+)\}")
_UNNAMED_APP = "this app"
_TRAILING = re.compile(r"[.\s]+$")


def _event_descriptions(descriptions: Iterable[str]) -> list[str]:
    """The declared events' descriptions as the subscribes sentence quotes
    them: each on one line, its trailing periods trimmed, each once."""
    out: list[str] = []
    for d in descriptions:
        c = _TRAILING.sub("", " ".join((d or "").split()))
        if c and c not in out:
            out.append(c)
    return out


def _subscribes_sentence(app: str, descriptions: Iterable[str]) -> str:
    """The sentence tools/list adds to a tool that declares the events it
    subscribes to: that the tool is how the app notifies the agent of them,
    in the events' own words (subscribes.json)."""
    ds = _event_descriptions(descriptions)
    if not ds:
        return ""
    of = fill("notifies_event" if len(ds) == 1 else "notifies_events", events=TEXT["notifies_events_separator"].join(ds))
    return (
        TEXT["notifies"].replace("{when}", of).replace("{App}", "This app" if app == _UNNAMED_APP else app).replace("{app}", app)
    )


def _with_subscribes(description: str, descriptions: Iterable[str], app: str = "") -> str:
    """A tool's description as tools/list gives it for a tool that declares
    the events it subscribes to: the subscribes sentence after the tool's own
    description, naming the app ("this app" without a name). The sentence
    comes from subscribes, always: a description is never checked for one it
    has already (subscribes.json compact_cases). No events leaves it as it
    was."""
    n = _subscribes_sentence((app or "").strip() or _UNNAMED_APP, descriptions)
    if not n:
        return description
    # The vendor's description as written (markdown, examples), only its
    # trailing whitespace dropped; a blank line before the sentence when it
    # has line breaks, else a space.
    d = (description or "").rstrip()
    if not d.strip():
        return n
    if "\n" in d or "\r" in d:
        return d + "\n\n" + n
    # A one-line description that doesn't end its sentence gets a period, so
    # the sentence doesn't run on from it.
    return d + ("" if _ends_sentence(d) else ".") + " " + n


#: What may follow a sentence's final mark: closing quotes, parentheses, brackets.
_CLOSERS = "\"')]}\u201d\u2019\u00bb"


def _ends_sentence(d: str) -> bool:
    """Whether d's last character, after any closing quotes, parentheses or
    brackets, is . ! ? or an ellipsis (subscribes.json compact_cases)."""
    t = d.rstrip(_CLOSERS)
    return bool(t) and t[-1] in ".!?\u2026"


def fill(key: str, **vars: Any) -> str:
    """TEXT[key] with each {name} replaced."""
    return _PLACEHOLDER.sub(lambda m: str(vars[m.group(1)]) if m.group(1) in vars else m.group(0), TEXT[key])


#: The event argument a native client passes a subscribe voucher in
#: (docs/subscribe-vouchers.md §4.4): reserved, and taken out before anything
#: reads the arguments.
VOUCHER_ARG = "wakeshift_voucher"


def _take_voucher(args: Any) -> tuple[Any, str | None]:
    """args without wakeshift_voucher, and the voucher: a non-empty string,
    else None (anything else in its place is no voucher). Only the exact name:
    Wakeshift_Voucher and the like stay arguments."""
    if not isinstance(args, Mapping) or VOUCHER_ARG not in args:
        return args, None
    rest = {k: v for k, v in args.items() if k != VOUCHER_ARG}
    v = args[VOUCHER_ARG]
    return rest, v if isinstance(v, str) and v else None


_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*){0,4}$")
_ARG_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_MAX_LISTED_EVENTS = 20


class _Invalid(Exception):
    """A value that doesn't match a schema: what's wrong, and the issues
    (each {"message", "path"}), for the UsageError its caller raises."""

    def __init__(self, what: str, issues: list[dict[str, Any]] | None = None) -> None:
        super().__init__(what)
        self.what = what
        self.issues = issues if issues is not None else [{"message": what, "path": []}]


def _compact(v: Any) -> str:
    """JSON as JavaScript's JSON.stringify writes it: a float with no
    fraction is written as a whole number (1.0 is 1)."""
    return json.dumps(_js_numbers(v), separators=(",", ":"), ensure_ascii=False)


def _js_numbers(v: Any) -> Any:
    """v with every float that has no fraction made an int, as JavaScript
    has a single number type."""
    if isinstance(v, float) and v.is_integer() and abs(v) < 1e21:
        return int(v)
    if isinstance(v, list | tuple):
        return [_js_numbers(x) for x in v]
    if isinstance(v, dict):
        return {k: _js_numbers(x) for k, x in v.items()}
    return v


def _default_text(description: str, args: Mapping[str, Any]) -> str:
    """The line an agent reads for an event without text, the same in every
    SDK (default-text.json): the description (whitespace collapsed, trailing
    periods and spaces trimmed), then its arguments in name order, "key
    value", skipping None and empty strings; strings raw, other values
    compact JSON. "A crawl finished (job 3f2a)"."""
    d = _TRAILING.sub("", " ".join(description.split()))
    parts = [f"{k} {v if isinstance(v, str) else _compact(v)}" for k, v in sorted(args.items()) if v is not None and v != ""]
    return f"{d} ({', '.join(parts)})" if parts else d


def _canonical(v: Any) -> str:
    """JSON with every object's keys in name order, as Wakeshift keeps a
    subscription's arguments: how the tools' results echo them."""
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


# ── Schemas ─────────────────────────────────────────────────────────────────
# A pydantic model's JSON Schema, written the way zod's is (and the Go SDK's):
# no titles or defaults, an optional field as its plain type, nested models
# inline. The three SDKs' events/list and manifests then match exactly.

_DROP = {"title", "default", "$schema", "examples"}


def _deref(node: Any, defs: Mapping[str, Any]) -> Any:
    if isinstance(node, list):
        return [_deref(x, defs) for x in node]
    if not isinstance(node, Mapping):
        return node
    ref = node.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        merged = dict(_deref(defs[ref[len("#/$defs/") :]], defs))
        for k, v in node.items():
            if k != "$ref":
                merged[k] = _deref(v, defs)
        return merged
    return {k: _deref(v, defs) for k, v in node.items() if k != "$defs"}


def _tidy(node: Any, root: bool = False) -> Any:
    if isinstance(node, list):
        return [_tidy(x) for x in node]
    if not isinstance(node, Mapping):
        return node
    out: dict[str, Any] = {}
    any_of = node.get("anyOf")
    if isinstance(any_of, list):
        rest = [x for x in any_of if not (isinstance(x, Mapping) and x.get("type") == "null")]
        if len(rest) == 1 and len(rest) < len(any_of):
            # Optional[X]: X (an absent field, as zod writes .optional()).
            base = dict(_tidy(rest[0]))
            for k, v in node.items():
                if k not in ("anyOf",) and k not in _DROP:
                    base[k] = _tidy(v)
            return base
    for k, v in node.items():
        if k in _DROP or (root and k == "description"):
            continue
        if k == "properties" and isinstance(v, Mapping):
            out[k] = {name: _tidy(p) for name, p in v.items()}
        else:
            out[k] = _tidy(v)
    return out


def _sort_required(node: Any) -> Any:
    """node with every required list in name order, whatever order the fields
    were defined in, as the other SDKs list them."""
    if isinstance(node, list):
        return [_sort_required(x) for x in node]
    if not isinstance(node, Mapping):
        return node
    out = {k: _sort_required(v) for k, v in node.items()}
    if isinstance(out.get("required"), list) and all(isinstance(r, str) for r in out["required"]):
        out["required"] = sorted(out["required"])
    return out


def json_schema(schema: Schema) -> dict[str, Any]:
    """A pydantic model, or a JSON Schema object, as the JSON Schema Wakeshift
    keeps (2020-12, without $schema)."""
    if isinstance(schema, type) and issubclass(schema, BaseModel):
        raw = schema.model_json_schema(by_alias=True)
        return _sort_required(_tidy(_deref(raw, raw.get("$defs", {})), root=True))
    if isinstance(schema, Mapping):
        out = dict(schema)
        out.pop("$schema", None)
        return _sort_required(out)
    raise usage("config", "a schema is a pydantic model class or a JSON Schema dict")


_JSON_TYPES: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "integer": (int,),
    "number": (int, float),
    "boolean": (bool,),
    "array": (list, tuple),
    "object": (dict,),
}


def _check_type(name: str, value: Any, prop: Mapping[str, Any]) -> None:
    t = prop.get("type")
    if not isinstance(t, str) or t not in _JSON_TYPES:
        return
    ok = isinstance(value, _JSON_TYPES[t])
    if t in ("integer", "number") and isinstance(value, bool):
        ok = False
    if t == "integer" and isinstance(value, float):
        ok = value.is_integer()
    if not ok:
        raise _Invalid(f"{name}: expected {t}", [{"message": f"expected {t}", "path": [name]}])
    enum = prop.get("enum")
    if isinstance(enum, list) and value not in enum:
        what = f"expected one of {', '.join(map(str, enum))}"
        raise _Invalid(f"{name}: {what}", [{"message": what, "path": [name]}])


class _Shape:
    """One of an event's two schemas, and how to check a value against it."""

    def __init__(self, schema: Schema) -> None:
        self.model: type[BaseModel] | None = schema if isinstance(schema, type) and issubclass(schema, BaseModel) else None
        self.json = json_schema(schema)

    def parse(self, value: Any) -> tuple[Any, dict[str, Any]]:
        """value checked: the form text and authorize get (the model, or a dict),
        and the JSON that goes to Wakeshift (unknown fields dropped)."""
        if value is None:
            value = {}
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="json", by_alias=True, exclude_unset=True)
        if not isinstance(value, Mapping):
            raise _Invalid("expected an object")
        if self.model is not None:
            try:
                inst = self.model.model_validate(dict(value))
            except ValidationError as err:
                errs = err.errors()
                raise _Invalid(
                    "; ".join((".".join(str(p) for p in e["loc"]) + ": " if e["loc"] else "") + e["msg"] for e in errs),
                    [{"message": e["msg"], "path": list(e["loc"])} for e in errs],
                ) from None
            data = inst.model_dump(mode="json", by_alias=True)
            # An optional field the caller left out stays out (as zod leaves it).
            for name, f in type(inst).model_fields.items():
                key = f.alias or name
                if name not in inst.model_fields_set and data.get(key) is None:
                    data.pop(key, None)
            return inst, data
        props = self.json.get("properties") or {}
        out: dict[str, Any] = {}
        for req in self.json.get("required") or []:
            if req not in value:
                raise _Invalid(f"{req}: required", [{"message": "required", "path": [req]}])
        for k, prop in props.items():
            if k in value:
                _check_type(k, value[k], prop if isinstance(prop, Mapping) else {})
                out[k] = value[k]
        return dict(out), out


# ── Events ──────────────────────────────────────────────────────────────────

#: The most subscriptions one status_of reads.
_MAX_STATUS = 1000


@dataclass(frozen=True)
class EventStatus:
    """Who is subscribed to an event with some arguments right now (event.status)."""

    #: Conversations subscribed (ended or expired subscriptions don't count).
    subscriptions: int
    #: Of those, connected now.
    connected: int
    #: How many are connected each way: stream, poll, hooks, webhook.
    via: dict[str, int]


@dataclass(frozen=True)
class SubscriptionStatus:
    """One subscription right now (event.status_of)."""

    id: str
    user: str
    #: The subscription is the user's, to this event, and hasn't ended. One that names nothing is False too.
    live: bool
    #: Its conversation is reachable now, and via says how.
    connected: bool
    via: str | None = None


#: An event's arguments and payload: pydantic models, or dicts for an event
#: defined with JSON Schema.
A = TypeVar("A", default=dict[str, Any])
P = TypeVar("P", default=dict[str, Any])


class WakeEvent(Generic[A, P]):
    """One of the app's event types (wake.define_event): its arguments (what
    a subscriber picks, A) and each occurrence's payload (P). emit,
    subscribe and create_publish_url take a model instance or a mapping."""

    name: str

    def __init__(
        self,
        wake: Wakeshift,
        name: str,
        *,
        description: str,
        args: Schema,
        payload: Schema,
        text: Callable[[Any, Any], str] | None = None,
        authorize: Callable[[Subscriber, Any], bool | Awaitable[bool]] | None = None,
        personal: bool = False,
        public: bool = False,
        delete_on_delivery: bool = False,
        title: str | None = None,
        topic: str | None = None,
        filter: Mapping[str, str] | None = None,
        on_subscribe: Callable[[Subscribed[Any]], Any] | None = None,
    ) -> None:
        if not _NAME_RE.match(name) or len(name) > 64:
            raise usage(
                "config", f"event name {_compact(name)}: lowercase dot-separated words (chat.message), at most 64 characters"
            )
        if name.startswith("wakeshift."):
            raise usage("config", "event names starting with wakeshift. are Wakeshift's")
        if not (description or "").strip():
            raise usage("config", f"event {name} needs a description")
        if text is not None and not callable(text):
            raise usage("config", f"event {name}: text is a function, (payload, args) -> str")
        self._wake = wake
        self.name = name
        self.description = description
        self.title = title
        if not isinstance(public, bool):
            raise usage("config", f"event {name}: public is True or False")
        if public and (personal or authorize is not None):
            raise usage(
                "config",
                f"event {name}: public can't be combined with personal or authorize (each alone says who may subscribe)",
            )
        self.personal = bool(personal)
        #: Anyone may subscribe, on every path (a native client needs a user).
        self.public = public
        self.delete_on_delivery = bool(delete_on_delivery)
        if on_subscribe is not None and not callable(on_subscribe):
            raise usage("config", f"event {name}: on_subscribe is a function of a Subscribed")
        self._text = text
        self._authorize = authorize
        self._on_subscribe = on_subscribe
        self._args = _Shape(args)
        self._payload = _Shape(payload)
        a, p = self._args.json, self._payload.json
        if a.get("type") != "object":
            raise usage("config", f"event {name}: args is an object schema")
        if p.get("type") != "object":
            raise usage("config", f"event {name}: payload is an object schema")
        props: Mapping[str, Any] = a.get("properties") or {}
        if VOUCHER_ARG in props:
            # A native client writes a subscribe voucher there (docs/subscribe-vouchers.md §4.4).
            raise usage(
                "config",
                f"event {name}: {VOUCHER_ARG} is Wakeshift's: a native client passes a subscribe voucher in it. "
                "Name the argument something else",
            )
        required = set(a.get("required") or [])
        payload_props: Mapping[str, Any] = p.get("properties") or {}
        flt: dict[str, str] | None = dict(filter) if filter is not None else None
        if topic is None:
            # ev/<name>/{a}/{b}: one segment per required scalar argument; an
            # optional one filters on the payload field of its name.
            segs = ["ev", name]
            flt = flt or {}
            for arg in sorted(props):
                if not _ARG_NAME_RE.match(arg):
                    raise usage("config", f"event {name}: argument {_compact(arg)}: lowercase letters, digits and _")
                t = (props.get(arg) or {}).get("type")
                scalar = t in ("string", "integer", "number", "boolean")
                if arg in required:
                    if not scalar or t == "number":
                        raise usage(
                            "config",
                            f"event {name}: argument {arg} routes occurrences, so it's a string, integer or boolean (or pass topic)",
                        )
                    segs.append("{" + arg + "}")
                elif scalar and arg in payload_props:
                    # Both on the same field is refused: neither can quietly win.
                    if filter is not None and arg in filter:
                        raise usage(
                            "config",
                            f"event {name}: filter on {arg} conflicts with optional argument {arg}, "
                            "which already filters that payload field (remove it from filter, or pass topic and filter)",
                        )
                    flt[arg] = "{" + arg + "}"
                else:
                    raise usage(
                        "config",
                        f"event {name}: optional argument {arg} filters on the payload field of its name, "
                        "which the payload doesn't have (or pass topic and filter)",
                    )
            topic = "/".join(segs)
            if not flt:
                flt = None
        self.manifest: dict[str, Any] = {"name": name, "description": description, "arguments": a, "payload": p, "topic": topic}
        if flt:
            self.manifest["filter"] = flt
        if self.personal:
            self.manifest["personal"] = True
        # Who may subscribe, which the core enforces too: only the app's own
        # tools, unless the event is personal, has authorize, or is public.
        self.manifest["subscribe"] = "tools" if self.access == "tools" else "any"
        if self.delete_on_delivery:
            # Wakeshift deletes an occurrence's content (text and payload) once
            # it's delivered, keeping only what shows it happened.
            self.manifest["delete_on_delivery"] = True

    @property
    def access(self) -> str:
        """Who may subscribe: tools (only the app's own tools, the default),
        personal, authorize or public."""
        if self.personal:
            return "personal"
        if self._authorize is not None:
            return "authorize"
        return "public" if self.public else "tools"

    @property
    def authorize(self) -> Callable[[Subscriber, Any], bool | Awaitable[bool]] | None:
        """The event's authorize, as define_event was given it (None without
        one): call it in a unit test with a ``Subscriber`` and the arguments.
        Wakeshift calls it with the arguments parsed (the args model, or a
        dict), refuses on a raise or a run over 10 seconds, and awaits it if
        it's async."""
        return self._authorize

    @property
    def on_subscribe(self) -> Callable[[Subscribed[Any]], Any] | None:
        """The event's on_subscribe, as define_event was given it (None without one)."""
        return self._on_subscribe

    def watch(
        self,
        args: A | Mapping[str, Any],
        check: Callable[[], P | Mapping[str, Any] | None | Awaitable[P | Mapping[str, Any] | None]],
        *,
        every: timedelta | int | float | str | None = None,
        timeout: timedelta | int | float | str | None = None,
        max_errors: int = 5,
        is_final: Callable[[BaseException], bool] | None = None,
        id: str | None = None,
        to: str | Recipient | Sequence[str | Recipient] | None = None,
    ) -> asyncio.Task[WatchResult]:
        """Calls ``check`` until it returns the payload (``None``: keep
        waiting), then emits this event once with ``args``: for a service
        with no usable webhook. ``check`` may be async, or a plain function
        (run in a worker thread). Returns the watch's task: awaiting it gives
        a ``WatchResult``, and it never raises (except ``CancelledError``, if
        you cancel the task itself).

        The first check runs at once, then one every ``every`` (a timedelta,
        seconds or "30s"; default 30 seconds, at least 1), until ``timeout``
        (default 1 hour), which covers the checks and the emit. A check that
        raises is retried with backoff (every × 2ⁿ, at most 5 minutes), and
        the watch fails after ``max_errors`` in a row (default 5), or at once
        on an error ``is_final(err)`` marks final (a 404, say). The emit
        carries ``id`` (default: one per watch), the same on each retry, so
        passing your webhook handler's id for the same occurrence makes the
        two emit once; it's retried only while retrying can help (no answer,
        429, 5xx). ``to``: a personal event's recipients, as emit's.

        A watch of the same event and arguments while one runs returns that
        one's task. At most 10 checks run at once per client; others wait
        their turn. On a disabled client nothing runs (``skipped``); in a
        tool call whose subscription to these arguments came back
        unavailable, nothing runs; if that call fails and its subscription is
        rolled back, a watch it started stops. ``wake.aclose()`` stops every
        watch. The watch lives in this process only: a restart or a
        serverless freeze loses it."""
        from ._watch import start

        return start(self, args, check, every=every, timeout=timeout, max_errors=max_errors, is_final=is_final, id=id, to=to)

    def _default_text(self, args: Mapping[str, Any]) -> str:
        """The line an agent reads for an event without text: its description
        and its arguments, "A crawl finished (job 3f2a)"."""
        return _default_text(self.description, args)

    def _parse_args(self, args: Any) -> tuple[Any, dict[str, Any]]:
        """A subscriber's arguments, checked."""
        try:
            return self._args.parse(args)
        except _Invalid as err:
            raise usage("bad_arguments", f"arguments for {self.name}: {err.what}", issues=err.issues) from None

    async def _allows(self, subscriber: Subscriber, args: Any) -> bool:
        """Whether this subscriber may subscribe with these arguments."""
        if self._authorize is None:
            return True
        try:
            r = self._authorize(subscriber, args)
            if inspect.isawaitable(r):
                r = await asyncio.wait_for(r, LIMITS["authorize"])
            return bool(r)
        except Exception as err:
            # An authorize that fails refuses, the same way on both paths; the
            # app hears the error's type, never its message (it may carry a token).
            self._wake._log(
                "authorize_failed",
                f"authorize for {self.name} raised {type(err).__name__}, so the subscription was refused",
                event=self.name,
                error_type=type(err).__name__,
            )
            return False

    def _definition(self, voucher_app: str | None = None) -> dict[str, Any]:
        """The events/list entry. With voucher_app (a tools-only event listed
        to a native request while the core hands out subscribe vouchers), its
        arguments gain the required wakeshift_voucher, described with the
        app's name."""
        schema: dict[str, Any] = self.manifest["arguments"]
        if voucher_app is not None:
            props = dict(schema.get("properties") or {})
            props[VOUCHER_ARG] = {"type": "string", "description": fill("voucher_argument", app=voucher_app or "this app")}
            schema = {**schema, "properties": props, "required": sorted([*(schema.get("required") or []), VOUCHER_ARG])}
        d: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            # A tools-only event is subscribed to only with a voucher, and only
            # with a webhook: events/poll of one is refused, voucher or not.
            "delivery": ["webhook"] if voucher_app is not None else list(DELIVERY),
            "inputSchema": schema,
            "payloadSchema": self.manifest["payload"],
        }
        if self.title:
            d["title"] = self.title
        return d

    async def emit(
        self,
        args: A | Mapping[str, Any],
        payload: P | Mapping[str, Any],
        *,
        to: str | Recipient | Sequence[str | Recipient] | None = None,
        id: str | None = None,
    ) -> Emitted:
        """Sends one occurrence: to everyone subscribed with these arguments,
        or, for a personal event, to its recipients (``to``): a user (every
        subscription they have to it with these arguments),
        ``Recipient(user=u, subscription=id)`` (exactly that conversation's,
        the id call.subscribe returned) or ``Recipient(native=u)`` (only the
        user's own MCP Events subscriptions), one or a list, at most 1,000.
        The payload is checked against the event's schema first. ``id`` makes
        retries emit once for a day: a retry of the same emit (arguments,
        recipients and payload) returns the first's result with
        ``duplicate=True``; the id on a different emit raises IdReusedError
        (``id_reused``) and sends nothing."""
        a_obj, a_json = self._parse_args(args)
        try:
            p_obj, p_json = self._payload.parse(payload)
        except _Invalid as err:
            raise usage("bad_arguments", f"payload for {self.name}: {err.what}", issues=err.issues) from None
        recipients = _recipients(self.name, to)
        if self.personal and not recipients:
            raise usage("bad_arguments", f"{self.name} is personal: pass to=, the subscribers it's for")
        if not self.personal and recipients:
            raise usage(
                "bad_arguments",
                f"{self.name} isn't personal: it goes to everyone subscribed with these arguments, so it takes no to",
            )
        text = self._text(p_obj, a_obj) if self._text is not None else self._default_text(a_json)
        return await self._wake._emit_event(
            name=self.name, arguments=a_json, payload=p_json, text=text, id=id, to=recipients or None
        )

    async def create_publish_url(
        self, args: A | Mapping[str, Any], *, to: str | None = None, signing_secret: str | None = None
    ) -> PublishURL:
        """A capability URL that emits this event with these arguments (a
        personal event: to the one subscriber `to` names), for services that
        can only send webhooks. A POST's body, a JSON object, is the payload,
        checked against the event's schema; since `text` doesn't run, the agent
        reads the event's default text and the payload whole. Anyone with the
        URL can emit until wake.revoke_publish_url(id), unless it has a
        signing_secret: the sender's Standard Webhooks secret (whsec_…, a Svix
        endpoint's), and then the URL takes only posts signed with it. The
        secret is sent to Wakeshift once and never returned: the result's
        signing has its fingerprint."""
        _, a_json = self._parse_args(args)
        if to is not None and not isinstance(to, str):
            raise usage("bad_arguments", f"{self.name}: a publish URL's to is exactly one user")
        who = (to or "").strip()
        if self.personal and not who:
            raise usage("bad_arguments", f"{self.name} is personal: pass to=, the one subscriber it's for")
        if not self.personal and who:
            raise usage(
                "bad_arguments",
                f"{self.name} isn't personal: it goes to everyone subscribed with these arguments, so it takes no to",
            )
        if signing_secret is not None and (not isinstance(signing_secret, str) or not signing_secret.strip()):
            raise usage("bad_signing_secret", f"{self.name}: a publish URL's signing_secret is empty")
        body: dict[str, Any] = {"name": self.name, "arguments": a_json}
        if who:
            body["to"] = who
        if signing_secret is not None:
            body["signing_secret"] = signing_secret.strip()
        out = await self._wake._request_app("POST", "/events/publish-urls", body, about_event=True)
        sig = out.get("signing")
        signing = (
            PublishURLSigning(
                fingerprint=sig.get("fingerprint", ""),
                set_at=sig.get("set_at", ""),
                previous_valid_until=sig.get("previous_valid_until"),
            )
            if isinstance(sig, dict)
            else None
        )
        return PublishURL(id=out["id"], url=out["url"], event=out.get("event", ""), signing=signing)

    async def status(self, args: A | Mapping[str, Any], *, to: str | None = None) -> EventStatus:
        """How many conversations are subscribed to this event with these
        arguments (a personal event: those of the one user ``to`` names), how
        many are connected now, and how. Only reads."""
        _, a_json = self._parse_args(args)
        if to is not None and not isinstance(to, str):
            raise usage("bad_arguments", f"{self.name}: status's to is exactly one user")
        who = (to or "").strip()
        if self.personal and not who:
            raise usage("bad_arguments", f"{self.name} is personal: pass to=, the one user to count for")
        if not self.personal and who:
            raise usage("bad_arguments", f"{self.name} isn't personal: status takes no to")
        body: dict[str, Any] = {"name": self.name, "arguments": a_json}
        if who:
            body["to"] = who
        out = await self._wake._request_app("POST", "/events/status", body, about_event=True)
        via = {"stream": 0, "poll": 0, "hooks": 0, "webhook": 0}
        via.update(out.get("via") or {})
        return EventStatus(subscriptions=int(out.get("subscriptions", 0)), connected=int(out.get("connected", 0)), via=via)

    async def status_of(self, subs: Sequence[Recipient]) -> list[SubscriptionStatus]:
        """Reads subscriptions to this event by ID, each a
        ``Recipient(user=u, subscription=id)`` for the user call.subscribe made
        it for, up to 1,000 at once: whether each is live and its conversation
        connected now, and how. Only reads."""
        if not subs:
            return []
        if len(subs) > _MAX_STATUS:
            raise usage("bad_request", f"{self.name}: at most {_MAX_STATUS} subscriptions in one status_of")
        refs: list[dict[str, str]] = []
        for s in subs:
            user = (s.user or "").strip() if isinstance(s, Recipient) else ""
            if not user or not isinstance(s.subscription, str) or not s.subscription.strip():
                raise usage("bad_request", f"{self.name}: status_of needs each subscription's user and subscription")
            refs.append({"user": user, "id": s.subscription.strip()})
        out = await self._wake._request_app(
            "POST", "/events/status", {"name": self.name, "subscriptions": refs}, about_event=True
        )
        return [
            SubscriptionStatus(
                id=r.get("id", ""),
                user=r.get("user", ""),
                live=bool(r.get("live")),
                connected=bool(r.get("connected")),
                via=r.get("via") or None,
            )
            for r in out.get("subscriptions") or []
        ]

    async def end(self, user: str, id: str) -> bool:
        """Ends one of user's subscriptions to this app's events, by its ID. False if there was none."""
        return bool((await self._wake._request_app("POST", "/event-subscriptions/end", {"user": user, "id": id})).get("ended"))


def _recipients(event: str, to: Any) -> list[Recipient]:
    """An emit's to as Recipients, checked: each names a user (and a
    subscription names its user too), with the whitespace around each name
    trimmed, as every SDK does (emit-recipients.json)."""
    if to is None:
        return []
    kinds = f"{event}: a recipient is a user's ID, Recipient(user=…, subscription=…) or Recipient(native=…)"
    if isinstance(to, str | Recipient):
        items = [to]
    elif isinstance(to, Sequence):
        items = list(to)
    else:
        raise usage("bad_arguments", kinds)

    def empty(what: str) -> UsageError:
        return usage("bad_arguments", f"{event}: a recipient's {what} is empty")

    out: list[Recipient] = []
    for r in items:
        if isinstance(r, str):
            r = Recipient(user=r)
        if not isinstance(r, Recipient):
            raise usage("bad_arguments", kinds)
        if r.native is not None:
            if r.user is not None or r.subscription is not None or not isinstance(r.native, str):
                raise usage("bad_arguments", kinds)
            if not r.native.strip():
                raise empty("user")
            r = Recipient(native=r.native.strip())
        else:
            if not isinstance(r.user, str | None) or not isinstance(r.subscription, str | None):
                raise usage("bad_arguments", kinds)
            if not (r.user or "").strip():
                raise empty("user")
            if r.subscription is not None and not r.subscription.strip():
                raise empty("subscription")
            r = Recipient(user=(r.user or "").strip(), subscription=None if r.subscription is None else r.subscription.strip())
        out.append(r)
    return out


# ── Serving ─────────────────────────────────────────────────────────────────


_detail = what_of


def _fail(text: str) -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=text)], is_error=True)


#: What attach's own tools and the native methods answer when identify failed
#: (or require_user refused an anonymous caller): Wakeshift's part refuses.
#: The same words in every SDK (TypeScript's refusedText): for the agent, never the developer's fix.
_REFUSED = {
    "anonymous": "this app needs you to sign in before it can subscribe you to anything",
    "failed": "this app couldn't verify who you are, so it can't subscribe you to anything now",
}


def _refused_text(reason: str, sentence: bool = False) -> str:
    """What a refused caller is told: by a built-in tool (sentence) or a native method."""
    t = _REFUSED.get(reason, _REFUSED["failed"])
    return t[0].upper() + t[1:] + "." if sentence else t


def _ok(text: str, structured: dict[str, Any], addition: _Addition | None = None) -> CallToolResult:
    """One of attach's own tools' results: its text, then Wakeshift's, with
    Wakeshift's _meta, and structured (TOOL_OUTPUT_SCHEMAS) with Wakeshift's
    text in its wakeshift field."""
    a = addition or _Addition()
    out = _apply_addition(a, text)
    add = (a.text or "").strip()
    out.structured_content = {**structured, "wakeshift": add} if add else dict(structured)
    return out


def _rfc3339(s: str) -> str:
    """A time as Date.toISOString() writes it, without milliseconds."""
    from ._client import _parse_time

    t = _parse_time(s)
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _poll_result(out: Mapping[str, Any]) -> dict[str, Any]:
    """An events/poll result: what Wakeshift answered, in the draft's names
    (tests/golden/mcp-events-wire.json)."""
    return {
        "resultType": RESULT_TYPE,
        "events": list(out.get("events") or []),
        "cursor": out.get("cursor"),
        "truncated": bool(out.get("truncated")),
        "hasMore": bool(out.get("has_more")),
        "nextPollMs": out.get("next_poll_ms"),
    }


def _rpc_error_of(err: BaseException, codes: Codes, event: str = "") -> Exception:
    """The JSON-RPC error for a failed native call, in the client's dialect's
    codes. A conversation that isn't the caller's is Forbidden, whether it
    exists or not."""
    from mcp.shared.exceptions import MCPError

    if isinstance(err, MCPError):
        return err
    if not isinstance(err, APIError) or isinstance(err, APIConnectionError):
        return MCPError(INTERNAL_ERROR, "Wakeshift couldn't be reached")
    c = err.code
    if c == "bad_voucher":
        # One answer, whatever the reason (docs/subscribe-vouchers.md §6.2).
        return MCPError(codes.forbidden, fill("native_bad_voucher", event=event or "this event"))
    if c == "callback_failed":
        return MCPError(codes.callback_endpoint, err.detail, {"reason": err.reason or ""})
    if c == "unknown_event":
        return MCPError(codes.not_found, err.detail, {"kind": "event"})
    if c == "unsupported":
        return MCPError(codes.unsupported, err.detail, {"feature": "deliveryMode"})
    if c == "subscription_limit":
        return MCPError(codes.resource_exhausted, err.detail, {"limit": "subscriptions"})
    if c == "subscription_stopped":
        return MCPError(codes.forbidden, err.detail, {"reason": "stopped"})
    if c == "tools_only":
        return MCPError(codes.forbidden, fill("native_tools_only", event=event or "this event"))
    if c in ("bad_request", "bad_arguments"):
        return MCPError(INVALID_PARAMS, err.detail)
    if c in ("unknown_session", "other_client", "unknown_conversation"):
        return MCPError(codes.forbidden, "another client of yours holds that subscription")
    return MCPError(INTERNAL_ERROR, "Wakeshift refused the subscription: " + err.detail)


async def subscribe_native(
    wake: Wakeshift, ev: WakeEvent[Any, Any], a_obj: Any, who: str | None, body: dict[str, Any]
) -> dict[str, Any]:
    """events/subscribe as Wakeshift takes it (body: call, name, arguments,
    delivery, dialect, and a subscribe voucher if there is one), then the
    event's on_subscribe for a new subscription. A redeemed voucher's
    subscription is the handing-out tool's, and gets the context its call
    passed, if this process handed it out; it's forgotten only once
    Wakeshift created the subscription."""
    from ._watch import fire

    out = await wake._request_app("POST", "/events/subscribe", body)
    if out.get("created"):
        via = out.get("via")
        tool = None
        if isinstance(via, str) and via.startswith("tool:"):
            tool = out.get("tool") if isinstance(out.get("tool"), str) and out.get("tool") else via[len("tool:") :]
        voucher = body.get("voucher")
        if tool:
            context = wake._take_voucher_context(voucher) if isinstance(voucher, str) else None
            fire(wake, ev, a_obj, who, "tool", tool, context=context)
        else:
            fire(wake, ev, a_obj, who, "native")
    return out


def attach_events(
    wake: Wakeshift,
    server: Any,
    events: list[WakeEvent],
    *,
    identify: Any = None,
    require_user: bool = False,
    is_native_client: Callable[[Request], bool] | None = None,
    builtin_tools: BuiltinTools | Literal[False] | None = None,
    wrap: Mapping[str, Sequence[WakeEvent]] | Callable[[str], Sequence[WakeEvent] | None] | None = None,
    host: Any = None,
) -> Attachment:
    """Wakeshift.attach on an MCPServer (mcp.server.mcpserver.MCPServer), a
    low-level Server or a FastMCP server: Wakeshift's middleware on every
    request, attach's own tools and the native events/* methods (not on
    FastMCP 2/3, whose mcp 1.x has none). Returns the handle tools that
    subscribe register with."""
    from mcp_types import RequestParams

    from ._hosts import host_of
    from ._toolcall import Attachment
    from ._watch import fire

    if not events:
        wake._log("no_events", "attach found no events, so it serves none")
    wanted, wait = builtin_choice(builtin_tools)
    host = host or host_of(server)
    if host.native_events:
        from mcp.shared.exceptions import MCPError
    elif wait:
        raise usage(
            "config",
            f"attach's builtin_tools wait needs MCP tasks, which {host.framework} (on mcp 1.x) doesn't serve: "
            "leave it off, or upgrade to FastMCP 4",
        )
    low = host.lowlevel
    attached = Attachment(wake, host, identify, require_user=require_user, is_native_client=is_native_client)
    attached._wait = wait
    attached._any_open = any(e.access != "tools" for e in events)
    attached._set_wrap(wrap)

    async def caller_of(ctx: Any) -> tuple[str | None, str | None, str | None]:
        # Who's calling: the user, the client's name identify gave, and why
        # Wakeshift's part of the request is refused, if it is.
        who, refused = await attached._who(ctx)
        return who.user, who.client, refused

    by_name = {e.name: e for e in events}

    def ctx_of(c: Any) -> Any:
        # The low-level request context, whichever form the handler got.
        return getattr(c, "request_context", c)

    # ── Native: events/list, events/subscribe, events/unsubscribe ─────────

    class _Params(RequestParams):
        model_config = ConfigDict(extra="allow")

    async def native_request(ctx: Any) -> bool:
        # Whether a request comes from a client that subscribes itself: the
        # test call.subscribe uses (Attachment._native_of).
        rc = ctx_of(ctx)
        client, _, _ = _request_info(rc)
        _, identified, _ = await caller_of(rc)
        return attached._native_of(rc, rc, client or identified or "")[0]

    async def events_list(ctx: Any, _params: Any) -> dict[str, Any]:
        # The events a native client may subscribe to, and, to a native
        # request while the core hands out subscribe vouchers, the tools-only
        # ones with a required wakeshift_voucher (docs/subscribe-vouchers.md §4.3).
        app: str | None = None
        if any(e.access == "tools" for e in events) and await native_request(ctx) and await wake._vouchers_offered():
            app = await wake._app_name()
        if app is None:
            return {"resultType": RESULT_TYPE, "events": [e._definition() for e in events if e.access != "tools"]}
        return {"resultType": RESULT_TYPE, "events": [e._definition(app if e.access == "tools" else None) for e in events]}

    async def events_stream(ctx: Any, _params: Any) -> dict[str, Any]:
        client, _, headers = _request_info(ctx)
        codes = codes_for(dialect_of(client, headers.get("user-agent", "") if headers else ""))
        raise MCPError(
            codes.unsupported,
            "push delivery isn't offered: use a webhook (events/subscribe) or poll (events/poll)",
            {"feature": "deliveryMode", "value": "stream"},
        )

    async def native(ctx: Any, method: str) -> dict[str, Any]:
        unsubscribe = method == "unsubscribe"
        p: dict[str, Any] = dict(ctx.params or {})
        # The voucher leaves the arguments before anything reads them (parsing,
        # authorize, on_subscribe, logs, the subscription key).
        p["arguments"], offered = _take_voucher(p.get("arguments"))
        client, meta, headers = _request_info(ctx)
        who, identified, refused = await caller_of(ctx_of(ctx))
        client = client or identified or ""
        ua = headers.get("user-agent", "") if headers else ""
        dialect = dialect_of(client, ua)
        codes = codes_for(dialect)
        name_header = headers.get(NAME_HEADER, "") if headers else ""
        if name_header and header_value(name_header) != p.get("name"):
            raise MCPError(HEADER_MISMATCH, "Header mismatch: Mcp-Name doesn't match params.name")
        name = p.get("name")
        ev = by_name.get(name) if isinstance(name, str) else None
        if ev is None:
            raise MCPError(codes.not_found, fill("native_unknown_event", event=_compact(p.get("name"))), {"kind": "event"})
        # Only the app's tools subscribe to it: every poll is refused, whatever
        # cursor it carries, and every subscribe but one with the subscribe
        # voucher a tool handed out, while the core hands them out. An open
        # event ignores a voucher.
        voucher: str | None = None
        if ev.access == "tools" and method != "poll" and offered is not None and await wake._vouchers_offered():
            voucher = offered
        if not unsubscribe and ev.access == "tools" and voucher is None:
            # A poll is never offered for one, voucher or not: the text names the webhook.
            raise MCPError(
                codes.forbidden, fill("native_tools_only_poll" if method == "poll" else "native_tools_only", event=ev.name)
            )
        raw_delivery = p.get("delivery")
        delivery: Mapping[str, Any] = raw_delivery if isinstance(raw_delivery, Mapping) else {}
        if method == "subscribe" and delivery.get("mode") != "webhook":
            raise MCPError(
                codes.unsupported,
                'delivery.mode must be "webhook": the only delivery offered',
                {"feature": "deliveryMode", "value": delivery.get("mode")},
            )
        try:
            a_obj, a_json = ev._parse_args(p.get("arguments") or {})
        except UsageError as err:
            raise MCPError(INVALID_PARAMS, what_of(err)) from None
        if refused:
            if refused == "anonymous":
                wake._log("user_required", f"require_user refused this {method} of {ev.name}: identify gave no user")
            raise MCPError(codes.forbidden, _refused_text(refused))
        # With a voucher, no user is needed: the voucher is the authorization,
        # and Wakeshift compares the user when it was handed to one.
        if not who and voucher is None:
            raise MCPError(codes.forbidden, fill("native_no_user"))
        call: dict[str, Any] = {"client": client, "meta": meta}
        if who:
            call["user"] = who
        if ua:
            call["user_agent"] = ua
        ts = _transport_session(headers) if voucher is not None else None
        if ts:
            call["transport_session"] = ts
        # A poll has no subscribe step (each poll can start the subscription),
        # so every poll asks authorize: access is checked again each time.
        request = attached._request(ctx_of(ctx))
        if not unsubscribe and not await ev._allows(Subscriber(user=who, client=client, native=True, request=request), a_obj):
            raise MCPError(codes.forbidden, fill("native_refused", event=ev.name))
        if method == "poll":
            poll: dict[str, Any] = {"call": call, "name": ev.name, "arguments": a_json}
            for k, wire in (("cursor", "cursor"), ("maxAgeMs", "max_age_ms"), ("maxEvents", "max_events")):
                if p.get(k) is not None:
                    poll[wire] = p[k]
            try:
                polled = await wake._request_app("POST", "/events/poll", poll)
            except Exception as err:
                raise _rpc_error_of(err, codes, ev.name) from err
            if polled.get("created"):
                fire(wake, ev, a_obj, who, "native")  # this poll started the subscription
            return _poll_result(polled)
        body: dict[str, Any] = {
            "call": call,
            "name": ev.name,
            "arguments": a_json,
            "delivery": dict(delivery),
            "dialect": dialect,
        }
        if voucher is not None:
            body["voucher"] = voucher
        try:
            if unsubscribe:
                out = await wake._request_app("POST", "/events/unsubscribe", body)
                if not out.get("ended") and dialect != "chatgpt":
                    raise MCPError(
                        codes.not_found,
                        "no subscription matches that event, arguments and delivery URL",
                        {"kind": "subscription"},
                    )
                return {"resultType": RESULT_TYPE}
            # Absent means null (the draft): none of them goes on as null.
            for k, wire in (("ttlMs", "ttl_ms"), ("cursor", "cursor"), ("maxAgeMs", "max_age_ms")):
                if p.get(k) is not None:
                    body[wire] = p[k]
            out = await subscribe_native(wake, ev, a_obj, who, body)
            res: dict[str, Any] = {
                "resultType": RESULT_TYPE,
                "id": out["id"],
                "refreshBefore": _rfc3339(out["refresh_before"]),
                "cursor": out.get("cursor"),
                "truncated": bool(out.get("truncated")),
            }
            if out.get("delivery_status"):
                res["deliveryStatus"] = out["delivery_status"]
            return res
        except Exception as err:
            raise _rpc_error_of(err, codes, ev.name) from err

    async def events_subscribe(ctx: Any, _params: Any) -> dict[str, Any]:
        return await native(ctx, "subscribe")

    async def events_unsubscribe(ctx: Any, _params: Any) -> dict[str, Any]:
        return await native(ctx, "unsubscribe")

    async def events_poll(ctx: Any, _params: Any) -> dict[str, Any]:
        return await native(ctx, "poll")

    handlers = getattr(low, "_request_handlers", {})
    if not host.native_events:
        # mcp 1.x (FastMCP 2/3): no events/* methods, so no client subscribes
        # natively; every client gets the built-in tools and the block.
        attached._serves_events = False
    elif any(m.startswith("events/") for m in handlers) or EXTENSION_KEY in (low.extensions or {}):
        # The server serves MCP Events of its own: Wakeshift serves none, and
        # never overwrites its capability (#70).
        attached._serves_events = False
        wake._log("events_capability_kept", "this server already declares MCP Events, so Wakeshift serves none of its own")
    elif events:
        low.add_request_handler("events/list", _Params, events_list)
        low.add_request_handler("events/subscribe", _Params, events_subscribe)
        low.add_request_handler("events/unsubscribe", _Params, events_unsubscribe)
        low.add_request_handler("events/poll", _Params, events_poll)
        low.add_request_handler("events/stream", _Params, events_stream)
        # Advertised whenever the server has an event: one open natively
        # (personal, authorize or public), or a tools-only one, which a native
        # client subscribes to with the subscribe voucher a tool handed it,
        # while the core hands them out (docs/subscribe-vouchers.md §4.3). With
        # an old core, events/list lists only open events.
        attached._advertises = True
        low.extensions[EXTENSION_KEY] = {"listChanged": False}

    # ── Bridged: subscribe_events, unsubscribe_events, list_subscriptions ─

    names = [e.name for e in events]
    # subscribe_events offers only the events open to it; unsubscribing
    # stays allowed for every event.
    opened = [e for e in events if e.access != "tools"]
    listed = "\n".join(
        fill(
            "event_line",
            name=e.name,
            arguments=", ".join(sorted((e.manifest["arguments"].get("properties") or {}).keys())),
            description=e.description,
        )
        for e in opened[:_MAX_LISTED_EVENTS]
    )
    more = fill("more", n=len(opened) - _MAX_LISTED_EVENTS) if len(opened) > _MAX_LISTED_EVENTS else ""
    event_prop = {"type": "string", "description": TEXT["event_argument"], "enum": names}
    open_prop = {**event_prop, "enum": [e.name for e in opened]}
    # Any object (events/list has each event's own schema); additionalProperties
    # true, never {}, which some clients read as false (events-list.json).
    args_prop = {"description": TEXT["arguments_argument"], "type": "object", "additionalProperties": True}

    async def subscribe_events(
        event: str | None = None,
        arguments: dict[str, Any] | None = None,
        *,
        wake_args: Mapping[str, Any],
        ctx: Any,
    ) -> CallToolResult:
        ev = by_name.get(event or "")
        if ev is None:
            return _fail(fill("unknown_event", event=event))
        if ev.access == "tools":
            return _fail(fill("tools_only", event=ev.name))
        try:
            a_obj, a_json = ev._parse_args(arguments or {})
        except UsageError as err:
            return _fail(what_of(err))
        rc = ctx_of(ctx)
        who, identified, refused = await caller_of(rc)
        if refused:
            return _fail(_refused_text(refused, sentence=True))
        if ev.personal and not who:
            return _fail(fill("personal_no_user", event=ev.name))
        client, _, _ = _request_info(rc)
        subscriber = Subscriber(user=who, client=client or identified or "", native=False, request=attached._request(rc))
        if not await ev._allows(subscriber, a_obj):
            return _fail(fill("refused", event=ev.name))
        try:
            out = await wake._request_app(
                "POST",
                "/event-subscriptions",
                {
                    "call": _call(rc, user=who, client=identified, args=wake_args, tool="subscribe_events", wait=wait).to_json(),
                    "name": ev.name,
                    "arguments": a_json,
                },
            )
        except Exception as err:
            if isinstance(err, APIError) and err.code == "tools_only":
                return _fail(fill("tools_only", event=ev.name))
            return _fail(fill("subscribe_failed", detail=_detail(err)))
        es = out.get("event_subscription") or {}
        created = bool(es.get("created"))
        if created:
            fire(wake, ev, a_obj, who, "subscribe_events")
        # The arguments as the core keeps them, where it says (CR L16).
        kept = es.get("arguments") if isinstance(es.get("arguments"), dict) else a_json
        return _ok(
            fill("subscribed" if created else "renewed", event=ev.name, arguments=_canonical(kept)),
            {"subscribed": {"event": ev.name, "arguments": kept or {}, "created": created}},
            _Addition.from_json(out.get("addition")),
        )

    async def unsubscribe_events(
        event: str | None = None,
        arguments: dict[str, Any] | None = None,
        id: str | None = None,
        *,
        wake_args: Mapping[str, Any],
        ctx: Any,
    ) -> CallToolResult:
        rc = ctx_of(ctx)
        who, identified, refused = await caller_of(rc)
        if refused:
            return _fail(_refused_text(refused, sentence=True))
        try:
            if id:
                # By user, else by the calling conversation (a server without
                # identify: its session_key names it).
                end: dict[str, Any] = {"user": who, "id": id}
                if not who:
                    end = {
                        "call": _call(
                            rc, user=None, client=identified, args=wake_args, tool="unsubscribe_events", wait=wait
                        ).to_json(),
                        "id": id,
                    }
                out = await wake._request_app("POST", "/event-subscriptions/end", end)
                return _ok(fill("ended" if out.get("ended") else "no_such_id", id=id), {"ended": bool(out.get("ended"))})
            ev = by_name.get(event or "")
            if ev is None:
                return _fail(fill("need"))
            _, a_json = ev._parse_args(arguments or {})
            out = await wake._request_app(
                "POST",
                "/event-subscriptions/unsubscribe",
                {
                    "call": _call(
                        rc, user=who, client=identified, args=wake_args, tool="unsubscribe_events", wait=wait
                    ).to_json(),
                    "name": ev.name,
                    "arguments": a_json,
                },
            )
            # addition: the conversation's session key stays the same.
            return _ok(
                fill("unsubscribed" if out.get("ended") else "not_subscribed", event=ev.name, arguments=_canonical(a_json)),
                {"ended": bool(out.get("ended"))},
                _Addition.from_json(out.get("addition")),
            )
        except UsageError as err:
            return _fail(what_of(err))
        except Exception as err:
            return _fail(fill("unsubscribe_failed", detail=_detail(err)))

    async def list_subscriptions(*, wake_args: Mapping[str, Any], ctx: Any) -> CallToolResult:
        rc = ctx_of(ctx)
        who, identified, refused = await caller_of(rc)
        if refused:
            return _fail(_refused_text(refused, sentence=True))
        # The user's subscriptions, else (no identify, or no user) the calling
        # conversation's, which its session_key names.
        body: dict[str, Any] = {"user": who}
        if not who:
            body = {
                "call": _call(rc, user=None, client=identified, args=wake_args, tool="list_subscriptions", wait=wait).to_json()
            }
        try:
            out = await wake._request_app("POST", "/event-subscriptions/list", body)
        except Exception as err:
            return _fail(fill("list_failed", detail=_detail(err)))
        subs = out.get("subscriptions") or []
        structured = {
            "subscriptions": [
                {
                    "id": s.get("id", ""),
                    "event": s.get("event", ""),
                    "arguments": s.get("arguments") or {},
                    "destination": s.get("destination", ""),
                    "since": _rfc3339(str(s.get("created_at", ""))) if s.get("created_at") else "",
                }
                for s in subs
            ]
        }
        if not subs:
            return _ok(fill("none"), structured)
        lines = []
        for s in subs:
            where = (
                fill("webhook", host=s.get("webhook_host") or TEXT["a_webhook"])
                if s.get("destination") == "webhook"
                else fill("polled")
                if s.get("destination") == "poll"
                else fill("conversation")
            )
            lines.append(
                fill(
                    "item",
                    id=s.get("id"),
                    event=s.get("event"),
                    arguments=_compact(s.get("arguments")),
                    destination=where,
                    since=str(s.get("created_at", ""))[:10],
                )
            )
        return _ok(fill("list", lines="\n".join(lines)), structured)

    own_tools = (
        (
            "subscribe_events",
            subscribe_events,
            f"{TEXT['subscribe_description']}{TEXT['events_heading']}{listed}{more}",
            {"event": open_prop, "arguments": args_prop, **wake_args(True)},
            ["event"],
        ),
        (
            "unsubscribe_events",
            unsubscribe_events,
            TEXT["unsubscribe_description"],
            {
                "event": event_prop,
                "arguments": args_prop,
                "id": {"type": "string", "description": TEXT["id_argument"]},
                **wake_args(False),
            },
            None,
        ),
        ("list_subscriptions", list_subscriptions, TEXT["list_description"], dict(wake_args(False)), None),
    )
    for name, fn, description, properties, required in own_tools if events else ():
        if name not in wanted or (name == "subscribe_events" and not opened):
            continue
        # The advertised input schema is written out: the enum of event names,
        # an object of arguments, and the same descriptions as the other SDKs.
        schema: dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            schema["required"] = required
        attached._builtins[name] = (
            fn,
            {
                "name": name,
                # The annotations' title, at the top level too, as in every SDK.
                "title": TOOL_ANNOTATIONS[name]["title"],
                "description": description,
                "inputSchema": schema,
                "outputSchema": copy.deepcopy(OUTPUT_SCHEMAS[name]),
                # A title and all four hints, as in every SDK (tool-annotations.json).
                "annotations": dict(TOOL_ANNOTATIONS[name]),
            },
        )

    # Clients that take MCP tasks (Claude Code): Wakeshift's wait tool,
    # answered with a task (tasks.py), if the app turns it on (wait).
    attached._task_server = TaskServer(wake, caller_of) if wait else None
    if attached._builtins:
        host.ensure_tool_handlers()
        attached._lists_builtins = host.list_builtins(attached)
    host.install_attachment(attached)
    lookup = getattr(low, "get_tool_input_schema", None)
    if lookup is not None:
        low.get_tool_input_schema = attached._input_schema_lookup(lookup)
    return attached
