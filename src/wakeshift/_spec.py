"""The SEP-3415 (MCP Events) constants the SDK uses, in one place, matching
Wakeshift's own (internal/mcpevents/event/spec.go) and the Node and Go SDKs':
the accepted text may still be reworded before it merges, and its error codes
are provisional."""

from __future__ import annotations

import base64
import binascii
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from ._clients import CLIENT_TABLE

#: The text these follow: SEP-3415 as merged, Final, on 2026-11-19 (8a18b4b). Where
#: clients differ from it, Wakeshift follows each client (webhook-id per client,
#: in the core; a missing Mcp-Name accepted, a mismatched one refused).
REVISION = "SEP-3415 (2026-11-19, 8a18b4b)"

#: The delivery modes Wakeshift offers for every open event type
#: (events/list's delivery): a webhook, or polls; PUSH is added, last, where
#: push is served.
DELIVERY = ("webhook", "poll")

#: The delivery mode events/list adds, last, where push (events/stream) is
#: served: the core reports it, the transport streams its responses, and
#: attach's push isn't False.
PUSH = "push"

#: The _meta key every events/stream notification carries: the request's
#: JSON-RPC ID, as sent.
SUBSCRIPTION_ID_META = "io.modelcontextprotocol/subscriptionId"

#: Capability keys: the draft's extension, and the top-level key ChatGPT reads. A server advertises both.
EXTENSION_KEY = "io.modelcontextprotocol/events"
TOP_LEVEL_KEY = "events"

#: On every events/* result.
RESULT_TYPE = "complete"

#: The _meta key of an occurrence's rendered line (Wakeshift adds it on delivery).
TEXT_META = "ai.wakeshift/text"

#: The Streamable HTTP header events/subscribe, events/unsubscribe, events/poll and events/stream carry: params.name.
NAME_HEADER = "mcp-name"

#: events/stream's answer (Unsupported, with value "push") where push isn't served.
STREAM_REFUSAL = "push delivery isn't offered: use a webhook (events/subscribe) or poll (events/poll)"

INVALID_PARAMS = -32602
HEADER_MISMATCH = -32020
INTERNAL_ERROR = -32603

#: A dialect the client table names (_clients.py): what a client speaks.
Dialect = Literal["spec", "draft", "openclaw", "chatgpt"]


@dataclass(frozen=True)
class Codes:
    not_found: int
    forbidden: int
    resource_exhausted: int
    unsupported: int
    callback_endpoint: int


def codes_for(dialect: str) -> Codes:
    """A dialect's error codes (its errors in the client table): the spec's
    (provisional) or ChatGPT's."""
    if CLIENT_TABLE["dialects"].get(dialect, {}).get("errors") == "chatgpt":
        return Codes(-32011, -32012, INVALID_PARAMS, INVALID_PARAMS, -32015)
    return Codes(-32023, -32024, -32025, -32026, -32027)


_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def _client_name(name: str) -> str:
    """A client name as the client table compares it: spaces and tabs
    trimmed, ASCII letters lowercased, anything else left as it is. ASCII
    only, so every SDK and the core normalize a name the same way (Unicode
    case folding and whitespace differ between languages: Turkish İ, NEL,
    BOM). The core's harnesses.ClientName."""
    return name.strip(" \t").translate(_ASCII_LOWER)


def chatgpt_desktop(client_name: str, meta: object) -> bool:
    """Whether a call is from ChatGPT's desktop app, which can't subscribe with
    MCP Events, rather than a Work chat on the web, which can. Both send the
    same client name ("openai-mcp (Codex)"), User-Agent and _meta keys, and
    neither declares MCP Events; the desktop app's capabilities (_meta's
    io.modelcontextprotocol/clientCapabilities) declare both the UI
    extension's "text/html+skybridge" mime type and the openai/form
    extension, the web's neither. Both are required: anything else, missing
    or malformed capabilities included, is ChatGPT's. Observed, not a
    published contract. The core's harnesses.ChatGPTDesktop; the same rule in
    every SDK."""
    if _client_name(client_name) != "openai-mcp (codex)":
        return False
    caps = meta.get("io.modelcontextprotocol/clientCapabilities") if isinstance(meta, dict) else None
    ext = caps.get("extensions") if isinstance(caps, dict) else None
    if not isinstance(ext, dict) or "openai/form" not in ext:
        return False
    ui = ext.get("io.modelcontextprotocol/ui")
    types = ui.get("mimeTypes") if isinstance(ui, dict) else None
    return isinstance(types, list) and "text/html+skybridge" in types


def chatgpt_desktop_chat(client_name: str, meta: object) -> bool:
    """Whether a call is from a regular chat in ChatGPT's desktop app (not
    Codex mode): ChatGPT's classic client name, "openai-mcp", with _meta
    openai/userAgent starting "Codex Desktop/". It can't subscribe with MCP
    Events and has no shell. The web's chats don't send that _meta.
    Observed, not a published contract. The core's
    harnesses.ChatGPTDesktopChat; the same rule in every SDK."""
    if _client_name(client_name) != "openai-mcp":
        return False
    ua = meta.get("openai/userAgent") if isinstance(meta, dict) else None
    return isinstance(ua, str) and ua.startswith("Codex Desktop/")


def chatgpt_not_native(client_name: str, meta: object) -> bool:
    """Whether a ChatGPT call can't subscribe with MCP Events: the desktop
    app, in Codex mode or a regular chat."""
    return chatgpt_desktop(client_name, meta) or chatgpt_desktop_chat(client_name, meta)


def dialect_of(client_name: str, user_agent: str, meta: Any = None) -> Dialect:
    """A client's dialect by the client table, from its clientInfo name,
    User-Agent and the request's _meta."""
    return evaluate_client(client_name, user_agent, meta, None).dialect


_SENTINEL_PREFIX = "=?base64?"
_SENTINEL_SUFFIX = "?="
# Canonical, padded standard Base64 (RFC 4648 §4).
_BASE64_CANONICAL = re.compile(r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?")


def header_value(h: str) -> str | None:
    """Decodes an Mcp-Name header: plain ASCII, or SEP-2243's sentinel
    (=?base64?…?=) holding canonical, padded standard Base64 of UTF-8 (the
    2026-07-28 transport spec's examples; the official SDKs refuse anything
    else). None: a sentinel that doesn't decode so (unpadded, non-canonical,
    not Base64, not UTF-8). The same rule in every SDK
    (sdk/typescript/test/mcp-events-interop.json)."""
    if (
        len(h) < len(_SENTINEL_PREFIX) + len(_SENTINEL_SUFFIX)
        or not h.startswith(_SENTINEL_PREFIX)
        or not h.endswith(_SENTINEL_SUFFIX)
    ):
        return h
    body = h[len(_SENTINEL_PREFIX) : -len(_SENTINEL_SUFFIX)]
    if not _BASE64_CANONICAL.fullmatch(body):
        return None
    try:
        b = base64.b64decode(body, validate=True)
    except (binascii.Error, ValueError):
        return None
    # Canonical also means the unused bits are zero: re-encoding gives the same text.
    if base64.b64encode(b).decode() != body:
        return None
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return None


#: The JSON Schema annotation that mirrors an argument into an
#: Mcp-Param-{Name} header (SEP-3415, custom headers from subscription arguments).
PARAM_ANNOTATION = "x-mcp-header"

#: The prefix of the headers PARAM_ANNOTATION names, in lowercase (header
#: names are case-insensitive).
PARAM_HEADER_PREFIX = "mcp-param-"


@dataclass(frozen=True)
class ParamHeader:
    """An annotated argument: its path (a chain of properties keys), the
    header's name ({Name} in Mcp-Param-{Name}), and its type."""

    path: tuple[str, ...]
    name: str
    type: str


# RFC 9110 §5.6.2 token (1*tchar): what a header's name may hold.
_TCHAR = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
_PARAM_TYPES = frozenset({"string", "integer", "boolean"})
# The keywords whose subschemas aren't statically reachable (the transport's
# Schema Extension): an annotation under any of them is invalid. A single
# subschema, a list of them, or (_SCHEMA_MAPS) a map of them.
_OFF_CHAIN = frozenset(
    {
        "items",
        "prefixItems",
        "contains",
        "additionalProperties",
        "unevaluatedProperties",
        "unevaluatedItems",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "contentSchema",
        "anyOf",
        "oneOf",
        "allOf",
        "patternProperties",
        "dependentSchemas",
        "dependencies",
        "$defs",
        "definitions",
    }
)
_SCHEMA_MAPS = frozenset({"patternProperties", "dependentSchemas", "dependencies", "$defs", "definitions"})


def _json(v: object) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except (TypeError, ValueError):
        return type(v).__name__


def param_headers_of(schema: object) -> tuple[list[ParamHeader], str | None]:
    """The x-mcp-header annotations in an event's arguments' schema, and why
    one is invalid (None when none is): the 2026-07-28 Streamable HTTP
    transport's constraints, which SEP-3415 takes for event types. Each is a
    non-empty RFC 9110 token, on a property typed "string", "integer" or
    "boolean" (one type: never number), reached from the root through
    properties keys only, and unique in any case. A spec client drops an event
    type that breaks one from events/list, so defining it is refused. The same
    rule in every SDK (sdk/typescript/test/mcp-events-interop.json
    mcp_param_schema)."""
    headers: list[ParamHeader] = []
    seen: dict[str, str] = {}
    # An explicit stack, so any finite schema is walked; reachable: every step
    # from the root was a properties key.
    stack: list[tuple[object, tuple[str, ...], bool]] = [(schema, (), True)]
    while stack:
        node, path, reachable = stack.pop()
        if not isinstance(node, Mapping):
            continue
        if PARAM_ANNOTATION in node:
            where = ".".join(path) if path else "the schema's root"
            if not reachable or not path:
                return [], (
                    f"{PARAM_ANNOTATION} on {where}: only a property reached from the root through properties "
                    "(not items, anyOf, oneOf, allOf, not, if/then/else, additionalProperties or $defs) can carry it"
                )
            name = node[PARAM_ANNOTATION]
            if not isinstance(name, str) or not _TCHAR.fullmatch(name):
                return [], (
                    f"{PARAM_ANNOTATION} on {where}: {_json(name)} isn't a header name "
                    "(letters, digits and !#$%&'*+-.^_`|~, at least one)"
                )
            t = node.get("type")
            if not isinstance(t, str) or t not in _PARAM_TYPES:
                return [], (
                    f"{PARAM_ANNOTATION} on {where}: only a string, integer or boolean property can carry it "
                    f"(its type is {_json(t)})"
                )
            other = seen.get(name.lower())
            if other is not None:
                return [], f"{PARAM_ANNOTATION} on {where}: {name} is already {other}'s (header names are case-insensitive)"
            seen[name.lower()] = where
            headers.append(ParamHeader(path, name, t))
        for k, v in node.items():
            if k == "properties" and isinstance(v, Mapping):
                for prop, sub in v.items():
                    stack.append((sub, (*path, str(prop)), reachable))
            elif k in _OFF_CHAIN:
                if isinstance(v, list):
                    subs: list[object] = list(v)
                elif k in _SCHEMA_MAPS and isinstance(v, Mapping):
                    subs = list(v.values())
                else:
                    subs = [v]
                stack.extend((sub, path, False) for sub in subs)
    # The order they're checked in: by path, whatever order the schema has them.
    headers.sort(key=lambda h: h.path)
    return headers, None


# A plain (not Base64) header value: visible ASCII, spaces and tabs.
_PLAIN_HEADER = re.compile(r"[\t\x20-\x7e]*")
# A header an integer is compared with as a number: a plain decimal, no sign or exponent.
_DECIMAL = re.compile(r"-?[0-9]+(\.[0-9]+)?")


def param_header_value(h: str) -> str | None:
    """An Mcp-Param header's value: trimmed of spaces and tabs, decoded
    (header_value), and None if malformed (a bad sentinel, or a plain value
    with anything but visible ASCII, spaces and tabs)."""
    v = h.strip(" \t")
    decoded = header_value(v)
    if decoded == v and not _PLAIN_HEADER.fullmatch(v):
        return None
    return decoded


def _header_text(v: object) -> str | None:
    """A body value as a client writes it in a header, before encoding: a
    string as it is, a boolean as true or false, a number in decimal as
    JavaScript writes it (3.0 is 3); None for anything else (an object, an
    array), which has none."""
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        try:
            return str(v)
        except ValueError:  # more digits than str() writes: no header form
            return None
    if isinstance(v, float) and math.isfinite(v):
        if v.is_integer() and abs(v) < 1e21:
            return str(int(v))
        return repr(v)
    return None


def _value_at(args: object, path: tuple[str, ...]) -> object:
    """The value at path in args (mappings only), or None."""
    node = args
    for k in path:
        if not isinstance(node, Mapping) or k not in node:
            return None
        node = node[k]
    return node


def param_header_refusal(
    annotated: Sequence[ParamHeader], args: object, values: Callable[[str], Sequence[str] | None]
) -> str | None:
    """Why an events/poll, events/stream, events/subscribe or
    events/unsubscribe over HTTP is refused for its Mcp-Param-{Name} headers
    (HTTP 400, HeaderMismatch), or None. Strict, as the spec requires,
    unlike a missing Mcp-Name: each annotated argument with a value other than
    None in the body needs its header, once, decoding (param_header_value) to
    the value as the client writes it (an integer compared as a number: 42.0
    is 42), and a header for an argument with no value or no header form is
    refused too. Headers no annotation names are ignored. values: a header's
    fields by its lowercase name (None: none). The same rule in every SDK
    (sdk/typescript/test/mcp-events-interop.json mcp_param)."""
    for p in annotated:
        header = f"Mcp-Param-{p.name}"
        where = "arguments." + ".".join(p.path)
        got = list(values(PARAM_HEADER_PREFIX + p.name.lower()) or [])
        if len(got) > 1:
            return f"Header mismatch: more than one {header} header"
        value = _value_at(args, p.path)
        text = None if value is None else _header_text(value)
        if text is None:
            if got:
                return f"Header mismatch: {header} is sent but {where} has no value it mirrors"
            continue
        if not got:
            return f"Header mismatch: {header} is missing ({where} has a value)"
        decoded = param_header_value(got[0])
        same = decoded == text
        if (
            decoded is not None
            and p.type == "integer"
            and not isinstance(value, bool)
            and (isinstance(value, int) or (isinstance(value, float) and value.is_integer()))
            and _DECIMAL.fullmatch(decoded)
        ):
            whole, _, fraction = decoded.partition(".")
            try:
                same = fraction.strip("0") == "" and int(whole) == int(value)
            except ValueError:  # more digits than int() takes
                same = False
        if not same:
            return f"Header mismatch: {header} doesn't match {where}"
    return None


def declares_events_extension(meta: object) -> bool:
    """Whether a request's _meta declares the MCP Events extension in its
    capabilities, as a 2026-07-28 client does on every request."""
    caps = meta.get("io.modelcontextprotocol/clientCapabilities") if isinstance(meta, Mapping) else None
    ext = caps.get("extensions") if isinstance(caps, Mapping) else None
    return isinstance(ext, Mapping) and EXTENSION_KEY in ext


#: The protocol revision whose transport requires Mcp-Method and MCP-Protocol-Version on every request.
MODERN_PROTOCOL = "2026-07-28"
_VERSION_META = "io.modelcontextprotocol/protocolVersion"


def transport_header_refusal(method: str, meta: Mapping[str, Any] | None, headers: Mapping[str, str]) -> str | None:
    """Why an events/* request over HTTP is refused for the transport's
    standard headers (HTTP 400, HeaderMismatch), or None: the 2026-07-28
    Streamable HTTP transport's Server Validation, which SEP-3415's Routing
    Headers apply to events methods. An Mcp-Method that's sent must equal the
    method exactly, on any request. A 2026-07-28 request (its
    MCP-Protocol-Version header or _meta's protocolVersion names it) needs
    Mcp-Method, and an MCP-Protocol-Version equal to _meta's when _meta names
    one. A legacy request (an initialize session's) may leave Mcp-Method out.
    The same rule in every SDK (sdk/typescript/test/mcp-events-interop.json
    transport_headers)."""
    got = headers.get("mcp-method")
    header_version = headers.get("mcp-protocol-version")
    meta_version = (meta or {}).get(_VERSION_META)
    if got is not None and got.strip(" \t") != method:
        return "Header mismatch: Mcp-Method doesn't match the request's method"
    if isinstance(meta_version, str) and meta_version:
        if header_version is None:
            return "Header mismatch: MCP-Protocol-Version is missing"
        if header_version.strip(" \t") != meta_version:
            return "Header mismatch: MCP-Protocol-Version doesn't match _meta's protocolVersion"
    # 2026-07-28 or later (dates compare as text), as the other SDKs read it.
    modern = (isinstance(meta_version, str) and meta_version >= MODERN_PROTOCOL) or (header_version or "").strip(
        " \t"
    ) >= MODERN_PROTOCOL
    if modern and got is None:
        return "Header mismatch: Mcp-Method is missing"
    return None


def name_header_refusal(header: str | None, name: object) -> str | None:
    """Why an events/subscribe, events/unsubscribe, events/poll or
    events/stream over HTTP is refused for its Mcp-Name header (HTTP 400,
    HeaderMismatch), or None if it isn't: one that's sent must decode
    (header_value) to params.name, from any client, whatever it claims to be.
    A missing one is accepted from every client: SEP-3415 and SEP-2243 say
    every client must send it, but ChatGPT, OpenClaw, the Vercel AI SDK, Hermes
    and Mastra don't yet (a documented deviation). header: the request's
    Mcp-Name (None or "": none). The same rule in every SDK
    (sdk/typescript/test/mcp-events-interop.json mcp_name)."""
    if not header:
        return None
    v = header_value(header.strip(" \t"))
    if v is None or v != name:
        return "Header mismatch: Mcp-Name doesn't match params.name"
    return None


#: Whether this SDK serves push (events/stream): the client table's
#: without_push entries (opencode) stop applying once it does.
SERVES_PUSH = False


@dataclass(frozen=True)
class ClientVerdict:
    """The client table's answer for one request: native-ness, the dialect,
    and the matched entry's id ("" when none did: capabilities decided)."""

    native: bool
    dialect: Dialect
    entry: str


def ua_products(user_agent: str) -> list[str]:
    """A User-Agent's products: each token's text before its first "/",
    ASCII-lowercased, the tokens split on spaces and tabs only (_client_name
    says why)."""
    out: list[str] = []
    for tok in re.split(r"[ \t]+", user_agent.translate(_ASCII_LOWER)):
        product = tok.split("/", 1)[0]
        if product:
            out.append(product)
    return out


# The built-in checks an entry can name (match.detector): one function in
# each codebase.
_DETECTORS = {"chatgpt_desktop": chatgpt_not_native}


def _declares(meta: Mapping[str, Any], session_extensions: Any) -> bool:
    if isinstance(session_extensions, Mapping) and EXTENSION_KEY in session_extensions:
        return True
    return declares_events_extension(meta)


def _matches(match: Mapping[str, list[str]], client: str, raw: str, products: list[str], meta: Mapping[str, Any]) -> bool:
    # Every field holds (AND), each by any of its values (OR).
    for field, values in match.items():
        ok = False
        for v in values:
            if field == "client":
                ok = client == v
            elif field == "client_prefix":
                ok = client.startswith(v)
            elif field == "ua_product":
                ok = v in products
            elif field == "meta_key":
                got = meta.get(v)
                ok = isinstance(got, str) and got != ""
            elif field == "detector":
                detector = _DETECTORS.get(v)
                ok = detector is not None and detector(raw, dict(meta))
            if ok:
                break
        if not ok:
            return False
    return True


def evaluate_client(
    client_name: str, user_agent: str, meta: Any, session_extensions: Any, serves_push: bool = SERVES_PUSH
) -> ClientVerdict:
    """The client table's verdict (the semantics in mcp-events-clients.json's _about): the first entry, in
    file order, whose match holds (a without_push entry doesn't apply when
    the server serves push) decides native-ness and the dialect; with none,
    capabilities decide, in the spec dialect. The app's own is_native_client
    answers before this. The same evaluator in every SDK and the core
    (sdk/typescript/test/mcp-events-interop.json native_client)."""
    meta = meta if isinstance(meta, Mapping) else {}
    client = _client_name(client_name)
    products = ua_products(user_agent)
    for e in CLIENT_TABLE["entries"]:
        if e.get("without_push") and serves_push:
            continue
        if not _matches(e["match"], client, client_name, products, meta):
            continue
        native = e["native"]
        if native == "never":
            verdict = False
        elif native == "undeclared":
            verdict = True
        else:
            verdict = _declares(meta, session_extensions)
        return ClientVerdict(verdict, e["dialect"], e["id"])
    return ClientVerdict(_declares(meta, session_extensions), "spec", "")


def native_client(client_name: str, user_agent: str, meta: Any, session_extensions: Any, serves_push: bool = SERVES_PUSH) -> bool:
    """Whether a client subscribes with MCP Events itself: the one rule on
    every path (tools/list, events/list, call.subscribe, after the app's
    is_native_client) and in every SDK: the client table (evaluate_client).
    Its first matching entry decides (ChatGPT's desktop app never; ChatGPT
    and OpenClaw without declaring; a harness Wakeshift wakes itself never);
    otherwise a client that declares the extension, in the request's _meta or
    its session's initialize (session_extensions, None without one).
    serves_push: push would be listed for this request (events/list's rule),
    so the table's without_push entries (opencode) don't apply."""
    return evaluate_client(client_name, user_agent, meta, session_extensions, serves_push).native


def _push_client(client_name: str, user_agent: str, meta: Any = None) -> bool:
    """Whether a client is one push wakes in its open conversation:
    the client table decides it by one of its without_push entries
    (opencode) where push isn't served."""
    entry = evaluate_client(client_name, user_agent, meta, None, serves_push=False).entry
    return any(e["id"] == entry and e.get("without_push") for e in CLIENT_TABLE["entries"])
