"""The SEP-3415 (MCP Events) constants the SDK uses, in one place, matching
Wakeshift's own (internal/mcpevents/event/spec.go) and the Node and Go SDKs':
the draft is still moving."""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from typing import Literal

#: The draft these follow.
REVISION = "SEP-3415 (2026-10-16)"

#: Capability keys: the draft's extension, and the top-level key ChatGPT reads. A server advertises both.
EXTENSION_KEY = "io.modelcontextprotocol/events"
TOP_LEVEL_KEY = "events"

#: On every events/* result.
RESULT_TYPE = "complete"

#: The _meta key of an occurrence's rendered line (Wakeshift adds it on delivery).
TEXT_META = "ai.wakeshift/text"

#: The Streamable HTTP header events/subscribe and events/unsubscribe carry: params.name.
NAME_HEADER = "mcp-name"

INVALID_PARAMS = -32602
HEADER_MISMATCH = -32020
INTERNAL_ERROR = -32603

Dialect = Literal["draft", "chatgpt"]


@dataclass(frozen=True)
class Codes:
    not_found: int
    forbidden: int
    resource_exhausted: int
    unsupported: int
    callback_endpoint: int


def codes_for(dialect: Dialect) -> Codes:
    """A dialect's error codes: the draft's (provisional) or ChatGPT's."""
    if dialect == "chatgpt":
        return Codes(-32011, -32012, INVALID_PARAMS, INVALID_PARAMS, -32015)
    return Codes(-32023, -32024, -32025, -32026, -32027)


def dialect_of(client_name: str, user_agent: str) -> Dialect:
    """A client's dialect, by its clientInfo name or User-Agent: ChatGPT's MCP client is openai-mcp."""
    if client_name.lower() == "openai-mcp" or user_agent.lower().startswith("openai-mcp"):
        return "chatgpt"
    return "draft"


def header_value(h: str) -> str | None:
    """Decodes an Mcp-Name header: plain ASCII, or SEP-2243's base64 sentinel
    (=?base64?…?=). None: a sentinel that doesn't decode."""
    if not (h.startswith("=?base64?") and h.endswith("?=")):
        return h
    body = h[len("=?base64?") : -2]
    try:
        b = base64.b64decode(body + "=" * (-len(body) % 4), validate=True)
    except (binascii.Error, ValueError):
        return None
    if base64.b64encode(b).decode().rstrip("=") != body.rstrip("="):
        return None
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return None
