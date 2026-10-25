"""harness_agent names the agent a guest's MCP client signs in as: its
harness's ID ("copilot-cli" → copilot, "Claude Code" → claude-code), else its
name as an ID. A port of the core's harnesses.AgentID: the Node SDK's
test/agent-ids.json is the Go function's output, and every SDK's tests check
it."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class _Known:
    id: str
    clients: tuple[str, ...] = field(default_factory=tuple)
    #: Its client name is one others send too (Hermes Agent's "mcp"): it keeps its own ID.
    unsure: bool = False


# The core's harness table (internal/harnesses): only what naming needs.
_KNOWN: tuple[_Known, ...] = (
    _Known("codex", ("codex-mcp-client",)),
    _Known("claude-code", ("claude-code",)),
    _Known("cursor", ("Cursor", "cursor-vscode")),
    _Known("cursor-cloud"),
    _Known("grok-bot"),
    _Known("copilot", ("copilot-cli",)),
    _Known("opencode", ("opencode",)),
    _Known("antigravity", ("antigravity-client",)),
    _Known("openclaw", ("openclaw-bundle-mcp",)),
    _Known("hermes", ("mcp",), unsure=True),
    _Known("chatgpt", ("openai-mcp",)),
    _Known("gemini", ("gemini-cli-mcp-client",)),
    _Known("goose", ("goose-cli", "goose-desktop")),
    _Known("cline", ("Cline",)),
    _Known("amp"),
    _Known("devin"),
    _Known("vscode", ("Visual Studio Code", "Visual Studio Code - Insiders")),
)


def harness_agent(client: str) -> str:
    """The agent ID an MCP client's name maps to: its harness's ID, else the
    name as an ID (2–40 lowercase letters, digits and dashes; "agent" if
    nothing's left)."""
    name = client.strip().lower()
    if name:
        for h in _KNOWN:
            if any(c.lower() == name for c in h.clients):
                if not h.unsure:
                    return h.id
                break
    ident = _slug(client)
    for h in _KNOWN:
        if h.unsure:
            continue
        if ident == h.id or ident.startswith(h.id + "-"):
            return h.id
        if any(ident == _slug(c) for c in h.clients):
            return h.id
    return ident


def _slug(name: str) -> str:
    """A name as an agent ID: lowercase letters, digits and dashes, up to 40
    characters, without anything in parentheses ("agent" if fewer than 2 are left)."""
    n = name.lower()
    i = n.find("(")
    if i > 0:
        n = n[:i]
    out = ""
    dash = False
    for r in n.strip():
        if "a" <= r <= "z" or "0" <= r <= "9":
            out += r
            dash = False
        elif not dash and out:
            out += "-"
            dash = True
    ident = out.strip("-")
    if len(ident) > 40:
        ident = ident[:40].strip("-")
    return "agent" if len(ident) < 2 else ident
