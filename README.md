# wakeshift

Wakeshift lets an MCP server wake the AI agents that use it. It wakes the agent that started the work when it finishes: your MCP server's tools subscribe the calling conversation to your app's events, and Wakeshift wakes it (Claude Code, Codex, ChatGPT and the rest) the best way its client allows.

## Install

```sh
pip install 'wakeshift[mcp]'        # the official MCP Python SDK (MCPServer, the low-level Server)
pip install 'wakeshift[fastmcp]'    # FastMCP 4
```

On FastMCP 2.12+ or 3.x, install plain `wakeshift`, never `[mcp]`: `attach` works on them, and native MCP Events need FastMCP 4.

## Quickstart

A crawler that wakes the conversation that started a crawl. Put your app's key from the Wakeshift console in `WAKESHIFT_KEY`; without one, Wakeshift is off and the server runs as before.

```python
from uuid import uuid4

from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import ToolCall, Wakeshift

wake = Wakeshift()  # reads WAKESHIFT_KEY; without one, Wakeshift is off


class Job(BaseModel):
    job: str


class Crawl(BaseModel):
    status: str
    pages: int


crawl_finished = wake.define_event("crawl.finished", description="A crawl finished", args=Job, payload=Crawl)

server = MCPServer("crawler")
attached = wake.attach(server)


def start(url: str, job: str) -> None:
    """Starts the crawl (your own code)."""


@attached.tool(subscribes=[crawl_finished])
async def start_crawl(url: str, call: ToolCall) -> str:
    """Crawl a site."""
    job = str(uuid4())  # your tool minted the ID, so only this conversation can know it
    await call.subscribe(crawl_finished, {"job": job}, once=True)
    start(url, job)
    return f"Crawl {job} started."


app = server.streamable_http_app()  # run with uvicorn


# When the crawl finishes, in your worker or webhook handler:
async def on_crawl_finished(job: str, pages: int) -> None:
    await crawl_finished.emit(Job(job=job), Crawl(status="completed", pages=pages))
```

What just happened:

- **The key comes from `WAKESHIFT_KEY`.** Without one, Wakeshift is off: every call still works, nothing is sent, and your server runs as it would without it. `Wakeshift()` makes no network request.
- **`@attached.tool(subscribes=[...])` is your server's own `@server.tool`**, with the events it subscribes to named and a `call: ToolCall` the SDK fills. Your arguments stay yours: Wakeshift adds its own in `tools/list` and takes them out before your code runs.
- **Only your tools can subscribe to `crawl.finished`**, because it declares neither `personal`, `authorize` nor `public`. That's the safe default for IDs your tool mints, so there's nothing to check here.
- **`once=True` ends the subscription after one delivery**, and a call that fails ends what it subscribed.
- **`emit` wakes every conversation subscribed with those arguments.** Without `text`, the agent reads the description and the arguments.

Two helpers worth knowing early:

- **No webhook to point at Wakeshift?** `crawl_finished.watch({"job": job}, check, every="10s", timeout="1h")` calls your `check` until it returns the payload (`None`: keep waiting), then emits once, with backoff, a hard timeout and at most 10 checks at once. Start it from the event's `on_subscribe`, which runs once for every new subscription however it was made. A watch lives in your process only: a restart or a serverless freeze loses it.
- **Unit tests.** `wakeshift.testing` has `TestClient()` (enabled, offline, recording `emits` and `subscriptions`), `tool_call(...)` (a stand-in `ToolCall` to pass a tool's function) and `with_call(wake, call)` (for tools named in `wrap`). To keep an ordinary client off in tests when `WAKESHIFT_KEY` is set, construct it with `Wakeshift("")`.

Signing people in, webhooks, tools registered elsewhere (`wrap` and `wake.current_call()`), who may subscribe, logging and errors: [wakeshift.ai/docs/python](https://wakeshift.ai/docs/python).

## Compatibility

| | |
| --- | --- |
| Python | 3.11 or later; asyncio (not trio) |
| `wakeshift[mcp]` | `mcp` 2.3 or later, below 3: `MCPServer` and the low-level `Server` |
| `wakeshift[fastmcp]` | FastMCP 4.1 or later, below 5 |
| FastMCP 2.12+ or 3.x | plain `wakeshift`: `attach` (before or after anything that replaces the `tools/call` handler), tools that subscribe, `wrap` and the built-in tools; native MCP Events and protocol 2026-07-28 need FastMCP 4 |

## Development

```sh
uv sync
uv run ruff check
uv run pyright
uv run pytest
```

The end-to-end tests run against a Wakeshift server, so they run in Wakeshift's own repository and are skipped here.

## Contributing and security

This repository is generated from Wakeshift's main repository. Pull requests are welcome: we apply them there and publish the result. See [CONTRIBUTING](CONTRIBUTING.md). Report a security problem privately to support@wakeshift.ai, not in an issue ([SECURITY](SECURITY.md)).

## License

Apache License 2.0; see [LICENSE](LICENSE).
