# wakeshift

Let agents subscribe to things in your app. You define the events your app offers and emit them when they happen; Wakeshift wakes every agent subscribed to them. Clients that speak MCP Events, such as ChatGPT, subscribe natively; every other agent (Claude Code, Codex, Cursor and the rest) subscribes with a tool the SDK adds, and Wakeshift wakes it its own way. You write no subscribe tool and handle no harnesses.

Python 3.10 or later, on the official MCP Python SDK (`mcp` 2.3 or later). The same API as the Node and Go SDKs.

## Install

```sh
pip install wakeshift
```

Or `uv add wakeshift`.

## Quickstart: events

Register an app in the Wakeshift console (Build) and put its key in `WAKESHIFT_KEY`. The SDK talks to `https://app.wakeshift.ai` unless `WAKESHIFT_CORE` names another address.

```python
import os

from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from wakeshift import GuestAuth, Wakeshift

wake = Wakeshift(os.environ["WAKESHIFT_KEY"])


class Channel(BaseModel):
    channel: str


class Message(BaseModel):
    sender: str
    text: str


message = wake.define_event(
    "chat.message",
    description="A new message in a channel",
    args=Channel,
    payload=Message,
    text=lambda m, a: f"{m.sender} in #{a.channel}: {m.text}",
)


class Table(BaseModel):
    table: str


class Turn(BaseModel):
    hole: list[str]
    to_call: int


# A personal event: each subscription belongs to its subscriber, and an
# occurrence goes to one of them, so it can carry their own private view.
turn = wake.define_event(
    "game.turn",
    description="It's your move at a table",
    args=Table,
    payload=Turn,
    text=lambda t, a: f"Your move at {a.table}: {t.to_call} to call",
    personal=True,
)

server = MCPServer("chat")
wake.attach(server)  # define every event first

# Guests: MCP clients sign in with no account, each in its own workspace.
guests = GuestAuth(wake, "https://chat.example.com/mcp")
app = guests.wrap(server.streamable_http_app())  # run with uvicorn


# When something happens:
async def on_message(channel: str, sender: str, text: str) -> None:
    await message.emit({"channel": channel}, {"sender": sender, "text": text})


async def on_turn(table: str, player: str, hole: list[str], to_call: int) -> None:
    await turn.emit({"table": table}, {"hole": hole, "to_call": to_call}, to=player)
```

That's all. An agent subscribes to `chat.message` for `eng`, and each message in #eng wakes it with the text line and the payload, framed as information from your app, not as instructions from its user.

- **`wake.define_event(name, description=, args=, payload=, text=, authorize=None, personal=False, delete_on_delivery=False)`** returns the event. `args` and `payload` are pydantic models or JSON Schemas. `args` is what a subscriber picks; its required string, integer and boolean fields route occurrences, so their values are lowercase IDs (a-z 0-9 . _ -), and an optional one filters on the payload field of the same name. `text(payload, args)` is the line the agent reads first. `authorize(subscriber, args)` (sync or async) decides who may subscribe. With `delete_on_delivery=True`, Wakeshift deletes each occurrence's text and payload once it's delivered.
- **`await event.emit(args, payload, to=None, id=None)`** checks the payload and sends it to everyone subscribed with those arguments; a personal event goes to the one subscriber `to` names. `id` makes retries emit once.
- **`wake.attach(server, identify=None)`** attaches Wakeshift to your MCP server. Today it serves `events/list`, `events/subscribe` and `events/unsubscribe` to clients that speak MCP Events, and adds the tools `subscribe_events` (for everyone else), `unsubscribe_events` and `list_subscriptions` (for every client, so a person can always see and stop what they're subscribed to). `identify(ctx)` says who is behind a request, as an `Identity(user=…)` with your ID for the person; by default, the guest `GuestAuth` signed in. Each connection is answered for its own client, so serve over stateful Streamable HTTP (the default) or stdio.
- **`await event.subscribe(ctx, args, user=, wake_args=)`** subscribes the conversation calling one of your own tools, the way `subscribe_events` would, so a tool like "join" sets up its wakes in one step. `result(text)` is your tool's text with what the agent needs added. **`await event.unsubscribe(ctx, args, user=, wake_args=)`** ends the calling conversation's subscription with those arguments, for a tool like "unwatch". **`await event.end(user, id)`** ends one of a user's subscriptions by its ID.

## Sign-in

- **`GuestAuth(wake, mcp_url)`** is a small OAuth server inside your app for MCP clients: dynamic client registration, authorize with PKCE, tokens, all signed with a key derived from your app key, so it keeps no state. Each registered client is a guest with its own workspace. `guests.wrap(app)` serves its endpoints and requires a token on `/mcp`; inside a tool, `guest_of(ctx)` is who's calling. Or call `handle(request)` and `authenticate(request)` from your own routes, or pass `verify_token` to the MCP SDK's own auth. Registration and token requests are rate-limited per address (IPv6 per /64); behind a proxy, set `proxy_hops`. `grants=True` gives each guest a grant under your manifest's agent scope, checked at every refresh.
- Your app identifies its own users, with `GuestAuth` or its own sign-in, and acts for the agents in the workspaces it keeps.

## Topics

Events are built on topics, which you can use directly: subscribe the conversation calling one of your tools, and publish to it.

```python
from mcp.server.mcpserver import Context
from mcp.types import CallToolResult

from wakeshift import ConversationId, Harness, SessionKey


@server.tool()
async def watch_ci(
    repo: str, ctx: Context, session_key: SessionKey = None, harness: Harness = None, conversation_id: ConversationId = None
) -> CallToolResult:
    """Wake me when CI finishes on a repo"""
    call = wake.call(ctx, session=session_key, harness=harness, conversation=conversation_id)
    sub = await wake.subscribe(call, f"ci/{repo.lower()}", replay="5m")
    return sub.result(f"Watching CI on {repo}.")


async def on_ci(repo: str, conclusion: str, url: str) -> None:
    await wake.publish(f"ci/{repo.lower()}", text=f"CI {conclusion} on {repo}", data={"url": url})
```

`SessionKey`, `Harness` and `ConversationId` add the optional arguments Wakeshift needs from the agent: the session key an earlier result gave it (a secret: don't log tool arguments that carry it), the program it runs in, and its own conversation ID. The other calls: `unsubscribe`, `send` (one subscription), `topic_status`, `subscription_status`, `publish_url` and `revoke_publish_url`, `annotate` (adds held messages to another tool's result), `close_topic`, `end_subscription`, `unsubscribe_user`, `forget_user` (deletes everything Wakeshift keeps for one of your users) and `publish_manifest`.

## Errors

Calls raise `WakeshiftError` with the API's `code` (`private_topic`, `bad_request`…) and HTTP `status`; `is_code(err, code)` checks one. A core that doesn't answer is `unreachable` (status 0), and the message names the address it tried.

## Keys

An app key is `wst_` followed by 50 letters and digits: your app's Ed25519 private key, with a checksum, so a mistyped key fails when you create `Wakeshift`. It never goes over the wire: each request carries a fresh assertion signed with it, valid two minutes. A key goes to `https://app.wakeshift.ai` unless you pass `core` or set `WAKESHIFT_CORE`.

The key doesn't name your app. `await wake.app()` asks Wakeshift which app it is and returns its `id` and `name`; after that, `wake.app_id` is the app's ID (before it, reading `wake.app_id` raises).

Older keys, `wst_test_<app>_<seed>` and `wst_live_<app>_<seed>`, keep working. They name the app, so `wake.app_id` works at once. A `wst_test_` key needs `core` or `WAKESHIFT_CORE`. `wake.mode` is `"test"` or `"live"` for an older key and `None` for a current one; it's deprecated.

## Documentation

The guides and the full reference are at [wakeshift.ai/docs](https://wakeshift.ai/docs): the [quickstart](https://wakeshift.ai/docs/quickstart), [events](https://wakeshift.ai/docs/events), [your users](https://wakeshift.ai/docs/users) and [the Python SDK](https://wakeshift.ai/docs/python). The same calls are in the [TypeScript](https://github.com/wakeshift/wakeshift-node) and [Go](https://github.com/wakeshift/wakeshift-go) SDKs.

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
