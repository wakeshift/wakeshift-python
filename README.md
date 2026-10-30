# wakeshift

Let agents subscribe to things in your app. You define the events your app offers and emit them when they happen; Wakeshift wakes every agent subscribed to them. Clients that speak MCP Events, such as ChatGPT, subscribe natively; every other agent (Claude Code, Codex, Cursor and the rest) subscribes with a tool the SDK adds, and Wakeshift wakes it its own way. You write no subscribe tool and handle no harnesses.

Python 3.11 or later, on the official MCP Python SDK (`mcp` 2.3 or later). The same API as the Node and Go SDKs.

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

from wakeshift import Identity, Recipient, ToolCall, Wakeshift

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


def user_of(ctx) -> str | None:
    """Your own sign-in: your ID for the person behind the request (ctx.request is the HTTP request)."""


server = MCPServer("chat")
attached = wake.attach(server, identify=lambda ctx: Identity(user=user_of(ctx)))  # define every event first


# Your own tool subscribes the conversation calling it.
@attached.tool()
async def join(table: str, call: ToolCall) -> str:
    """Take a seat at a table."""
    sub = await call.subscribe(turn, {"table": table}, reason=f"Playing at {table}")
    seats[table] = [*seats.get(table, []), Recipient(user=sub.user, subscription=sub.id)]
    return f"You're seated at {table}."


seats: dict[str, list[Recipient]] = {}  # your app's own membership
app = server.streamable_http_app()  # run with uvicorn, behind your sign-in


# When something happens:
async def on_message(channel: str, sender: str, text: str) -> None:
    await message.emit({"channel": channel}, {"sender": sender, "text": text})


async def on_turn(table: str, player: str, hole: list[str], to_call: int) -> None:
    await turn.emit({"table": table}, {"hole": hole, "to_call": to_call}, to=player)


async def on_deal(table: str) -> None:
    # One emit to every seated conversation; ended lists those no longer subscribed.
    out = await turn.emit({"table": table}, {"hole": [], "to_call": 0}, to=seats.get(table, []))
    seats[table] = [r for r in seats.get(table, []) if r not in out.ended]
```

That's all. An agent subscribes to `chat.message` for `eng`, and each message in #eng wakes it with the text line and the payload, framed as information from your app, not as instructions from its user.

- **`wake.define_event(name, description=, args=, payload=, text=, authorize=None, personal=False, delete_on_delivery=False)`** returns the event. `args` and `payload` are pydantic models or JSON Schemas. `args` is what a subscriber picks; its required string, integer and boolean fields route occurrences, so their values are lowercase IDs (a-z 0-9 . _ -), and an optional one filters on the payload field of the same name. `text(payload, args)` is the line the agent reads first. `authorize(subscriber, args)` (sync or async) decides who may subscribe. With `delete_on_delivery=True`, Wakeshift deletes each occurrence's text and payload once it's delivered.
- **`await event.emit(args, payload, to=None, id=None)`** checks the payload and sends it to everyone subscribed with those arguments. A personal event goes to its recipients, `to`: a user's ID (every subscription they have to it), `Recipient(user=…, subscription=…)` (exactly one conversation, from `call.subscribe`) or `Recipient(native=…)` (only the user's own MCP Events subscriptions), one or a list of up to 1,000. It returns `Emitted`: `matched`, `duplicate`, `unmatched` (recipients with no live subscription) and `ended` (subscriptions no longer live). `id` makes retries emit once.
- **`wake.attach(server, identify=None, tools=None, events=None)`** attaches Wakeshift to your MCP server. It serves `events/list`, `events/subscribe` and `events/unsubscribe` to clients that speak MCP Events, and adds the tools `subscribe_events` (for everyone else), `unsubscribe_events` and `list_subscriptions` (for every client, so a person can always see and stop what they're subscribed to); `tools` names fewer of them (`BUILTIN_TOOLS`; `[]` for none). `identify(ctx)` says who is behind a request, as an `Identity(user=…, client=…)`: `user` is your ID for the person, from your own sign-in, and `client` (optional) is the MCP client's name as your sign-in knows it, for a server that never sees `initialize`; a name the request carries wins. Without it, requests have no user, so native subscriptions, personal events and `list_subscriptions` are refused. Each connection is answered for its own client, so serve over stateful Streamable HTTP (the default) or stdio. It returns the handle your own tools register with. `events=EventsOptions(native=…)` tells it which calls come from clients that subscribe themselves (your own house agents, say).
- **`@attached.tool(name=None, description=None, ...)`** registers an async tool on the server (`MCPServer.tool`'s options pass through) with a parameter annotated `ToolCall`. The tool's schema gains `session_key`, `harness` and `conversation_id`, and hides `call`. An argument it doesn't declare is ignored, not refused. **`await call.subscribe(event, args, ttl=None, reason=None, label=None)`** subscribes the calling conversation and returns a `Subscription` (`id`, `user`, `created`, `session`, `recognized`, `native`); store `id` and `user` to reach that conversation later. **`await call.unsubscribe(event, args)`** ends it; **`await call.resolve_session()`** finds the conversation's session without subscribing. Wakeshift adds one block to whatever the tool returns, however many subscriptions it makes, telling the agent how it's woken. A call that fails (an exception, an `isError` result, an output its schema refuses, a canceled request) ends the subscriptions it made. A client that subscribes itself (ChatGPT, or one that declares MCP Events) isn't subscribed: `call.native` is true and the block tells it what to subscribe to. A return type subclassing `WakeOutput` carries the block's text in `structuredContent` too. `call.context` is MCP's `Context`.
- **`await event.end(user, id)`** ends one of a user's subscriptions by its ID.

## Sign-in

Your app signs its own users in, and passes who they are to `attach` as `identify`. It acts for their agents in the workspaces Wakeshift keeps for it:

- **`await wake.workspace(user, name)`** is the workspace Wakeshift keeps for one of your users, created on first use. That user's conversations are kept there.
- **`await wake.add_agent(workspace, agent, display_name)`** creates an agent in one of those workspaces (if new) and its grant under your manifest's agent scope. `harness_agent(client_name)` names an agent after its MCP client (`claude-code`, `codex`).
- **`await wake.check_agent(workspace, agent)`** raises `WakeshiftError` `no_grant` once the grant is revoked, `unknown_workspace` once the user is forgotten.

## Your own tools

Any tool registered with `@attached.tool()` can subscribe the calling conversation, end its subscription, or return structured output that carries Wakeshift's block:

```python
from wakeshift import WakeOutput


@attached.tool()
async def watch(channel: str, call: ToolCall) -> str:
    """Wake me on new messages in a channel."""
    await call.subscribe(message, {"channel": channel}, reason=f"Watching #{channel}")
    return f"Watching #{channel}."


class Seat(WakeOutput):
    table: str
    players: int


@attached.tool()
async def sit(table: str, call: ToolCall) -> Seat:
    """Take a seat, and see how many are at the table."""
    sub = await call.subscribe(turn, {"table": table})
    seats[table] = [*seats.get(table, []), Recipient(user=sub.user, subscription=sub.id)]
    return Seat(table=table, players=len(seats[table]))


@attached.tool()
async def leave(table: str, call: ToolCall) -> str:
    """Leave a table."""
    await call.unsubscribe(turn, {"table": table})
    seats[table] = [r for r in seats.get(table, []) if r.user != call.user]
    return f"You left {table}."


async def listening(channel: str) -> int:
    """How many conversations watching a channel are connected now."""
    return (await message.status({"channel": channel})).connected
```

`Seat` subclasses `WakeOutput`, so its `wakeshift` field carries the block's text too, for hosts that show the model only `structuredContent`. Your own signatures don't name `session_key`, `harness` or `conversation_id`: `@attached.tool()` adds them.

The other calls:

- **`await event.status(args, to=None)`** counts the conversations subscribed with those arguments (a personal event: one user's), how many are connected now, and how. **`await event.status_of([Recipient(user=…, subscription=…), …])`** reads up to 1,000 subscriptions by ID: whether each is live and its conversation connected.
- **`await event.publish_url(args, to=None)`** is a capability URL that emits the event from a webhook: a POST's body is the payload. **`await wake.revoke_publish_url(id)`** stops it working.
- **`await wake.forget_user(user)`** deletes everything Wakeshift keeps for one of your users. **`await wake.publish_manifest(manifest)`** tells Wakeshift what your app may do: `description`, `agent` (the scope of the agents your app acts for), `others_can_wake` (other people and their agents wake your users' agents through your app, so the consent screen says so) and `preapprove_tools` (your tools, by name, that agents' programs can run without asking each time; at most 20), typed as `Manifest`. It replaces the manifest, so it's the way to remove an event. You needn't call it otherwise: your MCP server, the process that calls `attach`, sends the manifest automatically the first time it needs it (a subscription, a wrapped tool's call, an emit), adding your defined events to the ones Wakeshift has and never removing one or clearing a setting, so processes that share your key keep each other's events. A process that only emits never sends it. Once you've called it, every automatic send carries the settings you passed. `events` is only for events defined elsewhere: they go before your defined ones, and listing a defined one is an error.

## Errors

Calls raise `WakeshiftError` with the API's `code` and HTTP `status`; `is_code(err, code)` checks one. The SDK checks some mistakes before sending (arguments and payloads against the event's schemas, recipients, another app's event) and raises the error Wakeshift gives for the same mistake, so the code is the same whichever caught it, and the same in the Go and Node SDKs. `ArgsError`, for arguments or a payload that don't match, is a `WakeshiftError` with code `bad_arguments`.

| Status | Code | When |
| --- | --- | --- |
| `400` | `bad_request` | Something in the request is invalid, including a field the API doesn't take. The message says what. Also more than 1,000 subscriptions in one `status_of`, or one without its user. |
| `400` | `bad_arguments` | An event's arguments or payload don't match its schemas, a routing value isn't a lowercase ID, or the recipients don't fit the event: a personal event's emit, publish URL or status without `to=` (a publish URL's or status's names exactly one user), a shared event's with one, a recipient with an empty user or subscription, or `call.subscribe` on a personal event with no user. |
| `401` | `unauthenticated` | The assertion was refused: expired, reused, signed with another key, or for another `aud` (a key for another Wakeshift). The message says which. |
| `403` | `other_client` | The `session_key` is a conversation in another MCP client of the same user. Call again without it. |
| `404` | `unknown_event` | Your app offers no event by that name, or `call.subscribe` or `call.unsubscribe` was given another app's event (one defined on another `Wakeshift` client). |
| `404` | `unknown_publish_url` | The publish URL ID isn't one of your app's. |
| `413` | `too_large` | The request body is over 64 KB (512 KB for an emit or a status call, room for 1,000 recipients). |
| `429` | `rate_limited` | Your emits to lists named over 10,000 recipients in a minute. |
| `0` | `unreachable` | No answer at all: a wrong `core`, a network problem, or the timeout. The message names the core it tried. |

## Keys

An app key is `wst_` followed by 50 letters and digits: your app's Ed25519 private key, with a checksum, so a mistyped key fails when you create `Wakeshift`. It never goes over the wire: each request carries a fresh assertion signed with it, valid two minutes. A key goes to `https://app.wakeshift.ai` unless you pass `core` or set `WAKESHIFT_CORE`.

The key doesn't name your app. `await wake.app()` asks Wakeshift which app it is and returns its `id` and `name`; after that, `wake.app_id` is the app's ID (before it, reading `wake.app_id` raises).

Older keys, `wst_test_<app>_<seed>` and `wst_live_<app>_<seed>`, keep working. They name the app, so `wake.app_id` works at once. A `wst_test_` key needs `core` or `WAKESHIFT_CORE`.

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
