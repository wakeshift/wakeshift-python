# Changelog

What changed in each release of `wakeshift`. 0.4.0 is the first published release: to start with it, read the [README](README.md). Its notes below, and the earlier releases', are for apps that ran the preview builds.

## 0.5.2 (2026-10-30)

### Added

- **Claude Code is woken with nothing to install, and no restarts.** `attach` now serves `wakeshift_wait` to clients that take MCP tasks on protocol 2026-07-28: a recent Claude Code in the terminal, where Anthropic has turned them on. Wakeshift tells those conversations to call it after they subscribe; it returns at once and waits in the background, through `/compact` and restarts of Claude Code, and wakes the conversation when your events arrive. Other clients don't see it, and keep the instructions they had. Nothing to change in your code.

### Fixed

- **Agents that see only `structuredContent` are told how they're woken.** Some hosts, Claude Code among them, show the model a tool's `structuredContent` and not its text. A wrapped tool returning `str`, as the README's example does, got `structuredContent` (`{"result": …}`) from the MCP SDK without Wakeshift's instructions in it, so the agent said it was watching and was never woken. Now a wrapped tool annotated `-> str` replies with text only (no output schema; pass `structured_output=True` to keep MCP's), and every structured result of a wrapped tool carries the instructions in its `wakeshift` field, which `tools/list` adds to the tool's output schema. Subclassing `WakeOutput` still declares the field in your own type; it's no longer required. Nothing to change in your code.

## 0.5.1 (2026-10-30)

### Added

- **Clients that speak MCP Events can poll.** `attach` now answers `events/poll`, and `events/list` offers `poll` beside `webhook` for every event. A client that can't take webhooks (an agent with no public URL, or Mastra in its poll mode) subscribes with its first poll, then each poll returns what happened since its cursor, in order. A poll's cursor acknowledges everything before it, so an event isn't lost when a poll fails: the next poll returns it again. Your `authorize` runs on every poll. Wakeshift keeps the subscription while the client polls (it lapses after 10 minutes without a poll), lists it with the user's other subscriptions (`list_subscriptions` says "polled by an MCP client"), and answers the client's next poll with `Forbidden` once the user stops it. Nothing to change in your code.
- **Webhooks for an event you no longer offer end.** When you remove an event from your app's manifest (with `publish_manifest`), Wakeshift ends each webhook subscription to it within an hour, telling the client the event is gone (`terminated`, with `NotFound` and `data.kind: "event"`), so the client lists your events again rather than waiting for its next refresh. A poll for it gets `NotFound` at once. Nothing to change in your code.

### Changed

- `attach` follows SEP-3415 as revised on 2026-10-27. `ResourceExhausted`'s data is now `{"limit": "subscriptions"}`, the draft's name for it (it was `{"kind": "subscriptions"}`). `events/stream` still answers `Unsupported`: push isn't offered.

## 0.5.0 (2026-10-29)

### Breaking

- **`recognised` is now `recognized`.** The `Recognised` class is `Recognized`, and `Subscription.recognised` and `ToolCall.recognised` are `recognized`. Rename them where you use them: `sub.recognised.harness` becomes `sub.recognized.harness`. What they hold is unchanged.

### Fixed

- **Processes that share your key no longer remove each other's events.** In 0.4.0 every process sent the events it defined as your app's whole list. With an MCP server and a separate emitter on one key, whichever sent last removed the other's events, and the other's emits failed with `unknown_event` until it restarted. Now only your MCP server, the process that calls `attach`, sends the manifest automatically, and Wakeshift merges it into the one it has: it adds or updates your events and the settings you passed to `publish_manifest`, and never removes an event or clears a setting. A process that doesn't call `attach` never sends it, whatever it calls (`emit`, `status`). `publish_manifest` still replaces the whole manifest, and is how you remove an event.
- A process with no events defined never sends the manifest. In 0.4.0 any app call from one (even a status read) sent an empty list, which removed every one of your app's events.
- If an event is defined only in a process that emits it, define it in your MCP server too (or publish it with `publish_manifest`): until a server sends it, emitting it fails with `unknown_event`.

## 0.4.0 (2026-10-29)

Define your app's events (`define_event`), attach Wakeshift to your MCP server (`attach`), subscribe the conversations that call your own tools (`@attached.tool()`, `call.subscribe`), and emit to them (`emit`). The rest of this section is what changed since 0.3.0.

### Breaking

One way to subscribe from your own tools. Register the tool with the handle `attach` returns, `@attached.tool()`, and subscribe with `call.subscribe`: Wakeshift adds what the agent needs to be woken to whatever you return, once per call however many subscriptions it makes, and a call that fails ends the subscriptions it made. The free `event.subscribe(ctx, …)`, `event.unsubscribe(ctx, …)` and `wake.call` are gone.

A join tool:

```python
# before
@server.tool()
async def join(
    table: str, ctx: Context, session_key: SessionKey = None, harness: Harness = None, conversation_id: ConversationId = None
) -> CallToolResult:
    """Take a seat at a table."""
    sub = await turn.subscribe(
        ctx,
        {"table": table},
        user=user_of(ctx),
        wake_args={"session_key": session_key, "harness": harness, "conversation_id": conversation_id},
    )
    await casino.seat(user_of(ctx), table, sub.id)
    return sub.result(f"You're seated at {table}.")


# now
attached = wake.attach(server, identify=lambda ctx: Identity(user=user_of(ctx)))


@attached.tool()
async def join(table: str, call: ToolCall) -> str:
    """Take a seat at a table."""
    sub = await call.subscribe(turn, {"table": table}, reason=f"Playing at {table}")
    await casino.seat(call.user, table, sub.id)
    return f"You're seated at {table}."
```

`@attached.tool()` adds `session_key`, `harness` and `conversation_id` to the tool's schema and hides `call`, so your own signature names neither.

A tool with structured output: subclass `WakeOutput`, and the block's text goes in `structuredContent["wakeshift"]` as well as the content, for hosts that show the model only `structuredContent`:

```python
# before
@server.tool()
async def sit(table: str, ctx: Context, session_key: SessionKey = None, harness: Harness = None) -> CallToolResult:
    sub = await turn.subscribe(
        ctx, {"table": table}, user=user_of(ctx), wake_args={"session_key": session_key, "harness": harness}
    )
    return sub.result(CallToolResult(content=[], structuredContent={"table": table, "players": players(table)}))


# now
class Seat(WakeOutput):
    table: str
    players: int


@attached.tool()
async def sit(table: str, call: ToolCall) -> Seat:
    await call.subscribe(turn, {"table": table})
    return Seat(table=table, players=players(table))
```

A leave tool:

```python
# before
@server.tool()
async def leave(table: str, ctx: Context, session_key: SessionKey = None, harness: Harness = None) -> str:
    await turn.unsubscribe(ctx, {"table": table}, user=user_of(ctx), wake_args={"session_key": session_key, "harness": harness})
    return f"You left {table}."


# now
@attached.tool()
async def leave(table: str, call: ToolCall) -> str:
    await call.unsubscribe(turn, {"table": table})
    return f"You left {table}."
```

Topics are gone: use events. Define an event for what you published to a topic, subscribe from a wrapped tool, emit to it, and count its subscribers with `event.status`:

```python
# before
await wake.publish(f"ci/{repo}", text=f"CI {conclusion} on {repo}", data={"url": url})
status = await wake.topic_status(f"ci/{repo}")

# now
build = wake.define_event(
    "ci.build", description="A CI run finished", args=Repo, payload=Build, text=lambda b, a: f"CI {b.conclusion} on {a.repo}"
)
await build.emit({"repo": repo}, {"conclusion": conclusion, "url": url})
status = await build.status({"repo": repo})
```

Groups are gone: keep membership in your app, store each member's `Subscription` (`id` and `user`), and emit to your members' subscriptions: `to=[Recipient(user=s.user, subscription=s.id) for s in members]`.

Errors the SDK raises before asking Wakeshift have the status and code Wakeshift gives for the same mistake, the same in the Go and Node SDKs:

- `ArgsError`, for arguments or a payload that don't match the event's schema, is a `WakeshiftError` with code `bad_arguments` (status 400), no longer a `ValueError`. A payload that doesn't match is `bad_arguments`, Wakeshift's code for an emit's payload, not `bad_payload`.
- `call.subscribe` or `call.unsubscribe` given another app's event raises `WakeshiftError` with code `unknown_event` (404), not `ValueError`.
- `status_of` with more than 1,000 subscriptions, or one without its user, is `bad_request`, not `bad_arguments`.

**A manifest's `events` add to the ones you define.** `publish_manifest`'s `events` used to replace the events `define_event` made. Now Wakeshift gets both, the listed ones first, and an event both defined and listed is an error (`ValueError`): nothing is sent. List only events defined elsewhere.

**A `filter` may not name a field an optional argument filters.** Without a `topic`, `define_event` adds a filter for each optional argument to your `filter`; where both named the same payload field, the argument's entry silently replaced yours. Now `define_event` raises `ValueError`, naming the field: remove it from `filter`, or pass `topic` and `filter` both.

Python 3.11 or later: Python 3.10 reached its end of life in October 2026, so the SDK requires 3.11 (`requires-python = ">=3.11"`) and is tested on 3.11 and 3.14.

### Removed

- `WakeEvent.subscribe(ctx, …)` and `WakeEvent.unsubscribe(ctx, …)`: use `call.subscribe` and `call.unsubscribe` in a wrapped tool.
- `CallerSubscription`, with its `result()`, `addition` and `agent`: `call.subscribe` returns a `Subscription`, whose `recognised` replaces `agent`, and the block is added for you.
- Topics: `wake.subscribe`, `wake.unsubscribe`, `wake.publish`, `wake.send`, `wake.topic_status`, `wake.subscription_status`, `wake.publish_url(topic)`, `wake.close_topic`, `wake.end_subscription` and `wake.unsubscribe_user`; `TopicSubscription`, `TopicStatus`, `SubscriptionStatus` and `Published` (its fields are on `Emitted`). `PublishUrl.topic` is gone: a publish URL is an event's (`event.publish_url(args)`). Use events: `call.subscribe`, `emit`, `event.status`, `event.publish_url`, `event.end`.
- `wake.call`, `wake.call_of`, `Call`, `apply_addition`, `Addition`, `wake.addition` and `wake.annotate`: the wrapper packs the call and adds the block.
- `UNKNOWN_CLIENT_WARNING`, which only the removed calls raised.
- `attach` returning nothing: it returns the handle, `Attached`.
- `AppKey.mode` and `wake.mode`, which meant nothing since current keys came in. Older keys (`wst_test_…`, `wst_live_…`) still work and name their app; a `wst_test_` key still needs a core.
- `session` as a hidden alias of `session_key`: a wrapped tool takes `session_key` only, and a `session` argument is ignored like any other the tool doesn't take.
- Reading a client's top-level `events` capability, the earlier MCP Events sketch's key: a client speaks MCP Events if it declares the extension (`extensions["io.modelcontextprotocol/events"]`) or is ChatGPT. `attach` still advertises both keys, since ChatGPT reads the top-level one.
- `LEGACY_KEY`, the top-level capability key `attach` advertises for ChatGPT. It's no longer exported; `EXTENSION_KEY` is the one a client declares.
- `SessionKey`, `Harness`, `ConversationId` and `WAKE_ARGS_SCHEMA`: with the free `event.subscribe` gone, nothing reads those arguments in a tool you register yourself. `@attached.tool()` adds them to a wrapped tool's schema.
- `speaks_events`: a wrapped tool's `call.native` says whether its caller subscribes to your events itself.
- The SDK's own plumbing is private: `Wakeshift.warn`, `request_app`, `emit_event` and `ensure_events`, and `WakeEvent.parse_args`, `allows` and `definition`, which were never documented, start with `_`.

Kept: `define_event`, `emit`, `event.publish_url`, `wake.revoke_publish_url`, `event.end`, `event.status` and `event.status_of`, `attach`'s three tools and native MCP Events, `forget_user`, `publish_manifest`, the users' workspaces calls, `harness_agent`, the errors and keys.

### Added

- `attach` returns a handle, `Attached`, and `@attached.tool()` registers an async tool on the `MCPServer` with a parameter annotated `wakeshift.ToolCall` (`user`, `tool`, `native`, `native_conversation`, `session`, `recognised`, `context`). `await call.subscribe(event, args, ttl=, reason=, label=)` subscribes the calling conversation and returns a `Subscription` (`id`, `user`, `created`, `session`, `recognised`, `native`); `await call.unsubscribe(event, args)` ends it. The tool's schema gains `session_key`, `harness` and `conversation_id` and hides `call` (its `__signature__` and evaluated `__annotations__`). A `ToolCall` used after its tool returned raises `CallEnded`.
- `Identity(client=…)`: the MCP client's name, for a stateless server that never sees `initialize`. Wakeshift reads it to tell which program the agent runs in, and whether the client subscribes to events itself. A name the request carries (`initialize`'s, or its `_meta`'s) wins.
- `attach(tools=[...])` names which of its own tools it adds (`BUILTIN_TOOLS`: `subscribe_events`, `unsubscribe_events`, `list_subscriptions`). All three by default; `[]` adds none.
- `wakeshift.WakeOutput`: a tool's return type subclassing it gets the block's text in `structuredContent["wakeshift"]` too.
- `await call.resolve_session()` finds or starts the calling conversation's session without subscribing it; `call.subscribe`'s `reason` is shown in the console.
- `emit` to several recipients in one call: `to=` takes a user's ID, `Recipient(user=…, subscription=…)`, `Recipient(native=…)` (only the user's own MCP Events subscriptions), or a list of up to 1,000, and returns `Emitted`, with `unmatched` and `ended` beside `matched` and `duplicate`.
- `event.status(args, to=…)`: how many conversations are subscribed to an event with these arguments (a personal event: one user's), how many are connected now, and how. `event.status_of([Recipient(user=…, subscription=…), …])` reads up to 1,000 subscriptions by ID: whether each is live and its conversation connected.
- A manifest's `others_can_wake` (your app wakes its users' agents with what other people and their agents do there, so the consent screen says so) and `preapprove_tools` (your tools, by name, that agents' programs can run without asking each time; at most 20). When a user connects Claude Code or Cursor CLI, Wakeshift offers to allow exactly those tools, in the same question as its install.
- `Manifest`, `AgentScope`, `ScopeEvent`, `WakeLimit` and `EventTypeManifest`: typed dicts with the Go and TypeScript SDKs' fields. `publish_manifest` takes and returns a `Manifest`.

### Changed

- A wrapped tool's reply carries one Wakeshift block, however many subscriptions it makes, added after `MCPServer`'s result conversion, and only when the call subscribed or names a conversation.
- A subscription a failed call made is ended: an exception, an `isError` result, an output the schema refuses, a JSON-RPC error or a canceled request rolls it back, under `asyncio.shield`, and the reply gets no block.
- A client that speaks MCP Events is told which event to subscribe to, or that it has, and isn't subscribed by the wrapper. Native callers are recognized on stateless servers too: `attach(..., events=EventsOptions(native=…))`, then `_meta["openai/session"]`, a known native client name or User-Agent, or a client declaring MCP Events. `call.unsubscribe` ends their own webhook subscriptions.
- `session_key` is the one name for the session argument.
- `attach`'s own tools (`subscribe_events`, `unsubscribe_events`, `list_subscriptions`) describe `session_key` to the agent as a wrapped tool does.
- `list_subscriptions` is marked read-only and idempotent (`readOnlyHint` and `idempotentHint` true), so clients that run read-only tools without asking (Codex, by default) don't ask before it.
- You needn't call `publish_manifest`. Wakeshift gets the manifest automatically the first time your app needs it (an emit, a status, a publish URL, a subscription), and again after a `define_event`. The automatic send carries everything your app configured: the manifest you last passed to `publish_manifest` (even if that call failed), or else the description, agent scope, `others_can_wake` and `preapprove_tools` Wakeshift already has. Before, an automatic send before your first `publish_manifest` carried only the events, wiping the rest.

### Fixed

- An emit's recipients are trimmed of the whitespace around their names before they're sent (they were only checked), and `status_of` trims each subscription ID, as the other SDKs do.
- A 3xx answer is an error, as in the Go and Node SDKs, not read as a success.
- An error whose `code` isn't a string has an empty code.
- An MCP Events client gets "Wakeshift couldn't be reached" when the core is unreachable, without the core's address; a `callback_failed` error's `data.reason` is always a string.
- Google's 500 page is recognized by "Server Error" within the body's first 4 KB.
- `event.publish_url(args, to=[…])` raises `bad_arguments` (a publish URL's `to` is exactly one user), as the other SDKs do, instead of failing with `AttributeError`; an emit's `to=` that isn't a user, a `Recipient` or a list of them raises `bad_arguments`, not `TypeError`.
- A wrapped tool whose block couldn't be added reports why through `on_warning`, as every other failure does, not through `logging`.
- A failed tool call's rollback no longer ends a subscription that another call on the same conversation renewed in the meantime (two calls in parallel, or a second call while the first was still running). The call that renewed it told the agent it would be woken; now it is.

## 0.3.0 (2026-10-26)

### Breaking

Guest sign-in is gone. Your app signs its own users in and tells Wakeshift who they are: pass `identify` to `attach`, and act for their agents in the workspaces Wakeshift keeps for it (`wake.workspace`, `wake.add_agent`, `wake.check_agent`). Apps on 0.2 that use `GuestAuth` must replace it with their own sign-in before upgrading.

Removed:

- `GuestAuth`, `Guest` and `guest_of`.
- `Wakeshift.secret_seed()`.
- `attach`'s default `identify`, which returned the guest `GuestAuth` signed in. Without `identify`, requests have no user: native subscriptions, personal events and `list_subscriptions` are refused.
- `starlette` as a direct dependency (only `GuestAuth` used it).

```python
# before
guests = GuestAuth(wake, "https://chat.example.com/mcp")
wake.attach(server)
app = guests.wrap(server.streamable_http_app())
# now
wake.attach(server, identify=lambda ctx: Identity(user=my_user(ctx)))
app = server.streamable_http_app()  # behind your own sign-in
```

### Added

- `Call.tool`: the name of the tool being called. `wake.call(ctx)` reads it from the request where the context has it (else pass `tool=`), and `wake.call_of(tool=...)` takes it. The connector beacon Wakeshift adds to a result names that tool, and Wakeshift's hooks act on it only from that tool's result. The tools `attach` adds, and an event's `subscribe` and `unsubscribe` (new `tool=`), pass theirs.

### Changed

- An app's user has at most 20 live MCP Events webhook subscriptions. Wakeshift refuses one more with `403 subscription_limit`, which `attach` answers with MCP Events' `ResourceExhausted` (`INVALID_PARAMS` for ChatGPT), with data `{"kind": "subscriptions"}`.

## 0.2.0 (2026-10-26)

### Breaking

Sign in with Wakeshift is gone: Wakeshift no longer gives apps grants in people's own workspaces. Your app identifies its own users, with `GuestAuth` or its own sign-in, and acts for the agents in the workspaces it keeps (`wake.workspace`, `wake.add_agent`, `wake.check_agent`). Topics, events and `GuestAuth` work as before.

Removed:

- `WakeshiftSignIn` and `SignInError`.
- `Agent` and `Claims`.
- On `Wakeshift`: `protected_resource()`, `challenge()`, `verify_access_token()`, `exchange()`, `pkce()`, `authorize_url()`, `sign_in()`, `refresh()` and the `mcp_url` argument and attribute.
- `SignedIn` and `Connection`.
- `serve_events`' `agent` argument, the `agent` argument of `wake.call`, `event.subscribe` and `event.unsubscribe`, and `Call.agent_token`.
- Wakeshift refuses a manifest with `accounts: "wakeshift"`, `workspace` or `agent.redirect_uris` (`bad_manifest`).

`serve_events` is renamed `attach`, the one call that attaches Wakeshift to your MCP server. Its `user` argument is replaced by `identify`, which returns an `Identity` (`Identity(user=…)`, exported from `wakeshift`). It behaves as before:

```python
# before
wake.serve_events(server, user=lambda ctx: my_user(ctx))
# now
wake.attach(server, identify=lambda ctx: Identity(user=my_user(ctx)))
```
