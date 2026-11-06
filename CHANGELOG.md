# Changelog

What changed in each release of `wakeshift`. 0.4.0 is the first published release: to start with it, read the [README](README.md). Its notes below, and the earlier releases', are for apps that ran the preview builds.

## 0.10.0 (2026-11-06)

One release with TypeScript 0.11.0 and Go v0.10.0, per the SDK design (docs/sdk-design-next.md). It breaks APIs where that makes them simpler. Read "Who may subscribe" first.

### Breaking

**Who may subscribe (read this first).** An event that declares neither `personal`, `authorize` nor `public` is now subscribed only by your own tools: `subscribe_events` and native (MCP Events) subscriptions stop for it, and Wakeshift ends the ones made that way when your new manifest arrives (`event_closed` in your logs says how many). If that's what you meant (it usually is: job IDs, crawls, runs), do nothing. To keep other paths open, add `authorize` (gated) or `public=True` (anyone). Apps that haven't upgraded see no change.

```python
# Before: anyone could subscribe through subscribe_events or natively
wake.define_event("chat.message", description="A message", args=Channel, payload=Message, text=...)
# After: say so, if that's what you want
wake.define_event("chat.message", description="A message", args=Channel, payload=Message, public=True)
```

**Install.** `mcp` is an extra now, so an emit-only process installs nothing it doesn't need, and plain `wakeshift` installs beside FastMCP 2 or 3 (where `attach` works too, below).

```sh
pip install wakeshift            # before
pip install 'wakeshift[mcp]'     # after: MCPServer or the low-level Server
pip install 'wakeshift[fastmcp]' # after: FastMCP 4 (unchanged)
```

**Construction.** The key comes from `WAKESHIFT_KEY`; a missing key gives a disabled client instead of raising.

```python
wake = Wakeshift(os.environ["WAKESHIFT_KEY"])  # before
wake = Wakeshift.optional(os.environ.get("WAKESHIFT_KEY"))  # before, where Wakeshift was optional
wake = Wakeshift()  # after: disabled without a key
wake = Wakeshift(required=True)  # after: raise without one
wake = Wakeshift(http_client=httpx2.AsyncClient())  # after: was http=
```

**identify takes a `wakeshift.Request`,** may be async, and runs only when Wakeshift needs it. If it raises or takes over 10 seconds, Wakeshift's part of the request is refused.

```python
wake.attach(server, identify=lambda ctx: Identity(user=user_of(ctx.request.headers)))  # before
wake.attach(server, identify=lambda r: Identity(user=user_of(r.headers)))  # after
wake.attach(server, identify=identify, require_user=True)  # after: refuse anonymous callers
```

**Tools that subscribe declare their events; subscribing to an undeclared one raises.**

```python
@attached.tool()  # before: an undeclared subscribe warned
async def watch(room: str, call: ToolCall) -> str: ...


@attached.tool(subscribes=[ping])  # after: UsageError undeclared_subscribe otherwise
async def watch(room: str, call: ToolCall) -> str: ...
```

**Tools registered elsewhere use `wrap`.**

```python
TOOLS = [wrapped_tool(types.Tool(name="watch", ...))]            # before
await attached.call(ctx).subscribe(ping, {"room": room})

TOOLS = [types.Tool(name="watch", ...)]                           # after
wake.attach(server, wrap={"watch": [ping]})
await wake.current_call().subscribe(ping, {"room": room})
```

**`attached.call(ctx)` in an `MCPServer` tool** is `wake.current_call()`, with the tool named in `wrap` (or registered with `@attached.tool`).

**Built-in tools.** attach's `tools=` is now `builtin_tools=`, and each tool takes a `bool`.

```python
wake.attach(server, tools={"subscribe": {"enabled": False}, "wait": {"enabled": True}})  # before
wake.attach(server, builtin_tools={"subscribe": False, "wait": True})  # after
wake.attach(server, events=EventsOptions(native=f))  # before
wake.attach(server, is_native_client=f)  # after: f takes a Request
```

**Subscription.**

```python
if sub.created: ...          # before
if sub.native: ...
if sub.state == "created":   # after: created, renewed, native, unavailable (with reason) or disabled
if sub.state == "native":
```

`sub.session` and `call.session` are `SessionKey`s, which never print; `.reveal()` gives the key.

**Errors.**

```python
except WakeshiftError as err:          # before
    if is_code(err, "id_reused"): ...
    if err.status == 0: ...            # unreachable
except ArgsError: ...

except IdReusedError: ...              # after
except APIConnectionError: ...         # unreachable or timeout; an APIError, status None
except UsageError as err:              # caught before sending (also a ValueError): err.code, err.issues
    ...
```

`CallEnded` and `UndeclaredSubscribe` are `UsageError`s with codes `call_ended` and `undeclared_subscribe`.

**Names.** `Attached` is `Attachment`, `PublishUrl` is `PublishURL`, `PublishUrlSigning` is `PublishURLSigning`, `EventSubscriptionStatus` is `SubscriptionStatus`, and `event.publish_url(args)` is `event.create_publish_url(args)`.

**Warnings** go through `logging.getLogger("wakeshift")`, with a code in `extra`; `on_warning` is gone.

**Custom sentences.** `notifies(...)` is gone: the sentence comes from `subscribes`, always. Delete it from your descriptions.

**Removed from the public surface:** `wrapped_tool`, `notifies`, `is_code`, `ArgsError`, `CallEnded`, `UndeclaredSubscribe`, `EventsOptions`, `AttachTools`, `ToolOptions`, `WaitToolOptions`, `WakeOutput`, `AppKey`, `parse_key`, `sign_assertion`, `harness_agent`, `BUILTIN_TOOLS`, `EVENT_TOOL_TEXT`, `TOOL_OUTPUT_SCHEMAS` and every other exported constant, `Wakeshift.optional`, `wakeshift.fastmcp.access_token` (use `Request.token`). The modules behind `wakeshift` are private.

**tools/list is compact.** Wakeshift's argument descriptions are short; all three of its arguments appear only on tools that subscribe to something and `subscribe_events`, `session_key` alone on other tools that subscribe and `unsubscribe_events`, none on `list_subscriptions` (every tool still accepts all three); the built-ins' descriptions are short and their output schemas carry no descriptions; a tool's `wakeshift` output field is `{"type": "string"}`.

### Added

- **Subscribe vouchers: ChatGPT can subscribe to tools-only events.** When Wakeshift offers it (its manifest answer reports `subscribe_vouchers`), `call.subscribe` of a tools-only event from an MCP Events client (ChatGPT) returns `native` instead of `unavailable`, and the result's block hands the client a one-time voucher for exactly that event and those arguments. The client writes it back as the event argument `wakeshift_voucher`, which the SDK takes out of the arguments before anything reads them and passes to Wakeshift; `on_subscribe` then runs with `via="tool"` and your tool's name. `events/list` lists tools-only events, with a required `wakeshift_voucher`, only to native clients and only while Wakeshift offers vouchers, and the events capability is advertised whenever the server has an event. `native` may now come with no user (`user=""`) for an anonymous caller. Nothing to write. `wakeshift_voucher` is now a reserved argument name (`define_event` refuses it), `redact_secrets` redacts `wsv_` vouchers, and a call whose only Wakeshift work is native lines returns your result unchanged when Wakeshift can't add its line (logged as `native_unavailable`).
- **`event.watch(args, check, every=, timeout=, max_errors=, is_final=, id=, to=)`**, for services with no usable webhook: it calls `check` (async, or a plain function run in a worker thread) until it returns the payload (`None`: keep waiting), then emits once. The first check runs at once, then one every `every` (default 30 seconds, at least 1); a check that raises is retried with backoff (`every` × 2ⁿ, at most 5 minutes), failing after `max_errors` in a row (default 5) or at once on an error `is_final(err)` marks final; `timeout` (default 1 hour) covers the checks and the emit. The emit carries `id` (default: one per watch), the same on every retry, and is retried only while that can help (no answer, 429, 5xx); `id_reused` (a webhook got there first) counts as emitted. At most 10 checks run at once per client. A watch of the same event and arguments while one runs returns that one. It returns an `asyncio.Task` of a **`WatchResult`** (`outcome`: `emitted`, `timeout`, `stopped`, `skipped` or `failed`, with `emitted`, `reason` and `error`), which never raises unless you cancel it. It does nothing on a disabled client, or in a tool call whose subscription to those arguments came back unavailable; a watch a failed call started for a subscription it made stops when the call's subscriptions are rolled back. `wake.aclose()` stops every watch. In-process only: a restart or a serverless freeze loses it. New log codes `watch_failed` and `watch_timeout`.
- **`on_subscribe=` on `define_event`**: runs once for each new subscription, made by a tool (after the call succeeds), `subscribe_events` or a native client (`events/subscribe`, or the `events/poll` that started it), in the background, with a **`Subscribed`** (`args`, `user`, `via`, `tool`). Start `event.watch` there and every path is covered. An error logs `on_subscribe_failed`; the subscription stands. `wake.aclose()` cancels what's still running.
- **`wakeshift.testing`**: `TestClient(app="Test app")`, a client that's enabled and offline (a fake of Wakeshift in memory; it never reads `WAKESHIFT_KEY`), recording `emits`, `subscriptions`, `unsubscribes` and `publish_urls`; `tool_call(tool=, user=, session=, native=, context=, events=, wake=)`, a stand-in `ToolCall` (a subclass, so `call: ToolCall` type-checks) whose `subscribe` returns a real `Subscription` and records it; and `with_call(wake, call)`, inside which `wake.current_call()` returns `call`. `Wakeshift("")` stays off whatever the environment says.
- **`context=` on `call.subscribe`, and `Subscribed.context`.** Pass any value of yours (the caller's credential, say) and the event's `on_subscribe` gets it, so a watch started there can poll with the caller's credentials: no map of IDs to credentials of your own. It's never sent to Wakeshift, logged or shown. For a voucher redemption (ChatGPT), `on_subscribe` gets the context of the call that handed the voucher out while this process still holds it (in memory, by the voucher's hash, until it's redeemed or expires, at most 1,000); a redemption on another instance, `subscribe_events` and native subscriptions get `None`, so a watch that needs the caller's credentials can't run there: log it, or fall back to your status tool. `on_subscribe` for a tool's subscription now runs in a copy of the contextvars the subscribe ran in.
- **`wakeshift.testing`, round 3:** `wake.fail_next("subscribe" | "unsubscribe", "unreachable" | "refused")` and `call.fail_next(...)` make the next one fail (no answer: `APIConnectionError`; or Wakeshift refusing the key: `APIError` `unauthenticated`); the test client hands out subscribe vouchers as Wakeshift does (to a ChatGPT-shaped request, or a stand-in made with `native=True, wake=` a `TestClient`, recorded on `call.vouchers` as `TestVoucher`s), and `await wake.redeem(voucher, webhook_url)` subscribes with one as ChatGPT would, through the SDK's own redemption (a `TestRedemption`: the same webhook again refreshes, anything else raises `bad_voucher`); `await call.finish()` runs `on_subscribe` (with each subscribe's `context`) for what a stand-in call created, as after a real call, and `async with with_call(wake, call)` runs it for you (a plain `with` can't wait for it, so it doesn't, and warns with a `RuntimeWarning` when the call subscribed to an event with `on_subscribe`); each `TestSubscription` a stand-in call records carries its subscribe's `context`; `await wake.aclose()` waits for the watch checks in flight and their emits first, so an async check's emit shows in `emits`.
- **`event.authorize`** (and `event.on_subscribe`): the function `define_event` was given, for unit tests.
- **`BUILTIN_TOOL_NAMES`**: `("subscribe_events", "unsubscribe_events", "list_subscriptions", "wakeshift_wait")`.
- **`builtin_tools=False`** turns every built-in tool off.
- **FastMCP 2.12+ and 3.x.** `wake.attach` takes a FastMCP 2 or 3 server, with plain `wakeshift` installed beside it (never `[mcp]`): `@attached.tool(subscribes=...)`, `wrap=` with `current_call()`, the built-in tools and `identify` work as on FastMCP 4, and Wakeshift's arguments are taken out of every call before any middleware (added before `attach` or after), FastMCP's own logging or any handler sees them. Native MCP Events, protocol 2026-07-28 and the wait tool need FastMCP 4: on 2 and 3 every client gets the built-in tools and the block, and `builtin_tools={"wait": True}` raises `UsageError` `config`. Served through another server (`mount`), an attached server's tools that subscribe refuse to run, as on FastMCP 4. FastMCP before 2.12 raises `ImportError`.
- **The disabled client.** Without a key, `define_event` and `attach` work and add nothing, `emit` and `call.subscribe` run their checks and send nothing (`state="disabled"`), and calls that need Wakeshift's answer raise `UsageError` `disabled`.
- **`public=True`** on `define_event`, and the manifest's `subscribe` setting, which the core enforces too.
- **`wrap=`** on `MCPServer`, FastMCP 4 and the low-level `Server`, and **`wake.current_call(request=None)`**. `wrap` may be a function of a tool's name returning its events (or `None`), asked on every tools/list and tools/call, for tools named at run time. A function that returns anything but a list of this client's events for a tool logs `wrap_bad_event` (once per tool) and serves that tool unwrapped.
- **Sync tools.** `@attached.tool` takes a plain `def` that subscribes to nothing; it runs in a worker thread, as `MCPServer` and FastMCP run one. A tool that subscribes must be `async def`.
- **A `title` on Wakeshift's own tools**, the same as their annotations' title.
- **`require_user=True`** and **`wakeshift.Request`** (`headers`, the verified `token`, `client_name`, `raw`). `token` is an `mcp.server.auth.provider.AccessToken` (or FastMCP's subclass of it), or None.
- **`SessionKey`**, **`redact_secrets`**, **`encode_arg_value`** and **`decode_arg_value`**. `encode_arg_value` keeps `a-z 0-9 - _` as they are (so `dep-abc123` stays `dep-abc123`) and escapes with `.`: a capital becomes `.` and its lowercase letter, any other byte `..` and two hex digits, and a result over 100 characters `..h` and 40 hex digits of SHA-256, which doesn't decode.
- **`await wake.aclose()`** and `async with Wakeshift() as wake`; one client may be shared across threads and event loops.
- **`max_retries=`** (default 2): a request that never reached Wakeshift is sent again, and an emit without an `id` gets one of the SDK's, so a retry wakes nobody twice.
- **`replay=` and `once=` on `call.subscribe`**, with `replay` checked before sending (above zero, at most 24 hours); `replay` and `ttl` take a `timedelta`, seconds or `"30m"`.
- **`signing_secret=` on `create_publish_url`**, whose `signing` has the secret's fingerprint.
- **Output schemas on Wakeshift's own tools**, with matching `structuredContent`.
- `@attached.tool` without parentheses; it returns the function typed as itself. `WakeEvent` is generic, and `define_event` is typed for pydantic models and for JSON Schema dicts.
- `text` is optional: without it, the agent reads the description and the arguments.

### Changed

- **A tool without `subscribes` is a plain registration**, with or without a key: `@attached.tool()` no longer needs a `ToolCall` parameter (it's still filled in if the function takes one), and never raises for its absence. With a key it still takes `session_key`, so a tool that only unsubscribes (`call.unsubscribe`, or `wake.current_call().unsubscribe`) works as with `subscribes=[]`. A configuration error `attach` or `@attached.tool` raises is also logged, with the new code `config` (a framework may turn the raise into a bare 500).
- **Watches never keep a process alive.** A sync `check` runs in a daemon thread of its own (it ran in the default executor, which a stdio server waited for after stdin ended); an async one is cancelled with the event loop, as before. Call `await wake.aclose()` if your process keeps running after the server returns.
- **Log codes:** `identify_failed` is split into `identify_error` (identify raised, returned something else or took over 10 seconds; its fix now says to raise only when the upstream can't answer) and `user_required` (info: `require_user` refused a caller identify gave no user). `call.subscribe` raises the same code as the line: `identify_failed` when identify raised, failed or timed out, and the new error code `user_required` when `require_user` refused the caller (`identify_failed` before). Log lines and error messages use Python's names (`require_user=False`, `define_event`, `on_subscribe`, `builtin_tools`, `resolve_session`, "raise"). New `monitor_line_long` (warn, once per event): an emit's text over 200 bytes leaves the payload no room on Claude Code's 480-byte Monitor line. `watch_failed` no longer says "stopped without emitting" twice. A watch stopped by an error `is_final` marks final logs the new `watch_stopped` at info instead of a `watch_failed` warning: it stopped as asked (its result is still `failed`, reason `check`).
- **The SDK's own requests no longer log through `httpx2`** (one INFO line per request); the app's own requests, and how it configured those loggers, are untouched.
- **`events/list` lists a tools-only event with `delivery: ["webhook"]`**, and `events/poll` of one is refused with "… can't be polled: it's subscribed to only through this app's tools, with a webhook" (it named the voucher, which can't help a poll).
- **Built-in tools carry no `execution`** unless one of the server's own tools does.
- **A Wakeshift failure never changes your tool's result.** When Wakeshift's addition to a result of a tool that subscribes fails (an error answer, a timeout, the core unreachable, a refused key), the agent gets the result exactly as your tool gave it, with no error and nothing asking it to call again: the tool may have started paid or visible work. Before, the call failed with "...Try again.". What the call subscribed is still rolled back, and the new `block_failed` code logs it (warn, at most once per five minutes). The built-in tools' answers for no answer from Wakeshift say only what happened ("Wakeshift didn't answer in time", "Wakeshift couldn't be reached"), and the `timeout` error's fix no longer says to retry: "The outcome is unknown; an emit sent again with the same id emits once."
- **Claude Code is never an MCP Events client**, nor is any other harness Wakeshift wakes through a path of its own (a Monitor, hooks, a connector, a plugin or a background command; Capy's and Muse's clients included), recognized by its client name or `User-Agent`: its calls get Wakeshift's block even when they declare the MCP Events capability. ChatGPT is recognized by `openai-mcp` in its client name or `User-Agent`, `openai-mcp/1.0.0 (Codex)` from Work chats included.
- **`list_subscriptions` works without `identify`**: with no user it lists the calling conversation's subscriptions (its `session_key`, which `tools/list` now lists for it), and `unsubscribe_events` by `id` ends one of them; with a user, both work as before.
- **`list_subscriptions` and `unsubscribe_events` are listed only where something can subscribe**: a tool whose `subscribes` names an event, or an event open to `subscribe_events`.
- **The built-in tools go through your own `tools/list` pipeline**: they're added inside the server's `tools/list` handler (`MCPServer`, the low-level `Server`) or put in FastMCP's tool registry (FastMCP 2, 3 and 4), so middleware that amends every listed tool (`securitySchemes`, `_meta`) amends them too. Their calls are still answered by Wakeshift.
- **FastMCP 2/3: attach in any order.** A `tools/call` handler set on the low-level server after `attach` (your own validator, an instrumentation wrapper) gets Wakeshift's strip too, so it never sees `session_key`.
- **A call that subscribes and then unsubscribes the same thing** gets no Wakeshift block.
- **`call.session` is None until Wakeshift resolved the conversation** (a subscribe that reached it, or `resolve_session`); it's never the raw `session_key` argument, which a caller can forge.
- **`await call.resolve_session()` returns None when nothing names the conversation** (no `session_key`, no `conversation_id` and no harness conversation in the call's `_meta`, such as a Codex thread), without a request to Wakeshift; before, it started a new session. Call it directly to find your own record of the conversation: a passed key is checked first.
- **A one-line description without a final period gets one** before the subscribes sentence ("…information. This tool is how…").
- **Quieter logs**: `no_key` at debug, so a stdio server without a key prints nothing; `events_capability_kept` at info. `identify_failed` names the error's type once and ends with one fix clause.
- **A refused caller reads the same words as in every SDK**: "This app needs you to sign in before it can subscribe you to anything." (`require_user`) or "This app couldn't verify who you are, so it can't subscribe you to anything now." (identify failed).
- **attach works at the protocol layer.** Wakeshift's arguments are taken out of every tool call, before your middleware or the host sees them; its own tools are served by its middleware, so a handler set again after attach keeps them; a server that serves its own MCP Events keeps them.
- Registering a tool named like a built-in, one whose input names `session_key`, `harness` or `conversation_id`, or one whose output names `wakeshift`, raises `UsageError` `config`. One attach per server.
- The core URL must be `https://` (plain `http` only to localhost, 127.0.0.1 or ::1).
- A cancellation during a failed call's rollback reaches the caller.
- MCP Events (the `events` capability and its extension) is advertised whenever the server defines an event, tools-only ones included (an MCP Events client subscribes to those with the voucher a tool's result hands it), unless the server serves its own MCP Events; a server with no events advertises none. On mcp 2.x hosts (MCPServer, FastMCP 4, the low-level Server); FastMCP 2 and 3 serve no MCP Events.
- `no_identify` and `no_key` log at debug, not info.
- The subscribes sentence names your app exactly as its name is written ("shipyard", not "Shipyard").
- `subscribe_events`' and `unsubscribe_events`' `arguments` schema is `{"type": "object", "additionalProperties": true}` with its description, never `additionalProperties: {}`.
- A tool that subscribes whose description is over 2048 characters with Wakeshift's sentence logs `description_too_long` once per tool: Claude Code keeps only the first 2048.
- A tool's description keeps its whitespace (markdown, JSON examples): only trailing whitespace is dropped, and the subscribes sentence follows a blank line when the description has more than one line, else a space.

## 0.9.3 (2026-11-05)

### Changed

- **Text over 1000 bytes is trimmed by Wakeshift rather than refused; no need to trim it yourself.** Wakeshift cuts it between characters to 1000 bytes, ending with "…", and sends the occurrence. `emit` returns `Emitted.text_trimmed=True` when it happens, and `on_warning` hears it once per event. An emit's `id` still compares the text you sent, so a retry with the same long text is a duplicate.

## 0.9.2 (2026-11-04)

### Added

- **Complete MCP tool annotations on Wakeshift's own tools.** `subscribe_events`, `unsubscribe_events`, `list_subscriptions` and `wakeshift_wait` now list a title and all four hints (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) in tools/list, so app stores that require them, such as ChatGPT's, accept your server. `subscribe_events` and `unsubscribe_events`: idempotent, and not read-only, destructive or open-world. `list_subscriptions`: read-only and idempotent (as before), and now not destructive or open-world. `wakeshift_wait`: not read-only, destructive, idempotent or open-world. Before, only `list_subscriptions` had annotations, so clients read the others with the spec's defaults: destructive and open-world. Every SDK gives the same ones, on MCPServer, FastMCP and the low-level Server.

## 0.9.1 (2026-11-03)

### Fixed

- **A relayed beacon line never ends a wrapped tool's result.** If your tool's own text ended in a line that looked like a Wakeshift beacon (for example, a chat message someone posted, relayed in your result), the SDK kept that line last and dropped Wakeshift's own. An agent's hook acts on the last line, so someone could get a beacon minted for your tool, post it, and connect another person's conversation to their own session when that person's agent called the tool. The SDK now strips beacon lines from the end of your text and always puts Wakeshift's beacon last. Upgrade if any wrapped tool returns content other people wrote.
- **tools/list no longer stalls on the app-name lookup.** A slow or failing `GET /app` made every tools/list wait up to 20 seconds, and the failure wasn't remembered. The lookup now waits at most 3 seconds, waits 60 seconds before asking again after a failure, and keeps a name it was given.
- **Calls waiting on a failed manifest send share its error.** Concurrent calls each repeated a manifest send that had failed; they now get the first send's error.

## 0.9.0 (2026-11-03)

### Changed

- **`wakeshift_wait` is opt-in.** `attach` no longer serves Claude Code's wait tool unless you turn it on: `wake.attach(server, tools={"wait": {"enabled": True}})`. While a conversation waits on it, Claude Code keeps a request open to your server for about 20 seconds at a time and sends the next about a second later, for as long as it waits, whether or not anything arrives: on a serverless host, a function invocation per waiting conversation, all the time. This changes behavior for apps on today's defaults: a recent Claude Code in the terminal whose user declines Wakeshift's plugin now waits with a background monitor, as every other Claude Code does, rather than on your server. In manual permission mode that means approving the monitor, and each restart every 25 minutes, instead of one approval of `wakeshift_wait`; and the wait no longer carries on through `claude --continue`. Users who install the plugin see no difference. To keep the wait tool, add `tools={"wait": {"enabled": True}}`, and on a serverless host give the function a timeout of at least 30 seconds. Without it, `server/discover` no longer offers the tasks extension, tools/list no longer lists `wakeshift_wait`, and Wakeshift's `tasks/get` and `tasks/cancel` go on to your MCP SDK.
- **`tools` takes a mapping, each tool `{"enabled": bool}`**: `subscribe`, `list` and `unsubscribe` (on unless disabled, as before) and `wait` (off unless enabled). `tools={"subscribe": {"enabled": False}}` replaces `tools=["unsubscribe_events", "list_subscriptions"]`. New types: `AttachTools`, `ToolOptions`, `WaitToolOptions` (TypedDicts).

### Deprecated

- **`tools` as a list of names** (`BUILTIN_TOOLS`). It still works in this release, adding the tools it names and never the wait tool, and warns once through `on_warning`.

## 0.8.0 (2026-11-03)

### Added

- **`@attached.tool(subscribes=[...])`: a tool declares the events it subscribes to.** tools/list then adds a sentence to the tool's description, written from those events' own descriptions, the same in every SDK. For an app named Shipyard and an event described "A build finished: it passed, failed or was canceled.": "This tool is how Shipyard notifies you in this conversation of this event: A build finished: it passed, failed or was canceled. Shipyard delivers its notifications through Wakeshift; the result may ask you to start a background listener and to offer the user a one-time install. Follow it." Several events read "of these events: …; …". The app is named as the console has it ("this app" without a name). In wakebench's realistic tier this matched `notifies(…, when=…)`: Claude Code asked the user about the install 9/10 times in auto mode and 10/10 in manual, Codex 5/6. Write event descriptions as plain statements of what happened, since agents read them there. Works on `MCPServer` and FastMCP 4.

- **`IdReusedError`: an emit's `id` used for a different emit is refused.** Wakeshift deduplicated emits on `id` alone, so reusing an `id` by accident (a timestamp or a commit SHA two occurrences share) for another event, other arguments, recipients or payload dropped the second emit silently, and it returned the first's `matched`, reading as delivered. Now Wakeshift compares what each emit sends: a retry of the same emit with its `id` still sends nothing new and returns the first's result with `duplicate=True`, and the same `id` on a different emit is refused with `409 id_reused` and sends nothing. `emit` raises `IdReusedError` for it, a `WakeshiftError` with code `id_reused` (`is_code(err, "id_reused")` works too). Recipients compare as a set, so a retry may list them in another order, and `"u1"` and `["u1"]` are the same.

### Changed

- **`notifies()` is an override, for custom wording.** A tool whose description already has its sentence gets no second one from `subscribes`. A low-level `Server`'s tools (`wrapped_tool`, `attached.call(ctx)`) can't declare their events, so they keep using `notifies()`.

### Deprecated

- **A `call.subscribe` the tool doesn't declare.** A subscribe from a tool registered without `subscribes`, or for an event its `subscribes` doesn't list, still works in this release, but warns once per tool through `on_warning`, naming the tool and the fix (`@attached.tool(subscribes=[...])`). From the next release it raises `UndeclaredSubscribe` and subscribes nothing. `attached.call(ctx)` on a low-level `Server` isn't affected.

### Fixed

- **An emit to one user with nothing subscribed lists them in `unmatched`.** It returned `matched=0` and an empty `unmatched`, so the recipient was in neither; an emit to several listed them correctly. Every recipient is now in `matched`'s count, `unmatched` or `ended`, whether `to` names one or many.

## 0.7.0 (2026-11-03)

### Added

- **`notifies(description)`, for tools that subscribe.** It returns your tool's description with a few sentences after it, the same in every SDK. For an app named Shipyard, with `when` "a build finishes": "This tool is how Shipyard notifies you in this conversation when a build finishes. Shipyard delivers its notifications through Wakeshift; the result may ask you to start a background listener and to offer the user a one-time install. Follow it." The app is named as the console has it (tools/list asks Wakeshift for the name once); `app` names it yourself, and with no name it says "this app". `when` is optional. Use it on the tools that call `call.subscribe` (`@attached.tool(description=notifies("Watch a build until it finishes.", when="a build finishes"))`). In wakebench's realistic tier (a CI app, a user who never mentions Wakeshift), Claude Code set Wakeshift's part of a subscribing tool's result aside as something the user didn't ask for unless the tool's own description said the tool notifies through it. The Go SDK has the same as `appkit.Notifies` (`appkit.NotifyApp`, `appkit.NotifyWhen`).

### Fixed

- **Emitting before any agent connects works.** An emit at startup, or from a webhook, before any agent had connected to your MCP server failed with `404 unknown_event`: the manifest was sent only once `attach` had run and a call needed it. Now, when Wakeshift answers an emit with `unknown_event`, the SDK sends your manifest (a merge, `POST /manifest/merge`, so it never removes an event another process sent, and carries only the settings you passed to `publish_manifest`) and emits once more. Emits that fail together send it once, and an emit Wakeshift already knows the event for costs nothing extra. A process that only emits now sends its events this way too, when Wakeshift doesn't have them. Calling `publish_manifest()` at startup still works, but you no longer need it. One consequence: a running process that still defines an event you removed with `publish_manifest` adds it back on its next emit, so stop defining it first.
- **`unknown_event` says why.** Wakeshift's message now names the event and the likely cause (no manifest sent yet), instead of "no event by that name". The code is unchanged.

## 0.6.1 (2026-11-02)

### Fixed

- **An unsubscribe without the session key says so.** When a wrapped tool's `call.unsubscribe` ends nothing because the call didn't pass the conversation's `session_key` (Claude Code's calls name no conversation, so the key is the only way Wakeshift finds it), Wakeshift's block on that call now tells the agent that nothing ended, to call again with the key it was given when it subscribed, and not to subscribe again until it has. `call.unsubscribe` still returns `False`, and the note follows your tool's own text. Before, the agent saw only your text ("This chat wasn't watching…"), subscribed again without the key, and got every message twice. The built-in `unsubscribe_events` tool's reply carries the same note.

## 0.6.0 (2026-11-01)

### Added

- **FastMCP 4.** `wake.attach` takes a FastMCP server (`fastmcp.FastMCP`, 4.1 or later) as it takes an `MCPServer`, with the same calls and the same behavior: native events, the built-in tools, the wait tool for clients that take MCP tasks, and `@attached.tool()`, which registers a FastMCP tool with its options, `Context` parameter and dependencies. Your FastMCP middleware sees a tool's result before Wakeshift adds to it. `wakeshift.fastmcp.identify_by_claim` names the user by a claim of FastMCP's access token. Install with `pip install 'wakeshift[fastmcp]'`. A wrapped tool's structured result is checked against its output schema, as on `MCPServer`, so a result a client would refuse fails the call and ends its subscriptions. Attach the server you run: one mounted on another refuses its wrapped tools and warns. FastMCP 2.x and 3.x, built on `mcp` 1.x, are refused with a `TypeError`.
- **The MCP SDK's low-level `Server`.** `wake.attach` takes a `mcp.server.lowlevel.Server` too, after its handlers are set: it serves native events and the wait tool, and adds its own tools to your `tools/list` and `tools/call` handlers. Your `on_call_tool` handler subscribes with `attached.call(ctx)`, which returns the call's `ToolCall`; the reply gets Wakeshift's block, and a call that raises or returns `isError` ends what it subscribed. `wrapped_tool(tool)` adds `session_key`, `harness` and `conversation_id` to a tool's input schema, and `wakeshift` to its output schema. `attached.call(ctx)` works in a tool attach didn't register on `MCPServer` or FastMCP too.

### Changed

- **One HTTP stack: `httpx2`, as `mcp` 2.x uses.** The SDK talked to Wakeshift through `httpx` while `mcp` itself uses `httpx2`, so every install carried both. It now uses `httpx2` (2.10 or later), and installs three fewer packages (`httpx`, `httpcore`, `certifi`). If you pass your own client as `Wakeshift(http=…)`, make it an `httpx2.AsyncClient`. TLS certificates are now checked against your operating system's certificate store (on macOS and Windows through `truststore`, on Linux the system's CA bundle), or `SSL_CERT_FILE` / `SSL_CERT_DIR` if you set them, not `certifi`'s bundle: a CA your OS trusts, such as a corporate or local one, now works with nothing to configure.
- **`pydantic` 2.12 or later**, the version `mcp` 2.x already requires.
- **`tools/list` is cached per client.** `attach` changes `tools/list` by what each request declares (the wait tool, `subscribe_events`), so a `cacheScope` of `public` your server set (`cache_hints` on `MCPServer`, `cache_scope` on FastMCP) is answered as `private`. `ttlMs`, and every other result's hints, are unchanged.

## 0.5.2 (2026-10-30)

### Added

- **Claude Code is woken with nothing to install, and no restarts.** `attach` now serves `wakeshift_wait` to clients that take MCP tasks on protocol 2026-07-28: a recent Claude Code in the terminal, where Anthropic has turned them on. Wakeshift tells those conversations to call it after they subscribe; it returns at once and waits in the background, through `/compact` and restarts of Claude Code, and wakes the conversation when your events arrive. Other clients don't see it, and keep the instructions they had. Nothing to change in your code.

### Fixed

- **Agents that see only `structuredContent` are told how they're woken.** Some hosts, Claude Code among them, show the model a tool's `structuredContent` and not its text. A wrapped tool returning `str`, as the README's example does, got `structuredContent` (`{"result": …}`) from the MCP SDK without Wakeshift's instructions in it, so the agent said it was watching and was never woken. Now a wrapped tool annotated `-> str` replies with text only (no output schema; pass `structured_output=True` to keep MCP's), and every structured result of a wrapped tool carries the instructions in its `wakeshift` field, which `tools/list` adds to the tool's output schema. Subclassing `WakeOutput` still declares the field in your own type; it's no longer required. Nothing to change in your code.

## 0.5.1 (2026-10-30)

Never published to PyPI. Everything below first shipped in 0.5.2.

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
