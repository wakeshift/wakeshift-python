# Changelog

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
