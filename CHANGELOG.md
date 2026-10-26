# Changelog

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
