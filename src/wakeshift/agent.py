"""Sign in with Wakeshift, for an app whose users sign in with their Wakeshift
accounts (manifest ``accounts: "wakeshift"``): MCP clients sign in at
Wakeshift and send the app an access token; the app verifies it
(``wake.verify_access_token``) and exchanges it for one it can act with (an
:class:`Agent`). The same as the Go SDK's App.Verify, App.Exchange and Agent,
and the Node SDK's verifyAccessToken, exchange and Agent."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey


@dataclass(frozen=True)
class Claims:
    """A verified access token's claims."""

    iss: str
    sub: str
    aud: str
    exp: int
    #: The person's workspace.
    wakeshift_tenant: str
    #: The agent the person signed in.
    wakeshift_agent: str
    client_id: str | None = None


@dataclass(frozen=True)
class Agent:
    """The app acting for one signed-in agent, through a token exchanged for the
    agent's own (so inside the consent its person gave). Pass it to
    serve_events' agent option, or to an event's subscribe and unsubscribe."""

    #: The person's workspace.
    workspace: str
    #: The agent's ID in that workspace.
    id: str
    #: The exchanged token: never log it or send it anywhere but Wakeshift.
    token: str = field(repr=False)
    #: When the exchanged token expires.
    expires_at: datetime = field(repr=False)

    def user(self) -> str:
        """The agent as a personal event's subscriber: its workspace and ID
        ("<workspace>/<id>"). Emit to it with ``to=agent.user()``."""
        return f"{self.workspace}/{self.id}"


def _b64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


class Keys:
    """Wakeshift's signing keys, fetched from its JWKS: when a key is unknown or
    over an hour old, at most once a minute. A failed fetch keeps the keys it had."""

    def __init__(self, fetch: Callable[[], Awaitable[Any]]) -> None:
        self._fetch = fetch
        self._keys: dict[str, Ed25519PublicKey] = {}
        self._fetched_at = 0.0
        self._tried_at = 0.0
        self._lock = asyncio.Lock()

    async def get(self, kid: str) -> Ed25519PublicKey | None:
        k = self._keys.get(kid)
        fresh = time.monotonic() - self._fetched_at < 3600
        tried = self._tried_at and time.monotonic() - self._tried_at < 60
        if (k is not None and fresh) or tried:
            return k
        async with self._lock:
            if self._tried_at and time.monotonic() - self._tried_at < 60:
                return self._keys.get(kid)
            self._tried_at = time.monotonic()
            try:
                doc = await self._fetch()
            except Exception:
                return self._keys.get(kid)
            nxt: dict[str, Ed25519PublicKey] = {}
            found = doc.get("keys") if isinstance(doc, dict) else None
            for j in found if isinstance(found, list) else []:
                if (
                    not isinstance(j, dict)
                    or j.get("crv") != "Ed25519"
                    or not isinstance(j.get("kid"), str)
                    or not isinstance(j.get("x"), str)
                ):
                    continue
                try:
                    raw = _b64(j["x"])
                except ValueError:
                    continue
                if len(raw) == 32:
                    nxt[j["kid"]] = Ed25519PublicKey.from_public_bytes(raw)
            if nxt:
                self._keys, self._fetched_at = nxt, time.monotonic()
            return self._keys.get(kid)


async def verify_access_token(keys: Keys, token: str, core: str, resource: str, now: float | None = None) -> Claims:
    """Verifies an access token an MCP client sent: signed by Wakeshift (keys from
    its JWKS), typ at+jwt, issued by core for resource (the app's MCP URL), not
    expired. Raises ValueError otherwise, with a short reason."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("invalid token")
    try:
        header = json.loads(_b64(parts[0]))
        raw = json.loads(_b64(parts[1]))
        sig = _b64(parts[2])
    except ValueError as err:
        raise ValueError("invalid token") from err
    if not isinstance(header, dict) or not isinstance(raw, dict):
        raise ValueError("invalid token")
    if header.get("alg") != "EdDSA" or header.get("typ") != "at+jwt" or not isinstance(header.get("kid"), str):
        raise ValueError("invalid token")
    key = await keys.get(header["kid"])
    if key is None:
        raise ValueError("invalid token")
    try:
        key.verify(sig, (parts[0] + "." + parts[1]).encode())
    except InvalidSignature as err:
        raise ValueError("invalid token") from err
    if raw.get("iss") != core:
        raise ValueError("token from another issuer")
    if raw.get("aud") != resource:
        raise ValueError("token for another app")
    exp = raw.get("exp")
    if not isinstance(exp, int) or int(now if now is not None else time.time()) >= exp:
        raise ValueError("token expired")
    return Claims(
        iss=raw["iss"],
        sub=str(raw.get("sub", "")),
        aud=raw["aud"],
        exp=exp,
        wakeshift_tenant=str(raw.get("wakeshift_tenant", "")),
        wakeshift_agent=str(raw.get("wakeshift_agent", "")),
        client_id=raw.get("client_id"),
    )
