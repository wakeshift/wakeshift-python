"""GuestAuth signs MCP clients (Claude Code, Codex…) in to a headless app as
guests: no Wakeshift account, no account with the app. It's a small OAuth
server inside the app (dynamic client registration, authorize with PKCE,
tokens), and it keeps no state: clients, codes and tokens are signed with a key
derived from the app's key. A port of the Node and Go SDKs' GuestAuth.

Each registered client (an install of a harness) is one guest: its own
workspace, with an agent named after the harness (claude-code). Sign-in shows
nothing: the person added the app to their agent, and that's the consent.

Since authorize has no user step, knowing a client_id must not be enough to
become its guest (client_ids travel in authorize URLs, which end up in logs and
browser history). Registration also gives the client a secret (derived from the
client_id, so nothing is stored), and the token endpoint requires it with every
code and refresh token. The guest's workspace and agent are made at the code
exchange, once the client has authenticated, not at authorize.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import inspect
import ipaddress
import json
import re
import secrets
import sys
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, unquote_to_bytes, urlencode, urlsplit, urlunsplit

from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, RedirectResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from .client import Wakeshift
from .errors import WakeshiftError
from .harness import harness_agent
from .key import b64url
from .limits import RateLimiter, client_addr
from .sealed import seal, unseal

#: How long an authorization code is good for, in seconds.
CODE_TTL = 60
# A PKCE code verifier, or an S256 challenge (RFC 7636 4.1, 4.2).
_PKCE_RE = re.compile(r"^[A-Za-z0-9._~-]{43,128}$")
_BEARER_RE = re.compile(r"^Bearer\s+(\S+)$", re.IGNORECASE)
_BASIC_RE = re.compile(r"^Basic\s+(\S+)$", re.IGNORECASE)
_BAD_ESCAPE_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")

#: The ASGI scope state key the guest a request signed in as is kept under.
STATE_KEY = "wakeshift_guest"


@dataclass(frozen=True)
class Guest:
    """Who a guest access token is for."""

    #: The app's ID for the guest: pass it as wake.call's user. guest:<id>, one per registered client.
    user: str
    #: The guest's workspace and agent (named after its harness: claude-code).
    workspace: str
    agent: str
    #: The client's name, e.g. "Claude Code".
    client: str
    expires: datetime


class _OAuthFailure(Exception):
    def __init__(self, status: int, code: str, message: str, headers: Mapping[str, str] | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.headers = dict(headers or {})


def guest_of(ctx: Any) -> Guest | None:
    """The guest a request came from, if GuestAuth signed it in: pass a tool's
    Context, a low-level request context, or a Starlette Request."""
    rc = getattr(ctx, "request_context", ctx)
    req = rc if isinstance(rc, Request) or isinstance(getattr(rc, "scope", None), Mapping) else getattr(rc, "request", None)
    scope = getattr(req, "scope", None)
    if isinstance(scope, Mapping):
        state = scope.get("state")
        g = state.get(STATE_KEY) if isinstance(state, Mapping) else None
        if isinstance(g, Guest):
            return g
        user = scope.get("user")
        token = getattr(user, "access_token", None)
        g = _guest_from_claims(getattr(token, "claims", None))
        if g:
            return g
    # MCPServer's own bearer auth (token_verifier=guests).
    try:
        from mcp.server.auth.middleware.auth_context import get_access_token
    except ImportError:  # pragma: no cover
        return None
    tok = get_access_token()
    return _guest_from_claims(getattr(tok, "claims", None)) if tok is not None else None


def _guest_from_claims(claims: Any) -> Guest | None:
    if not isinstance(claims, Mapping):
        return None
    g = claims.get(STATE_KEY)
    if not isinstance(g, Mapping) or not isinstance(g.get("user"), str):
        return None
    return Guest(
        user=g["user"],
        workspace=str(g.get("workspace", "")),
        agent=str(g.get("agent", "")),
        client=str(g.get("client", "")),
        expires=datetime.fromtimestamp(float(g.get("expires", 0)), tz=timezone.utc),
    )


def _guest_id(client_id: str) -> str:
    """The guest ID for a client: a hash of its client_id."""
    return b64url(hashlib.sha256(client_id.encode()).digest()[:18])


def _form_unescape(s: str) -> str | None:
    """Go's url.QueryUnescape: + is a space, %XX a byte; None if malformed."""
    if _BAD_ESCAPE_RE.search(s):
        return None
    try:
        return unquote_to_bytes(s.replace("+", " ")).decode("utf-8")
    except UnicodeDecodeError:
        return None


_BAD_REDIRECT = (
    "redirect URIs are https, http on loopback (localhost, 127.0.0.1, [::1]) "
    "or a private-use scheme (cursor://…), without a fragment"
)
_REDIRECT_SCHEME_RE = re.compile(r"[a-z][a-z0-9+.-]*")


def _valid_redirect(raw: str) -> bool:
    """https URLs, http on loopback (native apps, RFC 8252) and private-use
    schemes (cursor://…), never ones a browser would run or that carry a
    fragment, as the core's OAuth does."""
    i = raw.find(":")
    if i <= 0 or not _REDIRECT_SCHEME_RE.fullmatch(raw[:i]) or "#" in raw:
        return False
    scheme = raw[:i]
    try:
        u = urlsplit(raw)
        host = u.hostname or ""
    except ValueError:
        return False
    if scheme == "https":
        return host != ""
    if scheme == "http":
        if host == "localhost":
            return True
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return False
    return scheme not in ("javascript", "data", "file", "blob", "about", "vbscript")


def _first(pairs: list[tuple[str, str]], key: str) -> str | None:
    """A form's or query's first value for key (as URLSearchParams.get reads it)."""
    for k, v in pairs:
        if k == key:
            return v
    return None


def _with_params(url: str, params: Mapping[str, str]) -> str:
    """url with params set (each replacing any of the same name)."""
    parts = urlsplit(url)
    q = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in params]
    q.extend(params.items())
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))


async def _read_body(request: Request, limit: int) -> bytes:
    chunks: list[bytes] = []
    n = 0
    async for chunk in request.stream():
        n += len(chunk)
        if n > limit:
            raise ValueError("body too large")
        chunks.append(chunk)
    return b"".join(chunks)


def _json(status: int, v: Any, headers: Mapping[str, str] | None = None) -> Response:
    return JSONResponse(v, status_code=status, headers=dict(headers or {}))


def _oauth_err(status: int, code: str, desc: str, headers: Mapping[str, str] | None = None) -> Response:
    return _json(status, {"error": code, "error_description": desc}, headers)


def _text_err(status: int, msg: str) -> Response:
    return PlainTextResponse(msg + "\n", status_code=status)


class GuestAuth:
    """Guest sign-in for a headless app's MCP server.

    Serve its endpoints and check tokens with ``guests.wrap(app)`` (an ASGI
    middleware), or call ``handle``/``authenticate`` from your own routes. Inside
    a tool, ``guest_of(ctx)`` is who's calling."""

    def __init__(
        self,
        wake: Wakeshift,
        mcp_url: str,
        *,
        base: str | None = None,
        access_ttl: int = 3600,
        refresh_ttl: int = 30 * 24 * 3600,
        revoked: Callable[[Guest, datetime], bool | Awaitable[bool]] | None = None,
        grants: bool = False,
        proxy_hops: int = 0,
        register_limit: int = 20,
        token_limit: int = 60,
        rate_window: int = 600,
        on_error: Callable[[str, BaseException], None] | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        if wake is None:
            raise ValueError("wakeshift: GuestAuth needs wake")
        if not mcp_url:
            raise ValueError("wakeshift: GuestAuth needs mcp_url, the app's MCP URL")
        self._wake = wake
        self.mcp_url = mcp_url
        b = base if base is not None else (mcp_url[: -len("/mcp")] if mcp_url.endswith("/mcp") else mcp_url)
        self.base = b.rstrip("/")
        self._access_ttl = access_ttl if access_ttl > 0 else 3600
        self._refresh_ttl = refresh_ttl if refresh_ttl > 0 else 30 * 24 * 3600
        self._revoked = revoked
        self._grants = bool(grants)
        self._proxy_hops = proxy_hops
        self._now = now or time.time
        window = rate_window if rate_window > 0 else 600
        self._registers = RateLimiter(20 if register_limit == 0 else register_limit, window)
        self._tokens = RateLimiter(60 if token_limit == 0 else token_limit, window)
        self._on_error = on_error or (
            lambda m, err: print(m, f"{err.status} {err.code}" if isinstance(err, WakeshiftError) else repr(err), file=sys.stderr)
        )
        self._seed = wake.secret_seed()

    @property
    def resource_metadata_url(self) -> str:
        """Where MCP clients read the app's protected-resource metadata (RFC 9728)."""
        u = urlsplit(self.mcp_url)
        return f"{u.scheme}://{u.netloc}/.well-known/oauth-protected-resource"

    def _seconds(self) -> int:
        return int(self._now())

    # ── Serving ────────────────────────────────────────────────────────────

    async def handle(self, request: Request) -> Response | None:
        """Serves the OAuth endpoints and the metadata that points MCP clients at
        them. Returns None for any other request, for your own routes."""
        routes: dict[str, tuple[str, Callable[[Request], Any]]] = {
            "/.well-known/oauth-protected-resource": ("GET", self._resource_metadata),
            "/.well-known/oauth-protected-resource/mcp": ("GET", self._resource_metadata),
            "/.well-known/oauth-authorization-server": ("GET", self._metadata),
            "/oauth/register": ("POST", self._register),
            "/oauth/authorize": ("GET", self._authorize),
            "/oauth/token": ("POST", self._token),
        }
        r = routes.get(request.url.path)
        if r is None:
            return None
        method = "GET" if request.method == "HEAD" else request.method
        if method != r[0]:
            return PlainTextResponse(
                "method not allowed", status_code=405, headers={"Allow": "GET, HEAD" if r[0] == "GET" else r[0]}
            )
        try:
            out = r[1](request)
            return (await out) if inspect.isawaitable(out) else out
        except _OAuthFailure as err:
            return _oauth_err(err.status, err.code, str(err), err.headers)
        except Exception as err:
            self._on_error("wakeshift: guest sign-in failed", err)
            return _oauth_err(500, "server_error", "try again")

    def authenticate(self, request: Request) -> Guest | None:
        """The guest the request's bearer token is for, or None (answer with
        unauthorized(request) then)."""
        m = _BEARER_RE.match(request.headers.get("authorization", ""))
        return self.verify(m.group(1)) if m else None

    def unauthorized(self, request: Request) -> Response:
        """The 401 MCP clients start signing in from."""
        had = bool(_BEARER_RE.match(request.headers.get("authorization", "")))
        challenge = f'Bearer resource_metadata="{self.resource_metadata_url}"' + (', error="invalid_token"' if had else "")
        return PlainTextResponse(
            "sign in: add this app to your agent again", status_code=401, headers={"WWW-Authenticate": challenge}
        )

    def wrap(self, app: ASGIApp, *, protect: str = "/mcp") -> ASGIApp:
        """An ASGI app that serves the OAuth endpoints, requires a guest token for
        paths under protect (the MCP endpoint), and hands the rest to app. The
        guest is in the request's scope, where guest_of finds it."""

        async def asgi(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] != "http":
                await app(scope, receive, send)
                return
            request = Request(scope, receive)
            resp = await self.handle(request)
            if resp is not None:
                await resp(scope, receive, send)
                return
            path = scope.get("path", "")
            if path == protect or path.startswith(protect.rstrip("/") + "/"):
                g = self.authenticate(request)
                if g is None:
                    await self.unauthorized(request)(scope, receive, send)
                    return
                state = scope.get("state")
                if not isinstance(state, dict):
                    state = {}
                    scope["state"] = state
                state[STATE_KEY] = g
            await app(scope, receive, send)

        return asgi

    def verify(self, token: str) -> Guest | None:
        """Checks a guest access token; None if it isn't one of this app's, or has expired."""
        t = self._open("access", token)
        if not t or not isinstance(t.get("u"), str) or not isinstance(t.get("w"), str) or not isinstance(t.get("a"), str):
            return None
        e = t.get("e")
        if not isinstance(e, int) or isinstance(e, bool) or self._seconds() > e:
            return None
        return Guest(
            user=t["u"],
            workspace=t["w"],
            agent=t["a"],
            client=t["c"] if isinstance(t.get("c"), str) else "",
            expires=datetime.fromtimestamp(e, tz=timezone.utc),
        )

    async def verify_token(self, token: str) -> Any:
        """MCPServer's TokenVerifier: for ``MCPServer(token_verifier=guests, auth=…)``."""
        from mcp.server.auth.provider import AccessToken

        g = self.verify(token)
        if g is None:
            return None
        claims = {STATE_KEY: {**asdict(g), "expires": g.expires.timestamp()}}
        return AccessToken(
            token=token, client_id=g.user, scopes=[], expires_at=int(g.expires.timestamp()), resource=self.mcp_url, claims=claims
        )

    def _resource_metadata(self, _request: Request) -> Response:
        return _json(
            200, {"resource": self.mcp_url, "authorization_servers": [self.base], "bearer_methods_supported": ["header"]}
        )

    def _metadata(self, _request: Request) -> Response:
        b = self.base
        return _json(
            200,
            {
                "issuer": b,
                "authorization_endpoint": b + "/oauth/authorize",
                "token_endpoint": b + "/oauth/token",
                "registration_endpoint": b + "/oauth/register",
                "response_types_supported": ["code"],
                "grant_types_supported": ["authorization_code", "refresh_token"],
                "code_challenge_methods_supported": ["S256"],
                "token_endpoint_auth_methods_supported": ["client_secret_post", "client_secret_basic"],
            },
        )

    # ── Signed values ──────────────────────────────────────────────────────
    # Each kind (client, code, access, refresh) is signed with its own key
    # derived from the app's key, so one can't pass for another.

    def _mac(self, purpose: str) -> bytes:
        return hmac.new(self._seed, ("appkit-guest:" + purpose).encode(), hashlib.sha256).digest()

    def _sign(self, purpose: str, v: Any) -> str:
        return seal(self._mac(purpose), v)

    def _open(self, purpose: str, s: Any) -> dict[str, Any] | None:
        return unseal(self._mac(purpose), s)

    def _client_secret(self, client_id: str) -> str:
        """The secret registration gave a client: an HMAC of its client_id, so it's never stored and only the app can make it."""
        return b64url(hmac.new(self._mac("client-secret"), client_id.encode(), hashlib.sha256).digest())

    def _client_auth(self, request: Request, form: list[tuple[str, str]]) -> str | None:
        """The client a token request authenticated as, by HTTP Basic (RFC 6749
        2.3.1: each part form-encoded first) or client_id and client_secret in the
        form, if its secret is right."""
        basic = _BASIC_RE.match(request.headers.get("authorization", ""))
        if basic:
            try:
                raw = base64.b64decode(basic.group(1) + "=" * (-len(basic.group(1)) % 4)).decode("utf-8")
            except (ValueError, UnicodeDecodeError):
                return None
            colon = raw.find(":")
            if colon < 0:
                return None
            a = _form_unescape(raw[:colon])
            b = _form_unescape(raw[colon + 1 :])
            if a is None or b is None or _first(form, "client_secret"):
                return None  # one method at a time (RFC 6749 2.3)
            form_id = _first(form, "client_id")
            if form_id and form_id != a:
                return None
            ident, secret = a, b
        else:
            ident = _first(form, "client_id") or ""
            secret = _first(form, "client_secret") or ""
        if not ident or not secret or not self._open("client", ident):
            return None
        if not hmac.compare_digest(secret.encode(), self._client_secret(ident).encode()):
            return None
        return ident

    async def _register(self, request: Request) -> Response:
        peer = request.client.host if request.client else ""
        if not self._registers.allow(client_addr(request.scope.get("headers", []), peer, self._proxy_hops), self._now()):
            raise _OAuthFailure(429, "slow_down", "too many registrations from this address; try again later")
        try:
            inp = json.loads(await _read_body(request, 16 << 10) or b"null")
        except ValueError as err:
            raise _OAuthFailure(400, "invalid_client_metadata", "redirect_uris is required") from err
        if not isinstance(inp, dict):
            inp = {}
        uris = inp.get("redirect_uris")
        if not isinstance(uris, list) or not uris or len(uris) > 5 or not all(isinstance(u, str) for u in uris):
            raise _OAuthFailure(400, "invalid_client_metadata", "redirect_uris is required")
        for u in uris:
            if len(u) > 500 or not _valid_redirect(u):
                raise _OAuthFailure(400, "invalid_redirect_uri", _BAD_REDIRECT)
        raw_name = inp.get("client_name")
        name = raw_name.strip() if isinstance(raw_name, str) else ""
        if not name or len(name.encode()) > 80:
            name = "MCP client"
        ident = self._sign("client", {"n": name, "r": uris, "x": b64url(secrets.token_bytes(16))})
        return _json(
            201,
            {
                "client_id": ident,
                "client_secret": self._client_secret(ident),
                "client_secret_expires_at": 0,
                "client_name": name,
                "redirect_uris": uris,
                "token_endpoint_auth_method": "client_secret_post",
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"],
            },
            {"Cache-Control": "no-store"},
        )

    def _authorize(self, request: Request) -> Response:
        """Issues a code to a registered client with no user step (the person
        added the app to their agent). It creates nothing: the guest is made at
        the code exchange, once the client proves it holds its secret."""
        q = parse_qsl(request.url.query, keep_blank_values=True)
        client_id = _first(q, "client_id") or ""
        c = self._open("client", client_id)
        if not c or not isinstance(c.get("r"), list):
            return _text_err(400, "Unknown client. Register it again.")
        redirect = _first(q, "redirect_uri") or ""
        if redirect not in c["r"]:
            return _text_err(400, "That redirect URI isn't registered for this client.")
        state = _first(q, "state") or ""
        challenge = _first(q, "code_challenge") or ""
        if _first(q, "response_type") != "code" or not _PKCE_RE.match(challenge) or _first(q, "code_challenge_method") != "S256":
            # Said here, not by redirecting: anyone can register a client with
            # any https redirect, and there's no step for the person first, so
            # redirecting a bad request would make this an open redirect.
            return _text_err(400, "This sign-in request is incomplete: PKCE (S256) and response_type=code are required.")
        code = {"c": client_id, "r": redirect, "s": state, "p": challenge, "e": self._seconds() + CODE_TTL, "k": True}
        params = {"code": self._sign("code", code)}
        if state:
            params["state"] = state
        return RedirectResponse(_with_params(redirect, params), status_code=302)

    async def _token(self, request: Request) -> Response:
        peer = request.client.host if request.client else ""
        if not self._tokens.allow(client_addr(request.scope.get("headers", []), peer, self._proxy_hops), self._now()):
            raise _OAuthFailure(429, "slow_down", "too many token requests from this address; try again later")
        try:
            form = parse_qsl((await _read_body(request, 32 << 10)).decode("utf-8"), keep_blank_values=True)
        except (ValueError, UnicodeDecodeError) as err:
            raise _OAuthFailure(400, "invalid_request", "bad form") from err
        client_id = self._client_auth(request, form)
        if not client_id:
            raise _OAuthFailure(
                401,
                "invalid_client",
                "client authentication failed: send the client_id and client_secret registration gave you",
                {"WWW-Authenticate": 'Basic realm="oauth"'},
            )
        grant = _first(form, "grant_type")
        if grant == "authorization_code":
            c = self._open("code", _first(form, "code") or "")
            e = c.get("e") if c else None
            if (
                not c
                or c.get("k") is not True
                or not isinstance(e, int)
                or self._seconds() > e
                or c.get("c") != client_id
                or c.get("r") != (_first(form, "redirect_uri") or "")
            ):
                raise _OAuthFailure(400, "invalid_grant", "the code is invalid or expired")
            verifier = _first(form, "code_verifier") or ""
            got = b64url(hashlib.sha256(verifier.encode()).digest())
            if not _PKCE_RE.match(verifier) or not hmac.compare_digest(got.encode(), str(c.get("p")).encode()):
                raise _OAuthFailure(400, "invalid_grant", "PKCE verification failed")
            cl = self._open("client", client_id) or {}
            # One guest per registered client: its workspace, and its agent named after the harness.
            name = str(cl.get("n", ""))
            agent = harness_agent(name)
            user = "guest:" + _guest_id(client_id)
            try:
                workspace = await self._wake.workspace(user, "Guest (" + name + ")")
                if self._grants:
                    await self._wake.add_agent(workspace, agent, name)
            except WakeshiftError as err:
                if err.code == "workspace_limit":
                    raise _OAuthFailure(403, "access_denied", "this app can't take more guests right now") from err
                self._on_error("wakeshift: making a guest", err)
                raise _OAuthFailure(503, "temporarily_unavailable", "couldn't sign in; try again") from err
            except Exception as err:
                self._on_error("wakeshift: making a guest", err)
                raise _OAuthFailure(503, "temporarily_unavailable", "couldn't sign in; try again") from err
        elif grant == "refresh_token":
            t = self._open("refresh", _first(form, "refresh_token") or "")
            e = t.get("e") if t else None
            if (
                not t
                or not isinstance(e, int)
                or self._seconds() > e
                or t.get("i") != client_id
                or not isinstance(t.get("w"), str)
                or not isinstance(t.get("a"), str)
                or not isinstance(t.get("u"), str)
            ):
                raise _OAuthFailure(400, "invalid_grant", "the refresh token is invalid or expired")
            client_name = t["c"] if isinstance(t.get("c"), str) else ""
            guest = Guest(
                user=t["u"],
                workspace=t["w"],
                agent=t["a"],
                client=client_name,
                expires=datetime.fromtimestamp(e, tz=timezone.utc),
            )
            if self._revoked is not None:
                r = self._revoked(guest, datetime.fromtimestamp(float(t.get("t") or 0), tz=timezone.utc))
                if inspect.isawaitable(r):
                    r = await r
                if r:
                    raise _OAuthFailure(400, "invalid_grant", "the refresh token was revoked; sign in again")
            # The guest must still be there: forgetting the user (and, with
            # grants, revoking the grant) ends its refreshes.
            if not await self._still_there(t["u"], t["w"], t["a"], client_name):
                raise _OAuthFailure(400, "invalid_grant", "this guest's access was revoked; sign in again")
            workspace, agent, name, user = t["w"], t["a"], client_name, t["u"]
        else:
            raise _OAuthFailure(400, "unsupported_grant_type", "use authorization_code or refresh_token")
        now = self._seconds()
        access = self._sign("access", {"w": workspace, "a": agent, "c": name, "u": user, "e": now + self._access_ttl})
        # A new refresh token every time (rotation), dated, so revoked can
        # refuse those issued before a cutoff.
        refresh = self._sign(
            "refresh",
            {
                "w": workspace,
                "a": agent,
                "c": name,
                "u": user,
                "i": client_id,
                "t": now,
                "n": b64url(secrets.token_bytes(16)),
                "e": now + self._refresh_ttl,
            },
        )
        return _json(
            200,
            {"access_token": access, "token_type": "Bearer", "expires_in": self._access_ttl, "refresh_token": refresh},
            {"Cache-Control": "no-store"},
        )

    async def _still_there(self, user: str, workspace: str, agent: str, client: str) -> bool:
        """Whether the guest a refresh token names is still there: its workspace
        is the one Wakeshift keeps for its user (a forgotten user's is gone, so
        asking makes a new one, which is forgotten again at once), and, with
        grants, its agent's grant is live. A 503 when Wakeshift can't say."""
        try:
            if self._grants:
                await self._wake.check_agent(workspace, agent)
                return True
            if await self._wake.workspace(user, "Guest (" + client + ")") == workspace:
                return True
            await self._wake.forget_user(user)
            return False
        except WakeshiftError as err:
            if err.code in ("no_grant", "unknown_workspace", "workspace_limit"):
                return False
            self._on_error("wakeshift: checking a guest", err)
            raise _OAuthFailure(503, "temporarily_unavailable", "couldn't refresh; try again") from err
        except Exception as err:
            self._on_error("wakeshift: checking a guest", err)
            raise _OAuthFailure(503, "temporarily_unavailable", "couldn't refresh; try again") from err
