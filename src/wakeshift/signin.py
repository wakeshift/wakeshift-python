"""WakeshiftSignIn is the browser side of Sign in with Wakeshift for an app
whose users have Wakeshift accounts (manifest accounts: "wakeshift"): it sends
the person to Wakeshift to sign in and pick the agent, and on the way back
gives the app who signed in, a Wakeshift user ID that's the same across
sign-ins, devices and agents. Wakeshift refuses it for an app that keeps its
own users (GuestAuth's guests included).

It keeps no state: the PKCE verifier, the state and what the app wants carried
through (link) go in a signed, HttpOnly cookie for the redirect's path, good
for ten minutes. A port of the Node SDK's WakeshiftSignIn."""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import time
from collections.abc import Callable
from typing import Literal
from urllib.parse import parse_qsl, urlsplit

from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from .client import SignedIn, Wakeshift
from .key import b64url
from .sealed import seal, unseal

_TTL = 600  # seconds a sign-in may take
_COOKIE_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class SignInError(Exception):
    """Why a sign-in didn't finish: code is expired (no or old cookie), state
    (didn't start here), issuer (not from this Wakeshift), denied (the person or
    Wakeshift refused: see detail), or failed (the code exchange)."""

    def __init__(self, code: Literal["expired", "state", "issuer", "denied", "failed"], message: str, detail: str = "") -> None:
        super().__init__(message)
        self.code = code
        #: For denied: Wakeshift's OAuth error, e.g. access_denied.
        self.detail = detail


class WakeshiftSignIn:
    def __init__(
        self, wake: Wakeshift, redirect_uri: str, *, cookie: str = "wakeshift_signin", now: Callable[[], float] | None = None
    ) -> None:
        if wake is None:
            raise ValueError("wakeshift: WakeshiftSignIn needs wake")
        if not _COOKIE_RE.match(cookie):
            raise ValueError("wakeshift: the cookie name is letters, digits, _ and -")
        u = urlsplit(redirect_uri)
        if not u.scheme or not u.netloc:
            raise ValueError("wakeshift: redirect_uri is an absolute URL")
        self._wake = wake
        self.redirect_uri = redirect_uri
        self._cookie = cookie
        self._path = u.path or "/"
        self._secure = u.scheme == "https"
        self._now = now or time.time
        self._key = hmac.new(wake.secret_seed(), b"wakeshift-signin:state", hashlib.sha256).digest()

    async def start(self, *, link: str = "", agent_hint: str = "") -> Response:
        """Sends the person to Wakeshift's sign-in: a 302 with the cookie.
        link is carried through (signed, not encrypted: the browser can read it;
        at most 1000 characters). With a current key, the first call asks
        Wakeshift for the app's ID (wake.app())."""
        if len(link) > 1000:
            raise ValueError("wakeshift: link is at most 1000 characters")
        await self._wake._need_app_id()
        verifier, challenge = self._wake.pkce()
        state = b64url(secrets.token_bytes(16))
        pending = {"s": state, "v": verifier, "l": link, "e": int(self._now()) + _TTL}
        resp = RedirectResponse(
            self._wake.authorize_url(redirect_uri=self.redirect_uri, state=state, challenge=challenge, agent_hint=agent_hint),
            status_code=302,
            headers={"Cache-Control": "no-store"},
        )
        self._set_cookie(resp, seal(self._key, pending), _TTL)
        return resp

    async def finish(self, request: Request, response: Response) -> tuple[SignedIn, str]:
        """Finishes the sign-in on your callback route (the redirect URI): checks
        the cookie, the state and the issuer, then exchanges the code. Clears
        the cookie on response (the one you'll return) either way. Returns who
        signed in and the link; raises SignInError."""
        q = parse_qsl(request.url.query, keep_blank_values=True)

        def get(k: str) -> str | None:
            return next((v for kk, v in q if kk == k), None)

        p = unseal(self._key, request.cookies.get(self._cookie))
        self._set_cookie(response, "", 0)
        ok = p is not None and all(isinstance(p.get(k), str) for k in ("s", "v", "l")) and isinstance(p.get("e"), int)
        if not ok or int(self._now()) > p["e"]:  # type: ignore[index]
            raise SignInError("expired", "the sign-in didn't start here, or took too long")
        assert p is not None
        if not p["s"] or not hmac.compare_digest((get("state") or "").encode(), p["s"].encode()):
            raise SignInError("state", "the sign-in didn't start here")
        # RFC 9207: the response names its issuer, which must be this Wakeshift.
        if get("iss") != self._wake.core:
            raise SignInError("issuer", "the response didn't come from Wakeshift")
        err = get("error")
        if err:
            raise SignInError("denied", "the sign-in was refused", err)
        code = get("code")
        if not code:
            raise SignInError("failed", "the response has no code")
        try:
            signed_in = await self._wake.sign_in(code=code, verifier=p["v"], redirect_uri=self.redirect_uri)
        except Exception as e:
            raise SignInError("failed", "Wakeshift didn't finish the sign-in") from e
        return signed_in, p["l"]

    def _set_cookie(self, resp: Response, value: str, max_age: int) -> None:
        header = f"{self._cookie}={value}; Path={self._path}; Max-Age={max_age}; HttpOnly; SameSite=Lax" + (
            "; Secure" if self._secure else ""
        )
        resp.headers.append("Set-Cookie", header)
