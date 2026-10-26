"""App keys and the assertions they sign.

An app key from the Wakeshift console is the app's Ed25519 private key::

    wst_<50 base58 characters>

the 32-byte seed and a CRC32 checksum, naming no app: Wakeshift finds the app
by the key's ID (kid), which every assertion carries, and says which app it is
(``await wake.app()``). Older keys, which keep working, carry the app's ID and
a mode::

    wst_<test|live>_<app id>_<seed, base64url>

The key never goes over the wire: it signs short-lived assertions, and
Wakeshift keeps only the public half. The same rules as the Node and Go SDKs.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
import time
import zlib
from dataclasses import dataclass, field

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

_PREFIX = "wst_"
_APP_ID_RE = re.compile(r"^[a-z][a-z0-9-]{1,39}$")
_SEED_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_DIGITS = {c: i for i, c in enumerate(_ALPHABET)}
# A current key's body: 50 base58 characters, 36 bytes (58^50 > 2^288).
_LEN = 50

# Safe to show: none includes the key.
_ERR_PREFIX = "wakeshift: an app key starts with wst_"
_ERR_FORMAT = "wakeshift: an app key is wst_ followed by 50 letters and digits (or an older wst_test_… or wst_live_… key)"
_ERR_NO_APP_ID = "wakeshift: the app key has no app ID"
_ERR_MISTYPED = "wakeshift: the app key is cut short or mistyped"


def b64url(b: bytes) -> str:
    """base64url without padding."""
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def key_id(public_key: bytes) -> str:
    """A public key's kid: the first 16 bytes of its SHA-256, base64url (22
    characters). Wakeshift finds the key, and so the app, by it."""
    return b64url(hashlib.sha256(public_key).digest()[:16])


@dataclass(frozen=True)
class AppKey:
    #: "test" or "live" for an older key, None for a current one.
    #: Deprecated: the mode means nothing now.
    mode: str | None
    #: The app's ID for an older key, which names it; None for a current one
    #: (Wakeshift says: ``await wake.app()``).
    app_id: str | None
    #: The 32-byte seed the signing key is made from. Never send or log it.
    seed: bytes = field(repr=False)
    private_key: Ed25519PrivateKey = field(repr=False)
    #: The key's ID, which every assertion names it by.
    kid: str = ""


def _app_key(mode: str | None, app_id: str | None, seed: bytes) -> AppKey:
    priv = Ed25519PrivateKey.from_private_bytes(seed)
    pub = priv.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return AppKey(mode=mode, app_id=app_id, seed=seed, private_key=priv, kid=key_id(pub))


def parse_key(key: str) -> AppKey:
    """Reads an app key from the console, either format, after trimming
    whitespace. A current key's checksum is checked, so a mistyped key fails
    here, not at Wakeshift. Raises ValueError on anything else."""
    s = (key or "").strip()
    if not s.startswith(_PREFIX):
        raise ValueError(_ERR_PREFIX)
    rest = s[len(_PREFIX) :]
    if "_" in rest:
        # An older key (base58 has no "_").
        mode, _, after = rest.partition("_")
        if mode not in ("test", "live"):
            raise ValueError(_ERR_FORMAT)
        # IDs have no underscores; the seed (base64url) may.
        app_id, sep, enc = after.partition("_")
        if not sep or not _APP_ID_RE.match(app_id):
            raise ValueError(_ERR_NO_APP_ID)
        raw = b64url_decode(enc) if _SEED_RE.match(enc) else b""
        if len(raw) != 32:
            raise ValueError(_ERR_MISTYPED)
        return _app_key(mode, app_id, raw)
    if len(rest) != _LEN:
        raise ValueError(_ERR_MISTYPED)
    n = 0
    for c in rest:
        d = _DIGITS.get(c)
        if d is None:
            raise ValueError(_ERR_MISTYPED)
        n = n * 58 + d
    if n >= 1 << 288:
        raise ValueError(_ERR_MISTYPED)
    b = n.to_bytes(36, "big")
    seed = b[:32]
    if zlib.crc32(seed).to_bytes(4, "big") != b[32:]:
        raise ValueError(_ERR_MISTYPED)
    return _app_key(None, None, seed)


def public_key_of(k: AppKey) -> str:
    """The app's public key, base64url (what the console stores)."""
    return b64url(k.private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw))


def _b64json(v: object) -> str:
    return b64url(json.dumps(v, separators=(",", ":")).encode())


def sign_assertion(k: AppKey, core: str, now: float | None = None, app_id: str | None = None) -> str:
    """A fresh assertion of the app's identity for core (its base URL, no
    trailing slash): a compact JWS, alg EdDSA, typ app+jwt, living two minutes,
    naming the key by its kid. Its issuer and subject are the app's ID when
    it's known (an older key's, or app_id, as Wakeshift said), else the kid."""
    iat = int(time.time() if now is None else now)
    who = k.app_id or app_id or k.kid
    claims = {
        "iss": who,
        "sub": who,
        "aud": core + "/api/apps",
        "iat": iat,
        "exp": iat + 120,
        "jti": b64url(secrets.token_bytes(16)),
    }
    signing_input = _b64json({"alg": "EdDSA", "typ": "app+jwt", "kid": k.kid}) + "." + _b64json(claims)
    return signing_input + "." + b64url(k.private_key.sign(signing_input.encode("ascii")))
