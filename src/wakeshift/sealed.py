"""Signed values: base64url JSON, a ".", and its HMAC-SHA256 under key, so only
the app can make one and nobody can change one. Signed, not encrypted:
whoever holds one can read it. GuestAuth's clients, codes and tokens and
WakeshiftSignIn's cookie use these, each with its own key."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from typing import Any

from .key import b64url, b64url_decode

_B64URL_RE = re.compile(r"^[A-Za-z0-9_-]*$")


def seal(key: bytes, v: Any) -> str:
    """The signed form of v."""
    p = b64url(json.dumps(v, separators=(",", ":")).encode())
    return p + "." + b64url(hmac.new(key, p.encode("ascii"), hashlib.sha256).digest())


def unseal(key: bytes, s: Any) -> dict[str, Any] | None:
    """The JSON object s carries, if key signed it; None for anything else."""
    if not isinstance(s, str):
        return None
    i = s.find(".")
    if i < 0:
        return None
    p, sig = s[:i], s[i + 1 :]
    if not _B64URL_RE.match(p) or not _B64URL_RE.match(sig):
        return None
    try:
        got = b64url_decode(sig)
    except ValueError:
        return None
    # Only the canonical encoding: no second spelling of the same signature.
    if b64url(got) != sig:
        return None
    want = hmac.new(key, p.encode("ascii"), hashlib.sha256).digest()
    if not hmac.compare_digest(got, want):
        return None
    try:
        v = json.loads(b64url_decode(p).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    return v if isinstance(v, dict) else None
