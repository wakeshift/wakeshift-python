"""Secrets and argument values (design §2.13, §4).

- ``SessionKey``: a conversation's session key, which never prints: its
  ``repr``, ``str`` and ``format`` are ``[redacted]``, pickling it is refused,
  and ``reveal()`` gives the key to the code that must send it.
- ``redact_secrets(text)``: every Wakeshift secret format in text replaced
  with ``[redacted]``, for paths the SDK can't reach (your own argument
  logging, HTTP body logs, Sentry's ``before_send``).
- ``encode_arg_value`` / ``decode_arg_value``: any string as a routing
  argument value (``a-z 0-9 . _ -``, at most 100 characters), injectively, so
  case-sensitive IDs stay apart; ``.`` is the escape, and ordinary IDs read
  as they are (sdk/typescript/test/arg-values.json).
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, NoReturn

from ._errors import usage

REDACTED = "[redacted]"


class SessionKey:
    """A conversation's session key (``wsc_…``, or a handle such as
    ``claude-code#2``). It never prints, in any printer: ``repr``, ``str``,
    ``format``, ``dataclasses.asdict``, ``json.dumps(default=str)``, logging
    and a traceback's locals (Sentry's ``include_local_variables``) all show
    ``[redacted]``, and pickling it raises. Compare keys with ``==``; call
    ``reveal()`` only to send one where it must go."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("wakeshift: a SessionKey is made from a str")
        object.__setattr__(self, "_value", value)

    def reveal(self) -> str:
        """The key itself: for sending it, never for printing it."""
        return self._value

    def __repr__(self) -> str:
        return f"SessionKey({REDACTED!r})"

    def __str__(self) -> str:
        return REDACTED

    def __format__(self, spec: str) -> str:
        return format(REDACTED, spec)

    def __bool__(self) -> bool:
        return bool(self._value)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, SessionKey) and other._value == self._value

    def __hash__(self) -> int:
        return hash(("SessionKey", self._value))

    def __setattr__(self, name: str, value: Any) -> NoReturn:
        raise AttributeError("wakeshift: a SessionKey can't be changed")

    def __reduce__(self) -> NoReturn:
        raise TypeError("wakeshift: a SessionKey isn't pickled: it's a secret")

    def __copy__(self) -> SessionKey:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> SessionKey:
        return self


def _session(value: Any) -> SessionKey | None:
    """A session key from Wakeshift's answer or a tool's argument (None for none)."""
    if isinstance(value, SessionKey):
        return value if value else None
    return SessionKey(value) if isinstance(value, str) and value else None


def _reveal(key: SessionKey | str | None) -> str | None:
    if isinstance(key, SessionKey):
        return key.reveal() or None
    return key or None


_TOKEN = r"(?:[A-Za-z0-9_\-+/=]|%[0-9A-Fa-f]{2})"
_SECRETS = re.compile(
    rf"(?:wst|wsc|wsp|whsec|wkt1|wsv)_{_TOKEN}{{6,}}"
    rf"|eyJ{_TOKEN}{{4,}}\.{_TOKEN}{{4,}}\.{_TOKEN}*"
    rf"|(^|%[0-9A-Fa-f]{{2}}|[^A-Za-z])(link=|link%3[dD])(?:[A-Za-z0-9_.\-+/=]|%[0-9A-Fa-f]{{2}})+"
    r'|("link"\s*:\s*")[^"\\]+"'
    r'|(\\"link\\"\s*:\s*\\")[^"\\]+\\"'
)


def _replace(m: re.Match[str]) -> str:
    if m.group(2) is not None:
        return (m.group(1) or "") + m.group(2) + REDACTED
    if m.group(3) is not None:
        return m.group(3) + REDACTED + '"'
    if m.group(4) is not None:
        return m.group(4) + REDACTED + '\\"'
    return REDACTED


def redact_secrets(text: str) -> str:
    """text with every Wakeshift secret replaced by ``[redacted]``: app keys and
    connector tickets (``wst_``), session keys (``wsc_``), publish URL tokens
    (``wsp_``), signing secrets (``whsec_``), wait-task IDs (``wkt1_``),
    subscribe vouchers (``wsv_``),
    compact JWS/JWT (``eyJ…``) and stream links (``link=``, ``link%3D``,
    ``"link":"…"``, whose key stays), bare, URL-encoded or JSON-escaped
    (sdk/typescript/test/secret-pattern.json)."""
    return _SECRETS.sub(_replace, text)


_MAX = 100
_PASS = frozenset(b"abcdefghijklmnopqrstuvwxyz0123456789-_")
_LOWER = "abcdefghijklmnopqrstuvwxyz"
_HEX = "0123456789abcdef"


def encode_arg_value(value: str) -> str:
    """Any string as a routing argument value, injectively, on its UTF-8
    bytes: ``a-z``, ``0-9``, ``-`` and ``_`` stay, so ordinary IDs read as
    they are; ``.`` is the escape: ``A-Z`` becomes ``.`` and the lowercase
    letter, and every other byte (``.`` included) ``..`` and two lowercase
    hex digits. A result over 100 characters becomes ``..h`` and 40 hex
    digits of SHA-256 (which can't be decoded). Raises UsageError
    bad_arguments for a string that isn't valid UTF-8 (a lone surrogate).
    Never lowercase a case-sensitive ID instead."""
    try:
        data = value.encode("utf-8")
    except UnicodeEncodeError:
        raise usage("bad_arguments", "encode_arg_value takes a well-formed string: this one has a lone surrogate") from None
    out: list[str] = []
    for b in data:
        if b in _PASS:
            out.append(chr(b))
        elif 0x41 <= b <= 0x5A:
            out.append("." + chr(b + 32))
        else:
            out.append(f"..{b:02x}")
    enc = "".join(out)
    if len(enc) > _MAX:
        return "..h" + hashlib.sha256(data).hexdigest()[:40]
    return enc


def decode_arg_value(value: str) -> str:
    """The string encode_arg_value encoded: strict, so anything that isn't a
    canonical encoding (or is the hashed form) raises UsageError
    bad_arguments. An authorize that takes encoded values decodes them with
    this, and refuses what doesn't decode."""
    raw = bytearray()
    out = ""
    i = 0
    n = len(value)
    ok = True
    while i < n and ok:
        c = value[i]
        if c in _LOWER or c in "0123456789-_":
            raw.append(ord(c))
            i += 1
        elif c == "." and i + 1 < n and value[i + 1] in _LOWER:
            raw.append(ord(value[i + 1]) - 32)
            i += 2
        elif c == "." and value[i + 1 : i + 2] == "." and i + 3 < n and value[i + 2] in _HEX and value[i + 3] in _HEX:
            raw.append(int(value[i + 2 : i + 4], 16))
            i += 4
        else:
            ok = False
    if ok:
        try:
            out = bytes(raw).decode("utf-8")
        except UnicodeDecodeError:
            ok = False
    if not ok or encode_arg_value(out) != value:
        # Never the value itself in the message.
        raise usage("bad_arguments", "decode_arg_value takes only what encode_arg_value gives (and can't decode its hashed form)")
    return out
