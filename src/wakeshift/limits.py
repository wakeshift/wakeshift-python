"""Per-address limits for unauthenticated endpoints (GuestAuth's register and
token): who a request is from, behind trusted proxies, and how many requests
each address may make. In memory, per process."""

from __future__ import annotations

import ipaddress
from collections.abc import Iterable

# LIMITER_KEYS bounds a limiter's memory: past it, a new address is refused
# until old ones expire (the endpoints are unauthenticated, and IPv6 makes
# addresses cheap).
LIMITER_KEYS = 100_000


class RateLimiter:
    """At most max events per window (seconds) for each key, in memory, pruning
    idle keys as it goes. A negative max turns it off."""

    def __init__(self, max_events: int, window: float, keys: int = LIMITER_KEYS) -> None:
        self.max = max_events
        self.window = window
        self.keys = keys
        self._seen: dict[str, list[float]] = {}
        self._pruned = 0.0

    def _prune(self, now: float) -> None:
        cut = now - self.window
        for k in [k for k, ts in self._seen.items() if not ts or ts[-1] <= cut]:
            del self._seen[k]
        self._pruned = now

    def allow(self, key: str, now: float) -> bool:
        if self.max < 0:
            return True
        key = limit_key(key)
        cut = now - self.window
        if now - self._pruned > self.window:
            self._prune(now)
        if key not in self._seen and len(self._seen) >= self.keys:
            self._prune(now)
            if len(self._seen) >= self.keys:
                return False
        ts = [t for t in self._seen.get(key, []) if t > cut]
        if len(ts) >= self.max:
            self._seen[key] = ts
            return False
        ts.append(now)
        self._seen[key] = ts
        return True


def limit_key(addr: str) -> str:
    """What an address counts as for the limits: an IPv6 address's /64 (one host
    usually has a whole /64), anything else as it is."""
    a = addr.split("%", 1)[0]
    try:
        ip = ipaddress.ip_address(a)
    except ValueError:
        return addr
    if not isinstance(ip, ipaddress.IPv6Address) or ip.ipv4_mapped is not None:
        return addr
    groups = ip.exploded.split(":")
    return ":".join(format(int(g, 16), "x") for g in groups[:4]) + "::/64"


def _is_ip(s: str) -> bool:
    try:
        ipaddress.ip_address(s)
        return True
    except ValueError:
        return False


def _normalize(a: str) -> str:
    """An IPv4 address a dual-stack socket reports as ::ffff:a.b.c.d, as a.b.c.d."""
    if a.lower().startswith("::ffff:") and _is_ip(a[7:]) and "." in a[7:]:
        return a[7:]
    return a


def client_addr(headers: Iterable[tuple[bytes, bytes]], peer: str, hops: int) -> str:
    """The request's client address: with hops trusted proxies in front (each
    appending to X-Forwarded-For), the entry the outermost one added; entries
    before it are the client's own claims. headers are an ASGI scope's (the
    first X-Forwarded-For only, as Go's Header.Get reads it); peer is the
    connection's address."""
    forwarded = next((v.decode("latin-1") for k, v in headers if k.lower() == b"x-forwarded-for"), None)
    if forwarded and hops > 0:
        # Only an entry the proxies added: with fewer entries than hops, or one
        # that isn't an address, the header isn't the proxies' (a request that
        # reached the app some other way), so it's ignored.
        parts = forwarded.split(",")
        if len(parts) >= hops:
            ip = parts[len(parts) - hops].strip()
            if _is_ip(ip):
                return _normalize(ip)
    return _normalize(peer)
