"""The circuit breaker for the requests a tool call makes to Wakeshift.
Requests outside
tool calls (emits, watches) never pass through it."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

#: Once this many requests in a row made inside tool calls fail (unreachable,
#: a timeout, a 5xx), tool calls skip Wakeshift for BREAKER_PAUSE seconds,
#: then one probes. Read when used, so tests can shorten them.
BREAKER_FAILURES = 5
BREAKER_PAUSE = 30.0


class Breaker:
    """Counts in-call requests' outcomes in a row: BREAKER_FAILURES failures
    open it for BREAKER_PAUSE seconds, when every in-call request is skipped;
    then one request probes (others still skipped), and its outcome closes it
    or opens it again. clock is monotonic; tests replace it."""

    def __init__(self) -> None:
        self.clock: Callable[[], float] = time.monotonic
        self._lock = threading.Lock()
        self._failures = 0
        self._open_until: float | None = None
        self._probing = False

    @property
    def is_open(self) -> bool:
        """Whether it's open (skipping, or due a probe), without admitting anything."""
        with self._lock:
            return self._open_until is not None

    def admit(self) -> bool | None:
        """None: skip the request. True: it's the probe. False: a request as usual."""
        with self._lock:
            if self._open_until is None:
                return False
            if self._probing or self.clock() < self._open_until:
                return None
            self._probing = True
            return True

    def done(self, probe: bool, ok: bool | None) -> None:
        """A request's outcome: ok True (the core answered), False (it failed),
        None (neither: the caller canceled it, say)."""
        with self._lock:
            if probe:
                self._probing = False
                if ok is True:
                    self._failures, self._open_until = 0, None
                elif ok is False:
                    self._open_until = self.clock() + BREAKER_PAUSE
                return
            if ok is None or self._open_until is not None:
                return  # sent before it opened: the probe decides
            if ok:
                self._failures = 0
                return
            self._failures += 1
            if self._failures >= BREAKER_FAILURES:
                self._open_until = self.clock() + BREAKER_PAUSE
