"""event.watch, the in-process poll-then-emit-once helper for vendors whose
service has no usable webhook, and an event's on_subscribe, which runs once
for each new subscription whichever path made it
(sdk/typescript/test/watch.json pins both, the same in every SDK)::

    async def check() -> Crawl | None:
        s = await api.status(job)
        return Crawl(status=s.state, pages=s.pages) if s.done else None  # None: keep waiting

    crawl_finished.watch({"job": job}, check, every="10s", timeout="1h")

A watch lives in this process only: a restart, a deploy or a serverless
freeze loses it. Where the work can outlive the process, use the service's
webhook (a publish URL), or a queue or cron of your own.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import random
import secrets
import threading
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, Literal

from typing_extensions import TypeVar

from ._errors import APIConnectionError, APIError, IdReusedError, UsageError, usage

if TYPE_CHECKING:
    from ._client import Emitted, Recipient, Wakeshift
    from ._events import WakeEvent

#: The defaults and bounds (watch.json defaults).
DEFAULT_EVERY = 30.0
DEFAULT_TIMEOUT = 3600.0
DEFAULT_MAX_ERRORS = 5
MAX_CONCURRENT_CHECKS = 10
MIN_EVERY = 1.0
#: The longest wait after errors, unless every is longer.
_MAX_BACKOFF = 300.0
#: Jitter on each wait, at most this fraction either way (tests set 0).
_JITTER = 0.1
#: Every wait and the timeout are multiplied by this (tests shorten them).
_SCALE = 1.0

WatchOutcome = Literal["emitted", "timeout", "stopped", "skipped", "failed"]
WatchFailure = Literal["check", "errors", "emit"]
SubscribedVia = Literal["tool", "subscribe_events", "native"]

A = TypeVar("A", default=dict[str, Any])


@dataclass(frozen=True)
class WatchResult:
    """How a watch ended (the task event.watch returns):

    - ``emitted``: the check returned a payload and it was emitted
      (``emitted`` is the emit's result; None when another emit with the
      watch's id got there first, a webhook's, say).
    - ``timeout``: the timeout came before a payload.
    - ``stopped``: ``wake.aclose()`` stopped it, or the tool call that
      started it failed and its subscription was rolled back.
    - ``skipped``: nothing ran: a disabled client, a watch of the same event
      and arguments already running (its task is returned instead), or a
      tool call whose subscription to them came back unavailable.
    - ``failed``: ``reason`` is ``check`` (the check raised an error
      ``is_final`` marks final), ``errors`` (``max_errors`` errors in a row)
      or ``emit`` (the emit failed, and retrying wouldn't help); ``error`` is
      the last error."""

    outcome: WatchOutcome
    emitted: Emitted | None = None
    reason: WatchFailure | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class Subscribed(Generic[A]):
    """A new subscription, as an event's ``on_subscribe`` gets it: its
    arguments (the event's args model, or a dict), the subscriber's user
    (identify's; None without one), how it was made (``tool``,
    ``subscribe_events`` or ``native``) and, for ``tool``, the tool's name."""

    args: A
    user: str | None
    via: SubscribedVia
    tool: str | None = None
    #: The value the subscribing call passed as call.subscribe's ``context=``
    #: (via ``tool``), or, for a voucher redemption, the one the call that
    #: handed the voucher out passed, while this process still holds it
    #: (until the voucher is redeemed or expires; a redemption on another
    #: instance gets none). None otherwise. Never sent to Wakeshift, never
    #: logged or shown.
    context: Any = field(default=None, repr=False, compare=False)


def delay(every: float, errors_in_a_row: int) -> float:
    """How long a watch waits before its next check (watch.json delays):
    every after a pending check; after n errors in a row, every * 2^n, at
    most five minutes (or every, if longer)."""
    if errors_in_a_row <= 0:
        return every
    return min(every * 2**errors_in_a_row, max(every, _MAX_BACKOFF))


def _jittered(seconds: float) -> float:
    return seconds * (1 + random.uniform(-_JITTER, _JITTER)) if _JITTER else seconds


def _retryable(err: BaseException) -> bool:
    """An emit error worth retrying: no answer, rate limited, or Wakeshift's 5xx."""
    if isinstance(err, APIConnectionError):
        return True
    if isinstance(err, APIError) and not isinstance(err, IdReusedError):
        return err.code == "rate_limited" or (err.status is not None and (err.status == 429 or err.status >= 500))
    return False


class _Stopped(Exception):
    pass


class _TimedOut(Exception):
    pass


class _Watch:
    """One running watch: its task, and how to stop it."""

    def __init__(self) -> None:
        self.stop = asyncio.Event()
        self.task: asyncio.Task[WatchResult] | None = None
        #: Set while no check or emit is in flight (it's waiting): what the
        #: test client's aclose waits for. A new watch's first check is due at once.
        self.idle = asyncio.Event()


def _key(name: str, args: Mapping[str, Any]) -> tuple[str, str]:
    return name, json.dumps(args, sort_keys=True, separators=(",", ":"))


def start(
    event: WakeEvent[Any, Any],
    args: Any,
    check: Callable[[], Any],
    *,
    every: timedelta | int | float | str | None,
    timeout: timedelta | int | float | str | None,
    max_errors: int,
    is_final: Callable[[BaseException], bool] | None,
    id: str | None,
    to: str | Recipient | Sequence[str | Recipient] | None,
) -> asyncio.Task[WatchResult]:
    """WakeEvent.watch: the option checks, then the watch's task."""
    from ._events import _recipients
    from ._toolcall import _SCOPE, _seconds

    wake: Wakeshift = event._wake
    if not callable(check):
        raise usage("bad_arguments", f"watch of {event.name}: check is a function returning the payload, or None to keep waiting")
    every_s = float(_seconds("every", DEFAULT_EVERY if every is None else every) or DEFAULT_EVERY)
    timeout_s = float(_seconds("timeout", DEFAULT_TIMEOUT if timeout is None else timeout) or DEFAULT_TIMEOUT)
    if every_s < MIN_EVERY:
        raise usage("bad_arguments", f"watch of {event.name}: every is at least a second")
    if timeout_s < every_s:
        raise usage("bad_arguments", f"watch of {event.name}: timeout is at least every")
    if not isinstance(max_errors, int) or isinstance(max_errors, bool) or max_errors < 1:
        raise usage("bad_arguments", f"watch of {event.name}: max_errors is 1 or more")
    if is_final is not None and not callable(is_final):
        raise usage("bad_arguments", f"watch of {event.name}: is_final is a function of the check's error")
    _, a_json = event._parse_args(args)
    recipients = _recipients(event.name, to)
    if event.personal and not recipients:
        raise usage("bad_arguments", f"{event.name} is personal: pass to=, the subscribers its emit is for")
    if not event.personal and recipients:
        raise usage("bad_arguments", f"{event.name} isn't personal: its watch takes no to")
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        raise usage("config", f"watch of {event.name} needs a running event loop: call it from async code") from None

    def done(result: WatchResult) -> asyncio.Task[WatchResult]:
        async def now() -> WatchResult:
            return result

        return loop.create_task(now())

    if not wake.enabled:
        return done(WatchResult("skipped"))
    key = _key(event.name, a_json)
    running = wake._watches.get(key)
    if running is not None and running.task is not None and not running.task.done():
        return running.task
    scope = _SCOPE.get()
    if scope is not None and scope.attached._wake is wake and scope.states.get(key) in ("unavailable", "disabled"):
        return done(WatchResult("skipped"))
    w = _Watch()
    wid = id or "watch-" + secrets.token_hex(12)
    w.task = loop.create_task(_run(wake, event, a_json, check, w, every_s, timeout_s, max_errors, is_final, wid, recipients))
    wake._watches[key] = w

    def forget(_: Any) -> None:
        if wake._watches.get(key) is w:
            del wake._watches[key]

    w.task.add_done_callback(forget)
    if scope is not None and scope.attached._wake is wake and any(_key(c.name, c.arguments) == key for c in scope.created):
        # Started for a subscription this call made: a rollback stops it.
        scope.watches.append(w)
    return w.task


async def _run(
    wake: Wakeshift,
    event: WakeEvent[Any, Any],
    a_json: dict[str, Any],
    check: Callable[[], Any],
    w: _Watch,
    every: float,
    timeout: float,
    max_errors: int,
    is_final: Callable[[BaseException], bool] | None,
    wid: str,
    recipients: list[Recipient],
) -> WatchResult:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout * _SCALE
    sem = wake._loop().watch_checks

    async def bounded(aw: Awaitable[Any]) -> Any:
        """aw, unless the watch is stopped or the timeout comes first."""
        task = asyncio.ensure_future(aw)
        stopper = asyncio.ensure_future(w.stop.wait())
        try:
            remaining = deadline - loop.time()
            finished, _ = await asyncio.wait({task, stopper}, timeout=max(remaining, 0), return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            task.cancel()
            raise
        finally:
            stopper.cancel()
        if task in finished:
            return task.result()
        task.cancel()
        if w.stop.is_set():
            raise _Stopped
        raise _TimedOut

    async def pause(seconds: float) -> None:
        w.idle.set()
        try:
            await bounded(asyncio.sleep(_jittered(seconds) * _SCALE))
        finally:
            w.idle.clear()

    async def checked() -> Any:
        async with sem:  # at most MAX_CONCURRENT_CHECKS at once per client
            r = check() if _is_async(check) else await _in_daemon_thread(check)
            if inspect.isawaitable(r):
                r = await r
            return r

    def failed(reason: WatchFailure, err: BaseException) -> WatchResult:
        if reason == "check":
            # is_final marked the error final: the watch stopped as asked, nothing to fix.
            wake._log(
                "watch_stopped",
                f"the watch of {event.name} stopped: its check's error was marked final ({type(err).__name__})",
                event=event.name,
                error_type=type(err).__name__,
            )
            return WatchResult("failed", reason=reason, error=err)
        wake._log(
            "watch_failed",
            f"the watch of {event.name} ended ({reason}, {type(err).__name__})",
            event=event.name,
            reason=reason,
            error_type=type(err).__name__,
        )
        return WatchResult("failed", reason=reason, error=err)

    errors = 0
    try:
        while True:
            try:
                payload = await bounded(checked())
            except (_Stopped, _TimedOut):
                raise
            except Exception as err:
                if is_final is not None and is_final(err):
                    return failed("check", err)
                errors += 1
                if errors >= max_errors:
                    return failed("errors", err)
                await pause(delay(every, errors))
                continue
            errors = 0
            if payload is None:
                await pause(every)
                continue
            # Done: emit once, retrying only what retrying can fix.
            emit_errors = 0
            while True:
                try:
                    emitted = await bounded(event.emit(a_json, payload, id=wid, to=recipients or None))
                    return WatchResult("emitted", emitted=emitted)
                except (_Stopped, _TimedOut):
                    raise
                except IdReusedError:
                    return WatchResult("emitted", emitted=None)
                except (APIError, UsageError) as err:
                    if not _retryable(err):
                        return failed("emit", err)
                    emit_errors += 1
                    await pause(delay(every, emit_errors))
    except _Stopped:
        return WatchResult("stopped")
    except _TimedOut:
        wake._log("watch_timeout", f"the watch of {event.name} reached its timeout without a payload", event=event.name)
        return WatchResult("timeout")
    finally:
        w.idle.set()


async def _in_daemon_thread(fn: Callable[[], Any]) -> Any:
    """fn() in a thread of its own, as asyncio.to_thread runs it (in a copy
    of the contextvars), but a daemon one: a sync check still running never
    keeps the process alive once the server is done (a stdio server whose
    stdin ended), where the default executor would be waited for."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[Any] = loop.create_future()
    ctx = contextvars.copy_context()

    def settle(ok: bool, value: Any) -> None:
        if fut.done():
            return
        if ok:
            fut.set_result(value)
        else:
            fut.set_exception(value)

    def work() -> None:
        try:
            out, ok = ctx.run(fn), True
        except BaseException as err:  # noqa: BLE001 - handed to the awaiting task
            out, ok = err, False
        try:
            loop.call_soon_threadsafe(settle, ok, out)
        except RuntimeError:
            pass  # the loop closed while the check ran: nobody is waiting

    threading.Thread(target=work, name="wakeshift-watch-check", daemon=True).start()
    return await fut


def _is_async(fn: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(getattr(fn, "__call__", None))  # noqa: B004


def fire(
    wake: Wakeshift,
    event: WakeEvent[Any, Any],
    args: Any,
    user: str | None,
    via: SubscribedVia,
    tool: str | None = None,
    *,
    context: Any = None,
    run_in: contextvars.Context | None = None,
) -> None:
    """Runs event's on_subscribe for a new subscription, in the background:
    the request doesn't wait for it. An error is logged; the subscription
    stands. run_in: the contextvars of the tool call that subscribed (copied
    at call.subscribe), which on_subscribe runs in."""
    hook = event._on_subscribe
    if hook is None or not wake.enabled:
        return

    async def run() -> None:
        try:
            r = hook(Subscribed(args=args, user=user, via=via, tool=tool, context=context))
            if inspect.isawaitable(r):
                await r
        except Exception as err:
            wake._log(
                "on_subscribe_failed",
                f"on_subscribe for {event.name} raised {type(err).__name__}; the subscription stands",
                event=event.name,
                error_type=type(err).__name__,
            )

    task = asyncio.get_running_loop().create_task(run(), context=run_in.copy() if run_in is not None else None)
    wake._background.add(task)
    task.add_done_callback(wake._background.discard)


async def settle(wake: Wakeshift) -> None:
    """Waits until no watch on the current event loop has a check or an emit
    in flight (each is waiting for its next check, or done), and on_subscribe
    work started there has finished: the test client's aclose."""
    loop = asyncio.get_running_loop()
    while True:
        busy = [
            w.idle.wait()
            for w in list(wake._watches.values())
            if w.task is not None and w.task.get_loop() is loop and not w.idle.is_set()
        ]
        pending = [t for t in list(wake._background) if t.get_loop() is loop and not t.done()]
        if not busy and not pending:
            return
        await asyncio.gather(*busy, *pending, return_exceptions=True)
        await asyncio.sleep(0)  # what they started begins


async def stop_all(wake: Wakeshift) -> None:
    """Stops every watch on the current event loop, cancels on_subscribe work
    still running there, and waits for both (wake.aclose)."""
    loop = asyncio.get_running_loop()
    watches = [w for w in list(wake._watches.values()) if w.task is not None and w.task.get_loop() is loop]
    for w in watches:
        w.stop.set()
    background = [t for t in list(wake._background) if t.get_loop() is loop and not t.done()]
    for t in background:
        t.cancel()
    await asyncio.gather(*(w.task for w in watches if w.task is not None), *background, return_exceptions=True)
