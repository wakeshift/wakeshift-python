"""Unit-testing tools that subscribe, without a network or a real key.

- ``TestClient(app="Test app")``: a ``Wakeshift`` that's enabled and
  offline. A small fake of Wakeshift answers it in memory, so ``attach``,
  ``tools/list`` (with Wakeshift's arguments and sentence), tool calls that
  subscribe (and their block), the built-in tools, ``emit``,
  ``create_publish_url`` and ``status`` all work. It records ``emits``,
  ``subscriptions``, ``unsubscribes`` and ``publish_urls``; ``clear()``
  empties them. It never reads ``WAKESHIFT_KEY`` or ``WAKESHIFT_CORE``.
- ``tool_call(...)``: a stand-in ``ToolCall`` for calling a tool's function
  directly; its ``subscribe`` returns a real ``Subscription`` and records it.
- ``with_call(wake, call)``: inside it, ``wake.current_call()`` returns
  ``call``, for a tool named in attach's ``wrap``; ``async with`` also runs
  the call's ``finish()`` when the block succeeds.
- ``wake.fail_next("subscribe", "unreachable")`` (or ``call.fail_next``):
  the next subscribe (or unsubscribe) fails, ``unreachable`` (no answer) or
  ``refused`` (Wakeshift refused the key), to test a tool's error path.
- Vouchers: a native stand-in call made with ``wake=`` a TestClient, or a
  ChatGPT-shaped request to the attached server, gets a subscribe voucher
  for a tools-only event; ``await wake.redeem(voucher, webhook_url)``
  subscribes with it as ChatGPT would, and runs the event's on_subscribe
  with the subscribing call's ``context=``.
- ``await wake.aclose()`` first waits for the watch checks in flight (and
  their emits) and on_subscribe work, so what they emit is in ``emits``.

::

    from wakeshift.testing import TestClient, tool_call, with_call

    wake = TestClient()
    crawl_finished = wake.define_event("crawl.finished", ...)

    async def test_start_crawl() -> None:
        call = tool_call(tool="start_crawl", user="u1")
        await start_crawl("https://example.com", call)
        assert call.subscriptions[0].event == "crawl.finished"

To keep an ordinary client inert in tests when ``WAKESHIFT_KEY`` is set in
the environment, pass an empty key: ``Wakeshift("")`` never reads it.
"""

from __future__ import annotations

import asyncio
import contextvars
import itertools
import json
import secrets
import warnings
import zlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal

from ._client import Recognized, Wakeshift
from ._errors import APIConnectionError, APIError, usage
from ._secrets import SessionKey
from ._toolcall import _SENTINEL, _TEST_CALL, MAX_REPLAY, Subscription, ToolCall, _seconds
from ._watch import fire, settle

if TYPE_CHECKING:
    from ._events import WakeEvent

__all__ = [
    "TestClient",
    "TestEmit",
    "TestPublishURL",
    "TestRedemption",
    "TestSubscription",
    "TestToolCall",
    "TestUnsubscribe",
    "TestVoucher",
    "tool_call",
    "with_call",
]

#: The core a TestClient names: never asked (nothing is sent).
TEST_CORE = "https://wakeshift.test"

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _test_key() -> str:
    """A well-formed app key from a fixed, public seed: it signs nothing anyone accepts."""
    seed = bytes(range(32))
    n = int.from_bytes(seed + zlib.crc32(seed).to_bytes(4, "big"), "big")
    out = ""
    for _ in range(50):
        n, d = divmod(n, 58)
        out = _B58[d] + out
    return "wst_" + out


@dataclass(frozen=True)
class TestEmit:
    """One emit a TestClient received: what Wakeshift would get."""

    __test__ = False

    event: str
    args: dict[str, Any]
    payload: dict[str, Any]
    text: str
    id: str | None = None
    #: The recipients of a personal event, as sent (a user, or a list).
    to: Any = None


@dataclass(frozen=True)
class TestSubscription:
    """One subscribe a TestClient (or a ``tool_call``) received. ``via`` is
    ``tool``, ``subscribe_events`` or ``native``; ``state`` is ``created``
    or ``renewed`` (``native`` for a stand-in call made with native=True)."""

    __test__ = False

    event: str
    args: dict[str, Any]
    tool: str | None
    user: str | None
    via: str
    state: str
    once: bool = False
    replay: float | None = None
    ttl: float | None = None
    reason: str | None = None
    #: The ``context=`` a ``tool_call``'s subscribe passed (None for none;
    #: a subscription made through a real tool call doesn't record it).
    context: Any = None


@dataclass(frozen=True)
class TestUnsubscribe:
    """One unsubscribe a TestClient received; ``reason`` is ``rolled_back``
    for a tool call that failed after subscribing."""

    __test__ = False

    event: str
    args: dict[str, Any]
    ended: bool
    reason: str | None = None


@dataclass(frozen=True)
class TestPublishURL:
    """One publish URL a TestClient made."""

    __test__ = False

    id: str
    event: str
    args: dict[str, Any]
    to: str | None = None
    signed: bool = False


@dataclass(frozen=True)
class TestVoucher:
    """One subscribe voucher a stand-in call handed out (``call.vouchers``):
    the event, its arguments and the voucher, for ``wake.redeem``."""

    __test__ = False

    event: str
    args: dict[str, Any]
    voucher: str


@dataclass(frozen=True)
class TestRedemption:
    """What ``wake.redeem`` did: the subscription's id, and whether it was
    created (``False``: the same webhook refreshed it)."""

    __test__ = False

    id: str
    created: bool


#: What fail_next takes.
FailOp = Literal["subscribe", "unsubscribe"]
FailError = Literal["unreachable", "refused"]

#: The core's one answer to every refused voucher.
_BAD_VOUCHER = "This voucher can't subscribe to this event: it was used, it expired, or it's for other arguments."
_B32 = "abcdefghijklmnopqrstuvwxyz234567"


def _key(event: str, args: Any) -> str:
    return event + json.dumps(args or {}, sort_keys=True, separators=(",", ":"))


def _canonical(args: Any) -> str:
    return json.dumps(args or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _check_failure(op: str, error: str) -> None:
    if op not in ("subscribe", "unsubscribe"):
        raise usage("bad_arguments", f'fail_next takes "subscribe" or "unsubscribe", not {op!r}')
    if error not in ("unreachable", "refused"):
        raise usage("bad_arguments", f'fail_next fails with "unreachable" or "refused", not {error!r}')


def _failure(wake: Wakeshift | None, error: str) -> Exception:
    """The error a request fails with: no answer, or Wakeshift refusing the key."""
    if error == "unreachable":
        return APIConnectionError(
            f"wakeshift: can't reach Wakeshift at {TEST_CORE} (fail_next). Check that WAKESHIFT_CORE, or the core option, "
            "is the address of the console your key came from.",
            code="unreachable",
        )
    if wake is not None:
        wake._log("key_refused", "Wakeshift refused this app's key (401)")
    return APIError("the test client refused this request (fail_next)", code="unauthenticated", status=401)


class TestClient(Wakeshift):
    """A Wakeshift client that's enabled and offline, for unit tests: a fake
    of Wakeshift in memory answers everything it asks, and records the
    emits, subscribes, unsubscribes and publish URLs."""

    __test__ = False

    def __init__(self, app: str = "Test app") -> None:
        super().__init__(_test_key(), core=TEST_CORE)
        self._app_id = "test-app"
        self._test_app_name = app
        #: Every emit, in order.
        self.emits: list[TestEmit] = []
        #: Every subscribe, in order (renewals too).
        self.subscriptions: list[TestSubscription] = []
        #: Every unsubscribe, in order (rollbacks too).
        self.unsubscribes: list[TestUnsubscribe] = []
        #: Every publish URL made, in order.
        self.publish_urls: list[TestPublishURL] = []
        self._live: dict[tuple[str, str], TestSubscription] = {}
        self._emitted: dict[str, tuple[str, dict[str, Any]]] = {}
        self._ids = itertools.count(1)
        self._fail: dict[str, str] = {}
        #: The vouchers handed out: their event, arguments, tool and options,
        #: and the webhook that redeemed each.
        self._vouchers: dict[str, dict[str, Any]] = {}

    def __repr__(self) -> str:
        return f"TestClient(app={self._test_app_name!r})"

    def clear(self) -> None:
        """Forgets everything recorded, and every live subscription."""
        self.emits.clear()
        self.subscriptions.clear()
        self.unsubscribes.clear()
        self.publish_urls.clear()
        self._live.clear()
        self._emitted.clear()
        self._fail.clear()
        self._vouchers.clear()

    def fail_next(self, op: FailOp, error: FailError) -> None:
        """Makes the next ``op`` (``subscribe`` or ``unsubscribe``, a tool's
        call.subscribe or call.unsubscribe) fail with ``error``:
        ``unreachable`` (no answer: APIConnectionError) or ``refused``
        (Wakeshift refused the key: APIError ``unauthenticated``, and
        ``key_refused`` logs as it would)."""
        _check_failure(op, error)
        self._fail[op] = error

    async def redeem(self, voucher: str, webhook_url: str) -> TestRedemption:
        """Subscribes with a voucher a tool call handed out, as ChatGPT would
        (MCP Events, a webhook): through the SDK's own redemption, so the
        event's on_subscribe runs (awaited here) with the subscribing call's
        ``context=``. The same webhook again refreshes the subscription
        (``created`` False); any other use raises APIError ``bad_voucher``,
        as Wakeshift answers."""
        from ._events import subscribe_native

        v = self._vouchers.get(voucher)
        if v is None:
            raise APIError(_BAD_VOUCHER, code="bad_voucher", status=403)
        ev = self._events[v["event"]]
        a_obj, a_json = ev._parse_args(v["args"])
        body: dict[str, Any] = {
            "call": {"client": "openai-mcp", "meta": {}},
            "name": ev.name,
            "arguments": a_json,
            "delivery": {"mode": "webhook", "url": webhook_url},
            "dialect": "chatgpt",
            "voucher": voucher,
        }
        out = await subscribe_native(self, ev, a_obj, None, body)
        await settle(self)
        return TestRedemption(id=str(out["id"]), created=bool(out.get("created")))

    async def aclose(self) -> None:
        """Waits for the watch checks in flight (and their emits) and the
        on_subscribe work to finish, so what they emit is in ``emits``, then
        stops every watch, as Wakeshift.aclose does."""
        await settle(self)
        await super().aclose()

    def _mint_voucher(self, event: str, args: dict[str, Any], tool: str, user: str | None, options: dict[str, Any]) -> str:
        """A voucher for one webhook subscription to event with args, handed out by tool."""
        token = "wsv_" + "".join(secrets.choice(_B32) for _ in range(32))
        self._vouchers[token] = {"event": event, "args": args, "tool": tool, "user": user, "options": options, "url": None}
        return token

    # The fake: answers the app API as Wakeshift would, in memory.

    async def _request(self, method: str, path: str, body: Any = None, *, timeout: float | None = None) -> dict[str, Any]:
        b: dict[str, Any] = body if isinstance(body, dict) else {}
        raw_call = b.get("call")
        call: dict[str, Any] = raw_call if isinstance(raw_call, dict) else {}
        conversation = str(call.get("session") or call.get("user") or "test-conversation")
        n = next(self._ids)
        op = {"/event-subscriptions": "subscribe", "/event-subscriptions/unsubscribe": "unsubscribe"}.get(path)
        if op is not None and b.get("reason") != "rolled_back" and op in self._fail:
            raise _failure(self, self._fail.pop(op))
        if method == "GET" and path == "/app":
            return {"id": "test-app", "name": self._test_app_name, "key": {"key_id": "test", "name": "test"}}
        if path in ("/manifest", "/manifest/merge"):
            # It hands out subscribe vouchers, as Wakeshift does.
            return {**b, "features": ["subscribe_vouchers"]}
        if path == "/events/emit":
            sent = {k: b.get(k) for k in ("name", "arguments", "payload", "text", "to")}
            if b.get("id") in self._emitted:
                first, earlier = self._emitted[b["id"]]
                if earlier != sent:
                    raise APIError("this id was used for a different emit", code="id_reused", status=409)
                return {"matched": 0, "duplicate": True}
            given = b.get("id") if not str(b.get("id") or "").startswith("sdk-") else None  # the SDK's own retry id
            self.emits.append(
                TestEmit(b["name"], b.get("arguments") or {}, b.get("payload") or {}, b.get("text") or "", given, b.get("to"))
            )
            if b.get("id"):
                self._emitted[b["id"]] = (b["id"], sent)
            k = _key(b["name"], b.get("arguments"))
            return {"matched": sum(1 for (_, kk) in self._live if kk == k), "duplicate": False}
        if path == "/events/subscribe" and isinstance(b.get("voucher"), str):
            return self._redeemed(b, n)
        if path in ("/event-subscriptions", "/events/subscribe"):
            native = path == "/events/subscribe"
            tool = call.get("tool") if not native else None
            via = "native" if native else "subscribe_events" if tool == "subscribe_events" else "tool"
            lk = (conversation, _key(b["name"], b.get("arguments")))
            created = lk not in self._live
            rec = TestSubscription(
                event=b["name"],
                args=b.get("arguments") or {},
                tool=None if via != "tool" else tool,
                user=call.get("user"),
                via=via,
                state="created" if created else "renewed",
                once=bool(b.get("once")),
                replay=b.get("replay"),
                ttl=b.get("ttl"),
                reason=b.get("reason"),
            )
            self.subscriptions.append(rec)
            self._live[lk] = rec
            if native:
                return {
                    "id": f"sub_test_{n}",
                    "refresh_before": "2099-01-01T00:00:00Z",
                    "cursor": "c0",
                    "truncated": False,
                    "created": created,
                }
            out: dict[str, Any] = {
                "event_subscription": {
                    "id": f"sub_test_{n}",
                    "user": call.get("user"),
                    "created": created,
                    "arguments": rec.args,
                },
                "session": str(call.get("session") or "wsc_test"),
                "agent": {"harness": call.get("harness") or "", "via": "test"},
                "pending": b.get("pending") or "pa_test",
            }
            if not b.get("defer_addition"):
                out["addition"] = {"text": "Wakeshift (test client): this conversation is subscribed."}
            return out
        if path in ("/event-subscriptions/unsubscribe", "/events/unsubscribe"):
            lk = (conversation, _key(b["name"], b.get("arguments")))
            ended = self._live.pop(lk, None) is not None
            self.unsubscribes.append(TestUnsubscribe(b["name"], b.get("arguments") or {}, ended, b.get("reason")))
            return {"ended": ended}
        if path == "/calls/session":
            return {"session": str(call.get("session") or "wsc_test"), "pending": "pa_test", "agent": {"via": "test"}}
        if path == "/calls/addition":
            lines = []
            for line in b.get("native") or []:
                name, args = line.get("name"), line.get("arguments") or {}
                ev = self._events.get(name)
                if ev is not None and ev.access == "tools":
                    options = {k: line[k] for k in ("once", "ttl", "replay") if k in line}
                    token = self._mint_voucher(name, args, str(call.get("tool") or ""), call.get("user"), options)
                    lines.append(
                        f"To be woken by {name} with {_canonical(args)}, subscribe to it (MCP Events) with wakeshift_voucher "
                        f"set to {token} in its arguments. It works once, for these arguments."
                    )
                else:
                    lines.append(f"To be woken by {name} with {_canonical(args)}, subscribe to it (MCP Events).")
            if lines:
                return {"text": "\n".join(lines)}
            return {"text": "Wakeshift (test client): this conversation is woken when its events happen."}
        if path == "/event-subscriptions/list":
            who = b.get("user")
            subs = [
                {
                    "id": f"sub_test_{i}",
                    "event": s.event,
                    "arguments": s.args,
                    "destination": "conversation",
                    "created_at": "2026-01-01T00:00:00Z",
                }
                for i, ((conv, _), s) in enumerate(self._live.items(), 1)
                if (s.user == who if who else conv == conversation)
            ]
            return {"subscriptions": subs}
        if path == "/event-subscriptions/end":
            return {"ended": False}
        if path == "/events/publish-urls" and method == "POST":
            rec_p = TestPublishURL(f"pu_test_{n}", b["name"], b.get("arguments") or {}, b.get("to"), "signing_secret" in b)
            self.publish_urls.append(rec_p)
            return {"id": rec_p.id, "url": f"{TEST_CORE}/p/test_{n}", "event": b["name"]}
        if method == "DELETE" and path.startswith("/events/publish-urls/"):
            return {}
        if path == "/events/status":
            k = _key(b.get("name", ""), b.get("arguments"))
            count = sum(1 for (_, kk) in self._live if kk == k)
            if "subscriptions" in b:
                return {
                    "subscriptions": [
                        {"id": r.get("id"), "user": r.get("user"), "live": False, "connected": False} for r in b["subscriptions"]
                    ]
                }
            return {"subscriptions": count, "connected": 0, "via": {}}
        if path == "/events/poll":
            lk = (conversation, _key(b["name"], b.get("arguments")))
            created = lk not in self._live
            if created:
                self._live[lk] = TestSubscription(
                    b["name"], b.get("arguments") or {}, None, call.get("user"), "native", "created"
                )
                self.subscriptions.append(self._live[lk])
            return {
                "events": [],
                "cursor": "c0",
                "truncated": False,
                "has_more": False,
                "next_poll_ms": 10000,
                "created": created,
            }
        if path == "/users/forget":
            return {"forgotten": False}
        raise APIError(f"the test client doesn't answer {method} {path}", code="unsupported", status=404)

    def _redeemed(self, b: dict[str, Any], n: int) -> dict[str, Any]:
        """events/subscribe with a voucher, as Wakeshift answers it: the first
        use creates the handing-out tool's subscription; the same webhook
        again refreshes it; anything else is refused, with one answer."""
        v = self._vouchers.get(b["voucher"])
        url = (b.get("delivery") or {}).get("url")
        if v is None or v["event"] != b.get("name") or _canonical(v["args"]) != _canonical(b.get("arguments")):
            raise APIError(_BAD_VOUCHER, code="bad_voucher", status=403)
        if v["url"] is not None and v["url"] != url:
            raise APIError(_BAD_VOUCHER, code="bad_voucher", status=403)
        created = v["url"] is None
        if created:
            v["url"], v["id"] = url, f"sub_test_{n}"
        o = v["options"]
        rec = TestSubscription(
            event=v["event"],
            args=dict(v["args"]),
            tool=v["tool"],
            user=v["user"],
            via="tool",
            state="created" if created else "renewed",
            once=bool(o.get("once")),
            replay=o.get("replay"),
            ttl=o.get("ttl"),
        )
        self.subscriptions.append(rec)
        self._live[("voucher:" + str(v["id"]), _key(v["event"], v["args"]))] = rec
        return {
            "id": v["id"],
            "refresh_before": "2099-01-01T00:00:00Z",
            "cursor": "c0",
            "truncated": False,
            "created": created,
            "via": "tool:" + v["tool"],
            "tool": v["tool"],
        }


class TestToolCall(ToolCall):
    """A stand-in for the ToolCall a tool that subscribes gets, for calling
    the tool's function directly in a unit test (``tool_call()`` makes one).
    ``subscribe`` runs the same local checks as a real call (the event's
    arguments, ``replay`` and ``ttl``; the declared events, if ``events``
    was given) and returns a ``Subscription``: ``created`` the first time
    for an event and arguments, ``renewed`` after, ``native`` for a native
    call. It records ``subscriptions`` and ``unsubscribes``. ``session`` is
    None until the first subscribe or ``resolve_session``, as on a real call.

    A native call made with ``wake=`` a TestClient hands out a subscribe
    voucher for a tools-only event, as Wakeshift does (``vouchers``; redeem
    one with ``wake.redeem``). ``await call.finish()`` runs each event's
    on_subscribe for the subscriptions the call created, as a real call does
    once the tool succeeded (``async with with_call(...)`` runs it for you).
    ``fail_next`` makes its next subscribe or unsubscribe fail."""

    __test__ = False

    def __init__(
        self,
        *,
        tool: str,
        user: str | None,
        session: str | None,
        native: bool,
        context: Any,
        events: Sequence[WakeEvent[Any, Any]] | None,
        wake: Wakeshift | None,
    ) -> None:
        super().__init__(
            _SENTINEL,
            scope=None,
            context=context,
            user=user,
            tool=tool,
            native=native,
            native_conversation=None,
            session=SessionKey(session) if session else None,
            harness=None,
            conversation=None,
            declared=list(events) if events is not None else None,
            checked=events is not None,
            wake=wake,
        )
        self._given_session = session or "wsc_test"
        #: Every subscribe, in order.
        self.subscriptions: list[TestSubscription] = []
        #: Every unsubscribe, in order.
        self.unsubscribes: list[TestUnsubscribe] = []
        self._seen: set[str] = set()
        self._n = 0
        #: Every subscribe voucher handed out (a native call with a TestClient), in order.
        self.vouchers: list[TestVoucher] = []
        self._fail: dict[str, str] = {}
        #: The subscriptions the call created and still holds: the event, its
        #: parsed arguments, the subscribe's context= and contextvars.
        self._created: dict[str, tuple[WakeEvent[Any, Any], Any, Any, contextvars.Context]] = {}

    def fail_next(self, op: FailOp, error: FailError) -> None:
        """Makes the call's next ``op`` (``subscribe`` or ``unsubscribe``) fail
        with ``error`` (``unreachable`` or ``refused``), as
        ``TestClient.fail_next`` does."""
        _check_failure(op, error)
        self._fail[op] = error

    async def finish(self) -> None:
        """The tool succeeded: runs each event's on_subscribe (via ``tool``,
        this call's tool and user, the subscribe's ``context=``, in the
        contextvars the subscribe ran in) for the subscriptions the call
        created and still holds, as a real call does after the tool
        returns, and waits for them. Once per subscription."""
        created, self._created = self._created, {}
        wakes: list[Wakeshift] = []
        for event, a_obj, context, run_in in created.values():
            fire(event._wake, event, a_obj, self.user, "tool", self.tool, context=context, run_in=run_in)
            if event._wake not in wakes:
                wakes.append(event._wake)
        for w in wakes:
            pending = [t for t in list(w._background) if not t.done()]
            await asyncio.gather(*pending, return_exceptions=True)

    def __repr__(self) -> str:
        return f"TestToolCall(tool={self.tool!r}, user={self.user!r}, native={self.native!r})"

    async def subscribe(
        self,
        event: WakeEvent[Any, Any],
        args: Any,
        *,
        once: bool = False,
        replay: timedelta | int | float | str | None = None,
        ttl: timedelta | int | float | str | None = None,
        reason: str | None = None,
        label: str | None = None,
        context: Any = None,
    ) -> Subscription:
        if self._wake is not None:
            self._check_event(event)
        self._check_declared(event)
        a_obj, a_json = event._parse_args(args)
        replay_s = _seconds("replay", replay, most=MAX_REPLAY)
        ttl_s = _seconds("ttl", ttl)
        if event.personal and not self.user and not self.native:
            raise usage("bad_arguments", f"{event.name} is personal: subscribe needs a user, who the subscription belongs to")
        if "subscribe" in self._fail:
            raise _failure(self._wake, self._fail.pop("subscribe"))
        k = _key(event.name, a_json)
        state = "native" if self.native else "renewed" if k in self._seen else "created"
        self._seen.add(k)
        rec = TestSubscription(event.name, a_json, self.tool, self.user, "tool", state, once, replay_s, ttl_s, reason, context)
        self.subscriptions.append(rec)
        if isinstance(self._wake, TestClient):
            self._wake.subscriptions.append(rec)
        if self.native:
            if isinstance(self._wake, TestClient) and event.access == "tools":
                options: dict[str, Any] = {"once": True} if once else {}
                if ttl_s is not None:
                    options["ttl"] = ttl_s
                if replay_s is not None:
                    options["replay"] = replay_s
                token = self._wake._mint_voucher(event.name, a_json, self.tool, self.user, options)
                if context is not None:
                    self._wake._remember_voucher(token, context)
                self.vouchers.append(TestVoucher(event.name, a_json, token))
            return Subscription(state="native", user=self.user)
        if state == "created" or k in self._created:
            self._created[k] = (event, a_obj, context, contextvars.copy_context())
        self.session = SessionKey(self._given_session)
        self._n += 1
        return Subscription(
            state=state,  # type: ignore[arg-type]
            id=f"sub_test_{self._n}",
            user=self.user,
            session=self.session,
            recognized=Recognized(harness=None, via="test", claimed=None),
        )

    async def unsubscribe(self, event: WakeEvent[Any, Any], args: Any) -> bool:
        if self._wake is not None:
            self._check_event(event)
        _, a_json = event._parse_args(args)
        if "unsubscribe" in self._fail:
            raise _failure(self._wake, self._fail.pop("unsubscribe"))
        k = _key(event.name, a_json)
        ended = k in self._seen
        self._seen.discard(k)
        self._created.pop(k, None)
        self.unsubscribes.append(TestUnsubscribe(event.name, a_json, ended))
        return ended

    async def resolve_session(self) -> SessionKey | None:
        self.session = SessionKey(self._given_session)
        return self.session


def tool_call(
    *,
    tool: str = "test_tool",
    user: str | None = None,
    session: str | None = None,
    native: bool = False,
    context: Any = None,
    events: Sequence[WakeEvent[Any, Any]] | None = None,
    wake: Wakeshift | None = None,
) -> TestToolCall:
    """A stand-in ToolCall (``TestToolCall``) to pass a tool's function in a
    unit test: ``user`` as identify would give it, ``session`` as the
    session_key the agent passed, ``native`` for a client that subscribes
    itself, ``context`` as the tool's Context, ``events`` to check
    subscribes against the tool's declared events, ``wake`` to check events
    belong to that client (a ``TestClient`` records the subscribes too)."""
    return TestToolCall(tool=tool, user=user, session=session, native=native, context=context, events=events, wake=wake)


tool_call.__test__ = False  # type: ignore[attr-defined]  # pytest: not a test, whatever imports it


class with_call:  # noqa: N801 - used as a function
    """Inside the block (and the tasks started in it), ``wake.current_call()``
    returns ``call``: for unit-testing a tool named in attach's ``wrap``.
    ``async with`` also runs the call's ``finish()`` (each event's
    on_subscribe, as after a real call that succeeded, and so the watches
    it starts) when the block raises nothing::

        async with with_call(wake, tool_call(tool="start_crawl")) as call:
            await start_crawl_handler(...)
        assert call.subscriptions

    A plain ``with`` can't wait for on_subscribe, so it doesn't run it: when
    the call made subscriptions to an event with on_subscribe, it warns
    (RuntimeWarning) at the end of the block. Use ``async with``, or
    ``await call.finish()`` after the block."""

    def __init__(self, wake: Wakeshift, call: ToolCall) -> None:
        self._wake = wake
        self._call = call
        self._tokens: list[contextvars.Token[Any]] = []

    def __enter__(self) -> ToolCall:
        self._tokens.append(_TEST_CALL.set((self._wake, self._call)))
        return self._call

    def __exit__(self, kind: type[BaseException] | None, err: BaseException | None, tb: TracebackType | None) -> None:
        _TEST_CALL.reset(self._tokens.pop())
        call = self._call
        if (
            kind is None
            and isinstance(call, TestToolCall)
            and any(ev._on_subscribe is not None for ev, *_ in call._created.values())
        ):
            warnings.warn(
                f"with_call's plain `with` doesn't run on_subscribe (or the watches it starts) for {call.tool}'s "
                "subscriptions: use `async with with_call(...)`, or `await call.finish()` after the block",
                RuntimeWarning,
                stacklevel=2,
            )

    async def __aenter__(self) -> ToolCall:
        return self.__enter__()

    async def __aexit__(self, kind: type[BaseException] | None, err: BaseException | None, tb: TracebackType | None) -> None:
        _TEST_CALL.reset(self._tokens.pop())
        if kind is None and isinstance(self._call, TestToolCall):
            await self._call.finish()
