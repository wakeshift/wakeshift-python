"""The manifest the three SDKs send, and when (the shared golden file
manifest.json): an explicit publish replaces it; the automatic send (from a
process that serves the app's tools, attach, or after an emit's
unknown_event) merges the settings the app published and its events, its
listed events then the defined ones, refusing a name in both."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx2
import pytest
from mcp.server.mcpserver import MCPServer

from wakeshift import APIError, WakeEvent, Wakeshift

from .helpers import app_key, golden_file, sorted_json
from .test_golden import ChatArgs, ChatMessage, Run, RunArgs, Turn, TurnArgs

FIXTURE = golden_file("manifest.json")


def define_sample(wake: Wakeshift, name: str) -> WakeEvent:
    """One of events-list.json's sample event types (test_golden.py defines the same ones)."""
    if name == "chat.message":
        return wake.define_event(
            "chat.message",
            title="Chat message",
            description="A new message in a channel.",
            args=ChatArgs,
            payload=ChatMessage,
            text=lambda m, a: f"{m.sender}: {m.text}",
        )
    if name == "game.turn":
        return wake.define_event(
            "game.turn",
            description="It's your move at a table.",
            args=TurnArgs,
            payload=Turn,
            text=lambda t, a: f"Your move at {a.table}",
            personal=True,
        )
    if name == "ci.run":
        return wake.define_event(
            "ci.run",
            description="A CI run on a pull request finished.",
            args=RunArgs,
            payload=Run,
            text=lambda r, a: f"CI {r.status}",
        )
    raise AssertionError(f"no sample event {name}")


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=[c["name"] for c in FIXTURE["cases"]])
async def test_manifest_matches_the_other_sdks(case: dict[str, Any]) -> None:
    got: list[dict[str, Any]] = []
    answer: dict[str, Any] | None = None
    unavailable = {"status": 503, "body": {"error": {"code": "unavailable", "message": "try again"}}}
    unknown = case.get("unknown_emits") or 0

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal answer, unknown
        path = request.url.path.removeprefix("/api/apps/v1")
        if path == "/events/emit" and "unknown_emits" in case:
            got.append({"method": request.method, "path": path})
            if unknown > 0:
                unknown -= 1
                return httpx2.Response(404, json={"error": {"code": "unknown_event", "message": "no such event"}})
            return httpx2.Response(200, json={"matched": 0})
        if path != "/manifest" and not path.startswith("/manifest/"):
            return httpx2.Response(200, json={"matched": 0, "subscriptions": 0, "connected": 0, "forgotten": False})
        if request.method == "GET":
            got.append({"method": "GET", "path": path})
            return httpx2.Response(200, json={"description": ""})
        body = json.loads(request.content)
        got.append({"method": request.method, "path": path, "body": body})
        if answer is not None:
            reply, answer = answer, None
            return httpx2.Response(reply["status"], json=reply["body"])
        return httpx2.Response(200, json=body)

    wake = Wakeshift(app_key(), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)))
    events = {name: define_sample(wake, name) for name in case["define"]}
    emit = FIXTURE["emit"]
    for i, step in enumerate(case["steps"]):
        err: BaseException | None = None
        try:
            if step.get("attach"):
                wake.attach(MCPServer("sample"))
            elif step.get("fail_next"):
                answer = unavailable
            elif step.get("emit"):
                await events[emit["event"]].emit(emit["arguments"], emit["payload"])
            elif step.get("status"):
                await events[emit["event"]].status(emit["arguments"])
            elif step.get("forget_user"):
                await wake.forget_user("ada")
            elif "publish" in step:
                answer = step.get("answer")
                await wake.publish_manifest(step["publish"])
            elif "define" in step:
                events[step["define"]] = define_sample(wake, step["define"])
        except Exception as e:  # the step says whether it fails
            err = e
        if "error" in step:
            assert err is not None and step["error"] in str(err), f"step {i}: {err!r}, want an error saying {step['error']}"
        else:
            assert err is None, f"step {i}: {err!r}"
    want = []
    for r in case["requests"]:
        if "body" not in r:
            want.append({"method": r["method"], "path": r["path"]})
            continue
        body = dict(r["body"])
        if "events" in body:
            body["events"] = [events[e].manifest if isinstance(e, str) else e for e in body["events"]]
        want.append({"method": r["method"], "path": r["path"], "body": body})
    assert sorted_json(got) == sorted_json(want)


async def test_concurrent_unknown_emits_merge_once() -> None:
    """Emits that fail together, before Wakeshift has the event (an app
    emitting at startup, before any agent has connected), send the manifest
    once."""
    known = False
    merges = emits = 0

    async def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal known, merges, emits
        path = request.url.path.removeprefix("/api/apps/v1")
        if path == "/manifest/merge":
            merges += 1
            await asyncio.sleep(0.02)
            known = True
            return httpx2.Response(200, json=json.loads(request.content))
        emits += 1
        if not known:
            return httpx2.Response(404, json={"error": {"code": "unknown_event", "message": "no such event"}})
        return httpx2.Response(200, json={"matched": 0})

    wake = Wakeshift(app_key(), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)))
    chat = define_sample(wake, "chat.message")
    emit = FIXTURE["emit"]
    results = await asyncio.gather(*(chat.emit(emit["arguments"], emit["payload"]) for _ in range(5)))
    assert len(results) == 5
    assert merges == 1
    assert emits == 10


async def test_failed_send_is_shared_by_the_calls_waiting_for_it() -> None:
    """Calls about events that wait for one manifest send share its outcome:
    when it fails, they raise its error rather than each sending again, one
    after another (with Wakeshift slow, each up to a timeout). The next call
    sends again."""
    merges = 0

    async def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal merges
        path = request.url.path.removeprefix("/api/apps/v1")
        if path == "/manifest/merge":
            merges += 1
            await asyncio.sleep(0.05)
            return httpx2.Response(503, json={"error": {"code": "unavailable", "message": "down"}})
        return httpx2.Response(200, json={"ended": True})

    wake = Wakeshift(app_key(), core="https://core.test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)))
    chat = define_sample(wake, "chat.message")
    wake._serving = True  # as after attach
    results = await asyncio.gather(*(chat.end("ada", "s1") for _ in range(5)), return_exceptions=True)
    assert all(isinstance(r, APIError) and r.code == "unavailable" for r in results), results
    assert merges == 1
    with pytest.raises(APIError):
        await chat.end("ada", "s1")
    assert merges == 2
