"""The manifest the three SDKs send, and when (the shared golden file
manifest.json): an explicit publish replaces it; the automatic send, only from
a process that serves the app's tools (attach), merges the settings the app
published and its events, its listed events then the defined ones, refusing a
name in both."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from mcp.server.mcpserver import MCPServer

from wakeshift import WakeEvent, Wakeshift

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

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal answer
        path = request.url.path.removeprefix("/api/apps/v1")
        if path != "/manifest" and not path.startswith("/manifest/"):
            return httpx.Response(200, json={"matched": 0, "subscriptions": 0, "connected": 0, "forgotten": False})
        if request.method == "GET":
            got.append({"method": "GET", "path": path})
            return httpx.Response(200, json={"description": ""})
        body = json.loads(request.content)
        got.append({"method": request.method, "path": path, "body": body})
        if answer is not None:
            reply, answer = answer, None
            return httpx.Response(reply["status"], json=reply["body"])
        return httpx.Response(200, json=body)

    wake = Wakeshift(app_key(), core="http://core.test", http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
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
