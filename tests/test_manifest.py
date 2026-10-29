"""The manifest the three SDKs send, and when (the shared golden file
manifest.json): automatically, with the settings Wakeshift has or the app last
published, its listed events then the defined ones, refusing a name in both."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

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

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal answer
        path = request.url.path.removeprefix("/api/apps/v1")
        if path != "/manifest":
            return httpx.Response(200, json={"matched": 0})
        if request.method == "GET":
            got.append({"method": "GET"})
            return httpx.Response(200, json=case.get("stored", {"description": ""}))
        body = json.loads(request.content)
        got.append({"method": request.method, "body": body})
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
            if step.get("emit"):
                await events[emit["event"]].emit(emit["arguments"], emit["payload"])
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
            want.append({"method": r["method"]})
            continue
        body = dict(r["body"])
        if "events" in body:
            body["events"] = [events[e].manifest if isinstance(e, str) else e for e in body["events"]]
        want.append({"method": r["method"], "body": body})
    assert sorted_json(got) == sorted_json(want)
