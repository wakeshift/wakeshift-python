"""App keys, both formats, against the golden file the core and the Go and
Node SDKs share (app-keys.json, written by go test ./internal/appkey), and
what a current key changes in the client: the kid in every assertion, the
default core, and the app's ID from Wakeshift (await wake.app())."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from wakeshift import AppInfo, Wakeshift, parse_key, sign_assertion
from wakeshift.key import b64url_decode, public_key_of

from .helpers import format_key, golden_file

GOLDEN = golden_file("app-keys.json")
SEEDS = {s["name"]: s for s in GOLDEN["seeds"]}
NEW_KEY = next(a["key"] for a in GOLDEN["accepted"] if a["seed"] == "S" and a["key"].strip() == a["key"])
OLD_KEY = "wst_test_ci-bot_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
APP = {"id": "ci-bot", "name": "CI bot", "key": {"key_id": SEEDS["S"]["key_id"], "name": "prod"}}


@pytest.mark.parametrize("v", GOLDEN["accepted"], ids=lambda v: v["key"].strip())
def test_every_accepted_key_parses_to_its_seed_public_key_and_kid(v: dict[str, Any]) -> None:
    seed = SEEDS[v["seed"]]
    k = parse_key(v["key"])
    assert k.seed == b64url_decode(seed["seed"])
    assert public_key_of(k) == seed["public_key"]
    assert k.kid == seed["kid"]
    assert k.kid.startswith(seed["key_id"])
    assert (k.mode, k.app_id) == (v["mode"], v["app_id"])
    assert seed["seed"] not in repr(k), "the seed stays out of reprs"


@pytest.mark.parametrize("v", GOLDEN["refused"], ids=lambda v: v["why"])
def test_every_refused_key_is_refused_without_naming_it(v: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="^wakeshift:") as e:
        parse_key(v["key"])
    if v["key"]:
        assert v["key"] not in str(e.value)


def test_a_current_key_is_the_seed_and_its_crc32_in_base58() -> None:
    for s in GOLDEN["seeds"]:
        k = format_key(b64url_decode(s["seed"]))
        assert len(k) == 54
        assert parse_key(k).seed == b64url_decode(s["seed"])
    assert format_key(b64url_decode(SEEDS["S"]["seed"])) == NEW_KEY


def claims_of(tok: str) -> tuple[dict[str, Any], dict[str, Any]]:
    h, c, _ = tok.split(".")
    return json.loads(b64url_decode(h)), json.loads(b64url_decode(c))


def test_assertions_name_the_key_by_kid_and_the_app_when_known() -> None:
    new = parse_key(NEW_KEY)
    h, c = claims_of(sign_assertion(new, "https://app.wakeshift.ai"))
    assert h == {"alg": "EdDSA", "typ": "app+jwt", "kid": SEEDS["S"]["kid"]}
    assert c["iss"] == c["sub"] == SEEDS["S"]["kid"]
    # Once Wakeshift has said which app it is.
    _, c = claims_of(sign_assertion(new, "https://app.wakeshift.ai", app_id="ci-bot"))
    assert c["iss"] == c["sub"] == "ci-bot"

    old = parse_key(OLD_KEY)
    h, c = claims_of(sign_assertion(old, "https://app.wakeshift.ai"))
    assert h == {"alg": "EdDSA", "typ": "app+jwt", "kid": SEEDS["Z"]["kid"]}
    assert c["iss"] == c["sub"] == "ci-bot"


def test_only_a_test_key_needs_a_core(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WAKESHIFT_CORE", raising=False)
    with pytest.raises(ValueError, match="no core URL for this test key: set WAKESHIFT_CORE"):
        Wakeshift(OLD_KEY)
    assert Wakeshift(NEW_KEY).core == "https://app.wakeshift.ai"
    assert Wakeshift(OLD_KEY.replace("wst_test_", "wst_live_")).core == "https://app.wakeshift.ai"
    monkeypatch.setenv("WAKESHIFT_CORE", "https://app.staging.wakeshift.ai/")
    assert Wakeshift(NEW_KEY).core == "https://app.staging.wakeshift.ai"
    assert Wakeshift(OLD_KEY).core == "https://app.staging.wakeshift.ai"
    assert Wakeshift(NEW_KEY, core="http://127.0.0.1:9").core == "http://127.0.0.1:9"


class Core:
    """Answers /api/apps/v1/app and /oauth/token, recording each request."""

    def __init__(self, app: tuple[int, Any] = (200, APP)) -> None:
        self.app = app
        self.sent: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.sent.append(request)
        if request.url.path == "/api/apps/v1/app":
            return httpx.Response(self.app[0], json=self.app[1])
        if request.url.path == "/oauth/token":
            return httpx.Response(200, json={"access_token": "at", "expires_in": 900, "wakeshift_user": "u-1"})
        return httpx.Response(200, json={"workspace": "w-1"})

    def wake(self, key: str = NEW_KEY) -> Wakeshift:
        return Wakeshift(key, core="http://core.test", http=httpx.AsyncClient(transport=httpx.MockTransport(self)))

    def paths(self) -> list[str]:
        return [r.url.path for r in self.sent]


async def test_a_current_keys_app_id_is_wakeshifts_to_say() -> None:
    core = Core()
    wake = core.wake()
    assert wake.mode is None
    with pytest.raises(RuntimeError, match=r"^wakeshift: call await wake.app\(\) first: this key doesn't name its app"):
        _ = wake.app_id
    # Concurrent first calls ask once.
    infos = await asyncio.gather(wake.app(), wake.app(), wake.app())
    assert infos[0] == AppInfo(id="ci-bot", name="CI bot", key_id=SEEDS["S"]["key_id"], key_name="prod")
    assert all(i is infos[0] for i in infos)
    assert wake.app_id == "ci-bot"
    await wake.app()
    assert core.paths() == ["/api/apps/v1/app"]
    # The first assertion names the kid; once the app is known, the app.
    h, c = claims_of(core.sent[0].headers["authorization"].removeprefix("Bearer "))
    assert h["kid"] == SEEDS["S"]["kid"]
    assert c["iss"] == c["sub"] == SEEDS["S"]["kid"]
    await wake.workspace("ada", "Ada")
    h, c = claims_of(core.sent[-1].headers["authorization"].removeprefix("Bearer "))
    assert h["kid"] == SEEDS["S"]["kid"]
    assert c["iss"] == c["sub"] == "ci-bot"


async def test_a_failed_app_call_isnt_cached() -> None:
    core = Core(app=(401, {"error": {"code": "unauthorized", "message": "invalid assertion"}}))
    wake = core.wake()
    with pytest.raises(Exception, match="invalid assertion"):
        await wake.app()
    core.app = (200, APP)
    assert (await wake.app()).id == "ci-bot"
    assert wake.app_id == "ci-bot"


async def test_an_older_keys_app_id_is_its_own() -> None:
    core = Core(app=(200, {**APP, "name": "CI"}))
    wake = core.wake(OLD_KEY)
    assert (wake.app_id, wake.mode) == ("ci-bot", "test")
    assert core.sent == []
    assert (await wake.app()).name == "CI"
