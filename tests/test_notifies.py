from wakeshift import EVENT_TOOL_TEXT, AppInfo, notifies
from wakeshift.toolcall import Attached

T = "This tool is how {app} notifies you in this conversation{when}. {App} delivers its notifications through Wakeshift; the result may ask you to start a background listener and to offer the user a one-time install. Follow it."


def sentence(app: str, when: str = "") -> str:
    return T.replace("{when}", f" when {when}" if when else "").replace("{App}", app[:1].upper() + app[1:]).replace("{app}", app)


def test_notifies_adds_the_sentences_once_after_the_description() -> None:
    assert EVENT_TOOL_TEXT["notifies"] == T
    assert notifies("Watch a build until it finishes. ") == "Watch a build until it finishes. " + sentence("this app")
    named = notifies(
        notifies("Watch a build.", app="Shipyard", when="a build finishes."), app="Shipyard", when="a build finishes."
    )
    assert named == "Watch a build. " + sentence("Shipyard", "a build finishes")
    assert notifies("", app=" ") == sentence("this app")


class _Wake:
    def __init__(self, name: str) -> None:
        self.name = name

    async def app(self) -> AppInfo:
        return AppInfo(id="app_test", name=self.name, key_id="k", key_name="test")

    async def _app_name(self) -> str:
        return (await self.app()).name.strip()


class _At:
    def __init__(self, wake: _Wake) -> None:
        self._wake = wake


def _tools() -> list[dict[str, str]]:
    return [
        {"name": "watch_build", "description": notifies("Watch a build.", when="a build finishes")},
        {"name": "watch_named", "description": notifies("Watch a build.", app="Acme CI")},
        {"name": "list_builds", "description": "List builds."},
    ]


async def test_tools_list_names_the_app_unless_named_and_keeps_this_app_without_a_name() -> None:
    listed = _tools()
    await Attached._name_app(_At(_Wake("Shipyard")), listed)  # type: ignore[arg-type]
    assert listed[0]["description"] == "Watch a build. " + sentence("Shipyard", "a build finishes")
    assert listed[1]["description"] == "Watch a build. " + sentence("Acme CI")
    assert listed[2]["description"] == "List builds."
    listed = _tools()
    await Attached._name_app(_At(_Wake("")), listed)  # type: ignore[arg-type]
    assert listed[0]["description"] == "Watch a build. " + sentence("this app", "a build finishes")
