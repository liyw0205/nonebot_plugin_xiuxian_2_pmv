from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass
from types import SimpleNamespace

from nonebot_plugin_xiuxian_2.features.group_lifecycle.notice_application import (
    GroupLifecycleNoticeApplication,
)
from nonebot_plugin_xiuxian_2.xiuxian.qq_compat.lifecycle import LifecycleStateRegistry


class FakeQQEvent:
    __module__ = "nonebot.adapters.qq.event"

    def __init__(self, event_type: str, group_id: str, *, event_id: str = "event-1") -> None:
        self.event_type = event_type
        self.group_openid = group_id
        self.event_id = event_id

    def get_event_name(self) -> str:
        return self.event_type


class FakeBot:
    def __init__(self, self_id: str = "bot-1") -> None:
        self.self_id = self_id
        self.sent: list[dict] = []

    async def send_to_group(self, **kwargs) -> None:
        self.sent.append(kwargs)


class FakeRepository:
    def __init__(self, disabled_groups=()) -> None:
        self.data = {"welcome_disabled_groups": list(disabled_groups)}

    def read_data(self) -> dict:
        return dict(self.data)


class FakeAdminConfig:
    def __init__(self, disabled_groups=(), full_groups=()) -> None:
        self.repository = FakeRepository(disabled_groups)
        self.full_groups = set(full_groups)

    def set_full_message_group(self, group_id: str, *, enabled: bool) -> bool:
        if enabled:
            before = len(self.full_groups)
            self.full_groups.add(group_id)
            return len(self.full_groups) != before
        if group_id not in self.full_groups:
            return False
        self.full_groups.remove(group_id)
        return True


@dataclass
class FakeSettings:
    put_bot: tuple[str, ...] = ()
    shield_group: tuple[str, ...] = ("group-1",)
    response_group: bool = True
    group_welcome: bool = True
    group_welcome_msg: str = "道友欢迎入群"
    group_bot_join_msg: str = "修仙机器人已入驻"
    markdown_status: bool = False
    markdown_button_status: bool = False


class GroupLifecycleNoticeApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = LifecycleStateRegistry()
        self.admin_config = FakeAdminConfig()
        self.bot = FakeBot()
        self.settings = FakeSettings()
        self.app = self._application()

    def _application(self, *, segment=None):
        async def assign_bot(*, bot, event):
            return bot, None

        async def send_fallback(*args, **kwargs):
            raise AssertionError("fallback sending should not be used in these tests")

        return GroupLifecycleNoticeApplication(
            self.admin_config,
            settings_provider=lambda: self.settings,
            lifecycle_applier=self.registry.apply,
            assign_bot_fn=assign_bot,
            message_segment=segment,
            send_fallback=send_fallback,
            strip_links=lambda message: message,
        )

    def test_bot_join_sends_passive_message_and_finishes_matcher(self) -> None:
        event = FakeQQEvent("GROUP_ADD_ROBOT", "group-1")

        decision = asyncio.run(self.app.handle(self.bot, event))

        self.assertTrue(decision.finish_matcher)
        self.assertEqual(decision.action, "bot_join_group")
        self.assertEqual(
            self.bot.sent,
            [
                {
                    "group_openid": "group-1",
                    "message": "修仙机器人已入驻",
                    "event_id": "event-1",
                }
            ],
        )
        self.assertEqual(
            self.registry.get_group_state("bot-1", "group-1").event_counts,
            {"bot_join_group": 1},
        )

    def test_policy_denial_and_group_welcome_switch_suppress_delivery(self) -> None:
        event = FakeQQEvent("GROUP_MEMBER_ADD", "group-1")
        self.settings.shield_group = ("other-group",)
        denied = asyncio.run(self.app.handle(self.bot, event))
        self.assertFalse(denied.finish_matcher)
        self.assertEqual(self.bot.sent, [])

        self.settings.shield_group = ("group-1",)
        self.admin_config.repository.data["welcome_disabled_groups"] = ["group-1"]
        disabled = asyncio.run(self.app.handle(self.bot, event))
        self.assertFalse(disabled.finish_matcher)
        self.assertEqual(self.bot.sent, [])

    def test_bot_leave_clears_full_message_mark_without_welcome_policy(self) -> None:
        self.admin_config.full_groups.add("group-1")
        self.settings.shield_group = ()
        self.settings.group_welcome = False
        event = FakeQQEvent("GROUP_DEL_ROBOT", "group-1")

        decision = asyncio.run(self.app.handle(self.bot, event))

        self.assertFalse(decision.finish_matcher)
        self.assertEqual(decision.action, "bot_leave_group")
        self.assertNotIn("group-1", self.admin_config.full_groups)
        self.assertFalse(
            self.registry.get_group_state("bot-1", "group-1").joined
        )
        self.assertEqual(self.bot.sent, [])

    def test_member_welcome_uses_markdown_buttons_when_enabled(self) -> None:
        class SegmentFactory:
            @staticmethod
            def markdown_keyboard(bot, body, buttons):
                return {"kind": "keyboard", "body": body, "buttons": buttons}

        self.settings.markdown_status = True
        self.settings.markdown_button_status = True
        self.app = self._application(segment=SegmentFactory)
        event = FakeQQEvent("GROUP_MEMBER_ADD", "group-1")

        decision = asyncio.run(self.app.handle(self.bot, event))

        self.assertTrue(decision.finish_matcher)
        sent = self.bot.sent[0]["message"]
        self.assertEqual(sent["kind"], "keyboard")
        self.assertEqual(sent["body"], "道友欢迎入群")
        self.assertEqual(sent["buttons"][1], [("关闭欢迎", "关闭进群欢迎")])

    def test_legacy_notice_name_is_classified(self) -> None:
        event = SimpleNamespace(
            type="notice.group_increase",
            group_id="group-1",
            event_id="legacy-event",
            get_event_name=lambda: "notice.group_increase",
        )

        decision = asyncio.run(self.app.handle(self.bot, event))

        self.assertEqual(decision.action, "member_join_group")
        self.assertTrue(decision.finish_matcher)
        self.assertEqual(self.bot.sent[0]["event_id"], "legacy-event")


if __name__ == "__main__":
    unittest.main()
