from __future__ import annotations

import unittest
from dataclasses import dataclass
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.qq_compat import (
    InteractionAcknowledger,
    from_nonebot_event,
    get_interaction_context,
    get_lifecycle_context,
)


@dataclass
class FakeMessage:
    text: str

    def extract_plain_text(self) -> str:
        return self.text


class FakeQQEvent:
    __module__ = "nonebot.adapters.qq.event"

    def __init__(self, **values):
        self.__dict__.update(values)

    def get_event_name(self):
        return self.event_type

    def get_message(self):
        return FakeMessage(self.content)


class QQContextTests(unittest.IsolatedAsyncioTestCase):
    def test_group_event_is_normalized(self) -> None:
        event = FakeQQEvent(
            event_type="GROUP_AT_MESSAGE_CREATE",
            id="message-1",
            event_id="event-1",
            content="  /状态",
            group_openid="group-1",
            author=SimpleNamespace(
                id="legacy-user",
                member_openid="member-1",
                union_openid="union-1",
            ),
            message_scene=SimpleNamespace(ext=["foo=1&msg_idx=REFIDX%3Aabc"]),
            mentions=[
                SimpleNamespace(scope="single", is_you=True, bot=True, id="bot"),
                SimpleNamespace(scope="single", is_you=False, bot=False, id="user-2"),
                SimpleNamespace(scope="all", is_you=True, bot=False, id=""),
            ],
            attachments=[
                SimpleNamespace(
                    content_type="image/png",
                    url="https://example.invalid/a.png",
                    filename="a.png",
                    size=42,
                    width=10,
                    height=20,
                )
            ],
        )

        context = from_nonebot_event(event)

        self.assertEqual(context.scene, "group")
        self.assertEqual(context.user_id, "member-1")
        self.assertEqual(context.raw_user_id, "legacy-user")
        self.assertEqual(context.reference_id, "REFIDX:abc")
        self.assertTrue(context.mentions.at_self)
        self.assertTrue(context.mentions.at_all)
        self.assertEqual(context.mentions.other_user_ids, ("user-2",))
        self.assertEqual(context.attachments[0].filename, "a.png")

    def test_interaction_and_lifecycle_are_normalized(self) -> None:
        resolved = SimpleNamespace(
            user_id="user-1",
            message_id="message-1",
            button_id="button-1",
            button_data="/签到",
            feature_id="feature-1",
        )
        interaction = FakeQQEvent(
            event_type="INTERACTION_CREATE",
            id="interaction-1",
            event_id="event-1",
            content="",
            data=SimpleNamespace(resolved=resolved),
            group_openid="group-1",
            group_member_openid="member-1",
        )
        context = get_interaction_context(interaction)
        self.assertEqual(context.interaction_id, "interaction-1")
        self.assertEqual(context.button_data, "/签到")
        self.assertEqual(context.user_id, "member-1")

        lifecycle = FakeQQEvent(
            event_type="GROUP_MEMBER_REMOVE",
            content="",
            group_openid="group-1",
            member_openid="member-1",
        )
        life_context = get_lifecycle_context(lifecycle)
        self.assertEqual(life_context.action, "member_leave_group")
        self.assertEqual(from_nonebot_event(lifecycle).scene, "lifecycle")

    async def test_interaction_ack_is_idempotent(self) -> None:
        event = FakeQQEvent(
            event_type="INTERACTION_CREATE",
            id="interaction-1",
            content="",
            data=SimpleNamespace(resolved=SimpleNamespace()),
        )

        class Bot:
            def __init__(self):
                self.calls = []

            async def put_interaction(self, **kwargs):
                self.calls.append(kwargs)

        bot = Bot()
        acknowledger = InteractionAcknowledger()
        self.assertTrue(await acknowledger.ack(bot, event, 0))
        self.assertFalse(await acknowledger.ack(bot, event, 1))
        self.assertEqual(bot.calls, [{"interaction_id": "interaction-1", "code": 0}])

    async def test_failed_ack_can_be_retried(self) -> None:
        event = FakeQQEvent(
            event_type="INTERACTION_CREATE",
            id="interaction-retry",
            content="",
            data=SimpleNamespace(resolved=SimpleNamespace()),
        )

        class Bot:
            def __init__(self):
                self.calls = 0

            async def put_interaction(self, **_):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("temporary")

        bot = Bot()
        acknowledger = InteractionAcknowledger()
        with self.assertRaises(RuntimeError):
            await acknowledger.ack(bot, event)
        self.assertTrue(await acknowledger.ack(bot, event))
        self.assertEqual(bot.calls, 2)


if __name__ == "__main__":
    unittest.main()
