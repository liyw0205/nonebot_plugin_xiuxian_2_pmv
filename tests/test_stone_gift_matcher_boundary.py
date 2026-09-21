from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from nonebot_plugin_xiuxian_2.adapters.nonebot.commands import _build_stone
from nonebot_plugin_xiuxian_2.adapters.nonebot.context import CommandContext
from nonebot_plugin_xiuxian_2.features.stone_gift.application import StoneGiftApplication
from nonebot_plugin_xiuxian_2.features.stone_gift.migrations import apply_stone_gift, apply_stone_gift_limits
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class _Segment:
    type = "text"

    def __init__(self, text: str) -> None:
        self.data = {"text": text}


class _Message(list):
    def extract_plain_text(self) -> str:
        return "".join(item.data["text"] for item in self)


class _FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 13, tzinfo=timezone.utc)


class StoneGiftMatcherBoundaryTests(unittest.TestCase):
    def test_build_stone_passes_daily_limits_to_new_application(self) -> None:
        application = Mock()
        application.resolve_user.return_value = {
            "user_id": "200",
            "user_name": "乙",
            "level": "江湖好手",
        }
        application.reply.return_value = Mock()
        application.read_limits.return_value = {
            "transfer_date": "2026-09-13",
            "send_limit": 100_000_000,
            "receive_limit": 100_000_000,
            "send_used": 0,
            "receive_used": 0,
        }
        context = CommandContext(
            platform="onebot",
            scene="private",
            group_id=None,
            user_id="100",
            message_id="event-1",
            raw_event=object(),
        )

        _build_stone(context, application, _Message([_Segment("乙 1000")]))

        application.reply.assert_called_once()
        kwargs = application.reply.call_args.kwargs
        self.assertEqual(kwargs["sender_id"], "100")
        self.assertEqual(kwargs["recipient_id"], "200")
        self.assertEqual(kwargs["gross_amount"], 1000)
        self.assertIn("send_limit", kwargs)
        self.assertIn("receive_limit", kwargs)
        self.assertIn("send_used", kwargs)
        self.assertIn("receive_used", kwargs)

    def test_invalid_amount_never_calls_application(self) -> None:
        application = Mock()
        context = CommandContext(
            platform="onebot",
            scene="private",
            group_id=None,
            user_id="100",
            message_id="event-2",
            raw_event=object(),
        )

        plan = _build_stone(context, application, _Message([_Segment("乙 nope")]))

        self.assertIn("正确", plan.content)
        application.resolve_user.assert_not_called()
        application.reply.assert_not_called()

    def test_real_application_resolves_level_for_daily_limit_calculation(self) -> None:
        with TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "stone-gift.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT, level TEXT, stone INTEGER)"
                )
                uow.execute(
                    "INSERT INTO user_xiuxian VALUES (?, ?, ?, ?)",
                    ("100", "甲", "感气境初期", 100_000_001),
                )
                uow.execute(
                    "INSERT INTO user_xiuxian VALUES (?, ?, ?, ?)",
                    ("200", "乙", "江湖好手", 100),
                )
                apply_platform_schema(uow)
                apply_stone_gift(uow)
                apply_stone_gift_limits(uow)

            application = StoneGiftApplication(database, clock=_FixedClock())
            context = CommandContext(
                platform="onebot",
                scene="private",
                group_id=None,
                user_id="100",
                message_id="event-real-1",
                raw_event=object(),
            )

            plan = _build_stone(context, application, _Message([_Segment("乙 100000001")]))

            self.assertIn("共赠送100000001枚灵石", plan.content)


if __name__ == "__main__":
    unittest.main()
