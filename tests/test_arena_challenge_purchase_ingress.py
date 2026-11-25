from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
from nonebot.exception import FinishedException

nonebot.init()

from nonebot_plugin_xiuxian_2.features.arena.application import ArenaApplication
from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_challenge_purchase, apply_arena_state
from nonebot_plugin_xiuxian_2.features.arena.repository import ArenaChallengePurchaseSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from tests.test_db_backend import db_backend


class ArenaChallengePurchaseIngressTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        self.today = date.today().isoformat()
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',5000000)")
            apply_arena_challenge_purchase(uow)
            OperationLedger().ensure_schema(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_arena_state(uow)
            uow.execute(
                "INSERT INTO arena(user_id,daily_challenge_buys,daily_extra_challenges,last_buy_date) "
                "VALUES('u',1,1,?)", (self.today,)
            )

    def tearDown(self):
        self.temp.cleanup()

    def test_registered_command_preserves_reason_and_recovers_after_receipt_rejection(self):
        arena_module = __import__("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena", fromlist=["arena_buy_challenge"])
        registered = next(handler for handler in arena_module.arena_buy_challenge.handlers if handler.call is arena_module.arena_buy_challenge_)
        clock = SimpleNamespace(
            now=lambda: datetime.combine(
                date.fromisoformat(self.today), datetime.min.time(), tzinfo=timezone.utc
            )
        )
        repository = ArenaChallengePurchaseSqlRepository(
            self.game, self.player, clock=clock
        )
        application = ArenaApplication(
            self.game, self.player, repository=repository, clock=clock
        )
        bot = SimpleNamespace(self_id="bot")
        event = SimpleNamespace(message_id="purchase-message", get_user_id=lambda: "u")
        replies = []

        async def capture(*args, **kwargs):
            replies.append((args, kwargs))

        async def invoke():
            with db_backend.transaction(self.game) as conn:
                conn.execute("CREATE TRIGGER refuse_receipt BEFORE INSERT ON arena_challenge_purchase_operations BEGIN SELECT RAISE(IGNORE); END")
            with (
                patch.object(arena_module, "assign_bot", new=AsyncMock(return_value=(bot, None))),
                patch.object(arena_module, "check_user", return_value=(True, {"user_id": "u", "stone": 5000000}, "")),
                patch.object(arena_module, "arena_application", application),
                patch.object(arena_module, "arena_limit", SimpleNamespace(
                    daily_buy_limit=3,
                    daily_challenges=10,
                    get_user_arena_info=lambda user_id: {
                        "daily_challenge_buys": 1,
                        "daily_extra_challenges": 1,
                        "last_buy_date": self.today,
                        "daily_challenges_used": 0,
                    },
                )),
                patch.object(arena_module, "handle_send", new=capture),
            ):
                with self.assertRaises(FinishedException):
                    await registered.call(bot, event, args=SimpleNamespace(extract_plain_text=lambda: "1"))
                self.assertIn("竞技场购买次数回执保存失败", replies[-1][0][2])
                with db_backend.transaction(self.game) as conn:
                    conn.execute("DROP TRIGGER refuse_receipt")
                with self.assertRaises(FinishedException):
                    await registered.call(bot, event, args=SimpleNamespace(extract_plain_text=lambda: "1"))

        asyncio.run(invoke())
        self.assertEqual(len(replies), 2)
        self.assertIn("竞技场挑战次数购买成功", replies[-1][0][2])
        self.assertEqual("竞技场", replies[-1][1]["md_type"])
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(3000000, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM arena_challenge_purchase_operations").fetchone()[0])
        with sqlite3.connect(self.player) as conn:
            self.assertEqual((2, 2), conn.execute("SELECT daily_challenge_buys,daily_extra_challenges FROM arena").fetchone())

    def test_reason_reply_keeps_onebot_qq_markdown_and_send_failure_contracts(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        async def run_contracts():
            for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
                bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
                config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
                plain = AsyncMock()
                markdown_send = AsyncMock(side_effect=RuntimeError("isolated purchase delivery failure") if send_fails else None)
                with (
                    patch.object(utils, "XiuConfig", return_value=config),
                    patch.object(utils, "get_chat_scene", return_value="group"),
                    patch.object(utils, "is_group_event", return_value=True),
                    patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")),
                    patch.object(utils, "_send_event_message", plain),
                    patch.object(utils, "_send_markdown_or_keyboard", markdown_send),
                ):
                    await utils.handle_send(bot, SimpleNamespace(get_user_id=lambda: "u"), "竞技场购买次数回执保存失败", md_type="竞技场", k1="竞技场", v1="竞技场查看")
                if onebot or not markdown or send_fails:
                    plain.assert_awaited_once()
                    self.assertEqual(plain.call_args.kwargs["message"], "竞技场购买次数回执保存失败")
                else:
                    markdown_send.assert_awaited_once()

        asyncio.run(run_contracts())


if __name__ == "__main__":
    unittest.main()
