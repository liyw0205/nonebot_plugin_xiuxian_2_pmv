from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
from nonebot.exception import FinishedException

nonebot.init()

from nonebot_plugin_xiuxian_2.features.arena.application import ArenaApplication
from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_challenge_ticket, apply_arena_state
from nonebot_plugin_xiuxian_2.features.arena.repository import ArenaChallengePurchaseSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from tests.test_db_backend import db_backend


class ArenaChallengeTicketIngressTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            uow.execute("INSERT INTO back VALUES('u',20024,'竞技场挑战券','特殊道具',3,'','',3)")
            apply_arena_challenge_ticket(uow)
            OperationLedger().ensure_schema(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_arena_state(uow)
            uow.execute("INSERT INTO arena(user_id,daily_challenges_used,daily_extra_challenges) VALUES('u',2,1)")

    def tearDown(self):
        self.temp.cleanup()

    def test_registered_item_command_preserves_reason_and_recovers_after_receipt_rejection(self):
        back_module = __import__("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back", fromlist=["use_item"])
        arena_module = __import__("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena", fromlist=["use_arena_challenge_ticket"])
        registered = next(handler for handler in back_module.use_item.handlers if handler.call is back_module.use_item_)
        application = ArenaApplication(self.game, self.player, repository=ArenaChallengePurchaseSqlRepository(self.game, self.player))
        bot = SimpleNamespace(self_id="bot")
        event = SimpleNamespace(message_id="ticket-message", get_user_id=lambda: "u")
        replies = []

        class Items:
            @staticmethod
            def get_data_by_item_name(name):
                return (20024, {"type": "特殊道具", "name": "竞技场挑战券"})

        class MessageData:
            @staticmethod
            def goods_num(user_id, item_id):
                return 3

        async def capture(*args, **kwargs):
            replies.append((args, kwargs))

        async def invoke():
            with db_backend.transaction(self.game) as conn:
                conn.execute("CREATE TRIGGER refuse_receipt BEFORE INSERT ON arena_challenge_ticket_operations BEGIN SELECT RAISE(IGNORE); END")
            with (
                patch.object(back_module, "assign_bot", new=AsyncMock(return_value=(bot, None))),
                patch.object(back_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
                patch.object(back_module, "items", Items()),
                patch.object(back_module, "_sql_message", return_value=MessageData()),
                patch.object(back_module, "handle_send", new=capture),
                patch.object(arena_module, "assign_bot", new=AsyncMock(return_value=(bot, None))),
                patch.object(arena_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
                patch.object(arena_module, "arena_application", application),
                patch.object(arena_module, "_sql_message", return_value=MessageData()),
                patch.object(arena_module, "arena_limit", SimpleNamespace(
                    daily_challenges=10,
                    get_user_arena_info=lambda user_id: {
                        "daily_challenges_used": 2,
                        "daily_extra_challenges": 1,
                    },
                )),
                patch.object(arena_module, "handle_send", new=capture),
            ):
                with self.assertRaises(FinishedException):
                    await registered.call(bot, event, args="竞技场挑战券")
                self.assertIn("竞技场挑战券使用回执保存失败", replies[-1][0][2])
                with db_backend.transaction(self.game) as conn:
                    conn.execute("DROP TRIGGER refuse_receipt")
                with self.assertRaises(FinishedException):
                    await registered.call(bot, event, args="竞技场挑战券")

        asyncio.run(invoke())
        self.assertEqual(len(replies), 2)
        self.assertIn("使用竞技场挑战券 1 张", replies[-1][0][2])
        self.assertEqual("竞技场", replies[-1][1]["md_type"])
        with sqlite3.connect(self.game) as conn:
            self.assertEqual((2, 2, 1), conn.execute(
                "SELECT goods_num,bind_num,(SELECT COUNT(*) FROM arena_challenge_ticket_operations) FROM back"
            ).fetchone())
        with sqlite3.connect(self.player) as conn:
            self.assertEqual((1, 1), conn.execute("SELECT daily_challenges_used,daily_extra_challenges FROM arena").fetchone())

    def test_rejection_reply_keeps_onebot_qq_markdown_and_send_failure_contracts(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        async def run_contracts():
            for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
                bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
                config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
                plain = AsyncMock()
                markdown_send = AsyncMock(side_effect=RuntimeError("isolated ticket delivery failure") if send_fails else None)
                with (
                    patch.object(utils, "XiuConfig", return_value=config),
                    patch.object(utils, "get_chat_scene", return_value="group"),
                    patch.object(utils, "is_group_event", return_value=True),
                    patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")),
                    patch.object(utils, "_send_event_message", plain),
                    patch.object(utils, "_send_markdown_or_keyboard", markdown_send),
                ):
                    await utils.handle_send(bot, SimpleNamespace(get_user_id=lambda: "u"), "竞技场挑战券使用回执保存失败", md_type="竞技场", k1="竞技场", v1="竞技场查看")
                if onebot or not markdown or send_fails:
                    plain.assert_awaited_once()
                    self.assertEqual(plain.call_args.kwargs["message"], "竞技场挑战券使用回执保存失败")
                else:
                    markdown_send.assert_awaited_once()

        asyncio.run(run_contracts())


if __name__ == "__main__":
    unittest.main()
