from __future__ import annotations

import asyncio
import importlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import nonebot
from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.features.rift.application import RiftApplication
from nonebot_plugin_xiuxian_2.features.rift.migrations import (
    apply_rift_demon_token_operations,
    apply_rift_demon_token_player_schema,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class RiftDemonTokenUseApplicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="rift-demon-token-matcher-")
        root = Path(self.temp.name)
        self.game_db, self.player_db = root / "game.db", root / "player.db"
        self.rift = {"name": "boss", "rank": 4}
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',1000,500,100,80)")
            conn.execute("INSERT INTO user_cd VALUES('u',3,'now','30')")
            conn.execute("INSERT INTO rift_entries VALUES('u',%s,'active')", (json.dumps(self.rift),))
            conn.execute("INSERT INTO back VALUES('u',20018,'token','item',1,'','',1)")
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('CREATE TABLE rift(user_id TEXT PRIMARY KEY,"explore_count" INTEGER)')
            conn.execute("INSERT INTO rift VALUES('u',3)")
        with DatabaseUnitOfWork(self.game_db) as uow:
            apply_rift_demon_token_operations(uow)
        with DatabaseUnitOfWork(self.player_db) as uow:
            apply_rift_demon_token_player_schema(uow)
        self.application = RiftApplication(self.game_db, self.player_db)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_migrations_are_routed_to_their_owning_databases(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("rift.002", routed["game_db"])
        self.assertIn("rift.003", routed["player_db"])
        self.assertNotIn("rift.002", routed["player_db"])
        self.assertNotIn("rift.003", routed["game_db"])

    def test_registered_item_matcher_routes_20018_to_feature_application(self) -> None:
        back_module = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back")
        rift_module = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift")
        registered_handler = next(
            handler for handler in back_module.use_item.handlers if handler.call is back_module.use_item_
        )
        fake_bot = SimpleNamespace(self_id="bot")
        fake_event = SimpleNamespace(message_id="demon-token-message")
        outcome = {
            "delta": {"stone": 50, "exp": 10, "hp": -5, "mp": -2},
            "statistics": {"秘境打怪": 1},
            "message": "fixed battle",
        }

        class MessageData:
            @staticmethod
            def goods_num(user_id, item_id):
                with db_backend.connection(self.game_db) as conn:
                    row = conn.execute(
                        "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                return int(row[0]) if row else 0

        message_data = MessageData()
        send_feedback = AsyncMock()
        with (
            patch.object(back_module, "assign_bot", new=AsyncMock(return_value=(fake_bot, None))),
            patch.object(rift_module, "assign_bot", new=AsyncMock(return_value=(fake_bot, None))),
            patch.object(back_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
            patch.object(rift_module, "check_user", return_value=(True, {"user_id": "u", **{"stone": 1000, "exp": 500, "hp": 100, "mp": 80}}, "")),
            patch.object(rift_module, "check_user_type", return_value=(True, "")),
            patch.object(back_module, "items") as items,
            patch.object(back_module, "_sql_message", return_value=message_data),
            patch.object(rift_module, "read_rift_data", return_value=self.rift),
            patch.object(rift_module, "_rift_progress_snapshot", return_value=3),
            patch.object(rift_module, "_roll_rift_progress", return_value=(None, "progress")),
            patch.object(rift_module, "_roll_rift_boss_event", new=AsyncMock(return_value=("battle", "won", outcome))),
            patch.object(rift_module, "rift_application", self.application),
            patch.object(self.application, "replay_demon_token_battle", wraps=self.application.replay_demon_token_battle) as replay,
            patch.object(self.application, "settle_demon_token_battle", wraps=self.application.settle_demon_token_battle) as settle,
            patch.object(rift_module, "XiuConfig", return_value=SimpleNamespace(max_goods_num=1000)),
            patch.object(rift_module, "send_msg_handler", new=AsyncMock()),
            patch.object(rift_module, "handle_send", new=send_feedback),
            patch.object(rift_module, "log_message"),
            patch.object(back_module, "handle_send", new=send_feedback),
        ):
            items.get_data_by_item_name.return_value = (20018, {"type": "特殊道具", "name": "斩妖令"})
            with self.assertRaises(FinishedException):
                asyncio.run(registered_handler.call(fake_bot, fake_event, args="斩妖令"))

        self.assertEqual((replay.call_count, settle.call_count), (1, 1))
        self.assertEqual(settle.call_args.kwargs["item_id"], 20018)
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20018").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT status FROM rift_entries WHERE user_id='u'").fetchone()[0], "settled")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM rift_demon_token_battle_operations").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
