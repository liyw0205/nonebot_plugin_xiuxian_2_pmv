from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.migrations import apply_backpack_repair
from nonebot_plugin_xiuxian_2.features.back.repair_application import BackpackRepairApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class BackpackRepairApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "repair.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,"
                "goods_name TEXT,goods_num INTEGER,bind_num INTEGER DEFAULT 0,"
                "state INTEGER DEFAULT 0,update_time TEXT,UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES ('u1','甲')")
            conn.execute("INSERT INTO back VALUES ('u1',100,'旧名称',200,250,0,NULL)")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_default_application_requires_startup_migration(self) -> None:
        application = BackpackRepairApplication(self.database)
        with self.assertRaises(sqlite3.OperationalError):
            application.run("repair-missing", catalog={"100": "灵石袋"}, max_goods_num=100)
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertIsNone(
                uow.query_one(
                    "SELECT name FROM sqlite_master WHERE name='backpack_repair_tasks'"
                )
            )

    def test_migrated_application_repairs_and_replays(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            apply_backpack_repair(uow)
        application = BackpackRepairApplication(self.database)

        first = application.run(
            "repair-1", catalog={"100": "灵石袋"}, max_goods_num=100
        )
        duplicate = application.run("repair-1", batch_size=100)

        self.assertEqual((first.status, first.done), ("applied", True))
        self.assertEqual(duplicate.status, "duplicate")
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT goods_num,bind_num,goods_name FROM back "
                "WHERE user_id=%s AND goods_id=%s",
                ("u1", 100),
            ).fetchone()
        self.assertEqual(tuple(row), (100, 100, "灵石袋"))

    def test_migration_is_routed_only_to_game_database(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("back.014", routed["game_db"])
        for key in routed:
            if key != "game_db":
                self.assertNotIn("back.014", routed[key])


if __name__ == "__main__":
    unittest.main()
