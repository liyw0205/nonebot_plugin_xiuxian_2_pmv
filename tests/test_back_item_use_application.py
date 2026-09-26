from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.item_use_application import ItemUseApplication
from nonebot_plugin_xiuxian_2.features.back.item_use_repository import ItemUseSqlRepository
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_item_use
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class BackItemUseApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER DEFAULT 0,state INTEGER DEFAULT 0,UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES('u',20012,5,4,0)")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_item_use(uow)
        self.application = ItemUseApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_consumes_batch_and_clamps_bind_count(self) -> None:
        result = self.application.apply("item-use-1", "u", 20012, 2, expected_item_count=5)
        self.assertEqual((result.status, result.item_remaining), ("applied", 3))
        with db_backend.connection(self.database) as conn:
            self.assertEqual((3, 3), tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone()))

    def test_replay_and_payload_conflict_are_idempotent(self) -> None:
        first = self.application.apply("same", "u", 20012, 2, expected_item_count=5)
        duplicate = self.application.apply("same", "u", 20012, 2, expected_item_count=5)
        conflict = self.application.apply("same", "u", 20012, 1, expected_item_count=3)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "operation_conflict"))

    def test_state_and_inventory_rejections_do_not_consume(self) -> None:
        self.assertEqual(
            self.application.apply("stale", "u", 20012, 2, expected_item_count=4).status,
            "state_changed",
        )
        self.assertEqual(self.application.apply("short", "u", 20012, 9).status, "item_insufficient")
        with db_backend.connection(self.database) as conn:
            self.assertEqual((5, 4), tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone()))

    def test_operation_insert_failure_rolls_back_consumption(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_item_use BEFORE INSERT ON back_item_use_operations "
                "BEGIN SELECT RAISE(ABORT,'reject operation'); END"
            )
        with self.assertRaises(sqlite3.DatabaseError):
            self.application.apply("rollback", "u", 20012, 2, expected_item_count=5)
        with db_backend.connection(self.database) as conn:
            self.assertEqual((5, 4), tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone()))

    def test_migration_is_game_database_only(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("back.016", routed["game_db"])
        self.assertIn("back.017", routed["game_db"])
        for key in routed:
            if key != "game_db":
                self.assertNotIn("back.016", routed[key])
                self.assertNotIn("back.017", routed[key])


if __name__ == "__main__":
    unittest.main()
