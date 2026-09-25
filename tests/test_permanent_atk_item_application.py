from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.migrations import apply_permanent_atk_item
from nonebot_plugin_xiuxian_2.features.back.permanent_atk_item_application import (
    PermanentAtkItemApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class PermanentAtkItemApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "permanent-atk-item.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY, atk_buff INTEGER)")
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "bind_num INTEGER, day_num INTEGER, all_num INTEGER, "
                "UNIQUE(user_id, goods_id))"
            )
            conn.execute("INSERT INTO BuffInfo VALUES (%s, %s)", ("user", 20))
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 15002, 3, 2, 0, 1),
            )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_permanent_atk_item(uow)
        self.application = PermanentAtkItemApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            atk = conn.execute(
                "SELECT atk_buff FROM BuffInfo WHERE user_id=%s", ("user",)
            ).fetchone()[0]
            item = conn.execute(
                "SELECT goods_num,bind_num,day_num,all_num FROM back "
                "WHERE user_id=%s AND goods_id=%s",
                ("user", 15002),
            ).fetchone()
            operation_count = conn.execute(
                "SELECT COUNT(*) FROM permanent_atk_item_operations"
            ).fetchone()[0]
        return int(atk), tuple(map(int, item)), int(operation_count)

    def test_use_updates_attack_and_inventory_atomically(self) -> None:
        result = self.application.apply("atk-1", "user", 15002, 2, 12)

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.state(), (32, (1, 0, 2, 3), 1))

    def test_duplicate_replays_recorded_result_without_mutation(self) -> None:
        first = self.application.apply("atk-repeat", "user", 15002, 1, 6)
        second = self.application.apply("atk-repeat", "user", 15002, 3, 99)

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual((second.quantity, second.atk_gain), (1, 6))
        self.assertEqual(self.state(), (26, (2, 1, 1, 2), 1))

    def test_insufficient_item_and_missing_buff_do_not_mutate_state(self) -> None:
        insufficient = self.application.apply("atk-poor", "user", 15002, 4, 24)
        missing = self.application.apply("atk-missing", "absent", 15002, 1, 6)

        self.assertEqual((insufficient.status, missing.status), ("item_insufficient", "buff_missing"))
        self.assertEqual(self.state(), (20, (3, 2, 0, 1), 0))

    def test_trigger_failure_rolls_back_attack_and_inventory(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_atk_item BEFORE INSERT ON permanent_atk_item_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.application.apply("atk-fail", "user", 15002, 1, 6)

        self.assertEqual(self.state(), (20, (3, 2, 0, 1), 0))

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY, atk_buff INTEGER)")
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER)")

            with self.assertRaises(sqlite3.OperationalError):
                PermanentAtkItemApplication(database).apply(
                    "missing-schema", "user", 15002, 1, 6
                )

            with DatabaseUnitOfWork(database) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name='permanent_atk_item_operations'"
                    )
                )

    def test_migration_is_routed_only_to_game_database(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }

        self.assertIn("back.011", routed["game_db"])
        for key in routed:
            if key != "game_db":
                self.assertNotIn("back.011", routed[key])


if __name__ == "__main__":
    unittest.main()
