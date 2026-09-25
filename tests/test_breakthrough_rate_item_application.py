from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.breakthrough_rate_item_application import (
    BreakthroughRateItemApplication,
)
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_breakthrough_rate_item
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class BreakthroughRateItemApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "breakthrough-rate-item.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(user_id TEXT PRIMARY KEY, level_up_rate INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back "
                "(user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, "
                "day_num INTEGER, all_num INTEGER, UNIQUE(user_id, goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 12))
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 15151, 3, 2, 1, 4),
            )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_breakthrough_rate_item(uow)
        self.application = BreakthroughRateItemApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            rate = conn.execute(
                "SELECT level_up_rate FROM user_xiuxian WHERE user_id=%s", ("user",)
            ).fetchone()[0]
            item = conn.execute(
                "SELECT goods_num, bind_num, day_num, all_num FROM back "
                "WHERE user_id=%s AND goods_id=%s",
                ("user", 15151),
            ).fetchone()
            operation_count = conn.execute(
                "SELECT COUNT(*) FROM breakthrough_rate_item_operations"
            ).fetchone()[0]
        return int(rate), tuple(map(int, item)), int(operation_count)

    def test_use_consumes_elixir_and_increases_rate_atomically(self) -> None:
        result = self.application.apply("rate-1", "user", 15151, 2, 10)

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.state(), (22, (1, 0, 3, 6), 1))

    def test_duplicate_replays_recorded_result_without_mutation(self) -> None:
        first = self.application.apply("rate-repeat", "user", 15151, 1, 5)
        second = self.application.apply("rate-repeat", "user", 15151, 3, 99)

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual((second.quantity, second.rate_gain), (1, 5))
        self.assertEqual(self.state(), (17, (2, 1, 2, 5), 1))

    def test_insufficient_elixir_and_missing_user_do_not_mutate_state(self) -> None:
        result = self.application.apply("rate-poor", "user", 15151, 4, 20)
        missing = self.application.apply("rate-missing", "absent", 15151, 1, 5)

        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(missing.status, "user_missing")
        self.assertEqual(self.state(), (12, (3, 2, 1, 4), 0))

    def test_trigger_failure_rolls_back_elixir_and_rate(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_rate_item BEFORE INSERT ON "
                "breakthrough_rate_item_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.application.apply("rate-fail", "user", 15151, 1, 5)

        self.assertEqual(self.state(), (12, (3, 2, 1, 4), 0))

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, level_up_rate INTEGER)"
                )
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER)")

            with self.assertRaises(sqlite3.OperationalError):
                BreakthroughRateItemApplication(database).apply(
                    "missing-schema", "user", 15151, 1, 5
                )

            with DatabaseUnitOfWork(database) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name='breakthrough_rate_item_operations'"
                    )
                )

    def test_migration_is_routed_only_to_game_database(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }

        self.assertIn("back.009", routed["game_db"])
        for key in routed:
            if key != "game_db":
                self.assertNotIn("back.009", routed[key])


if __name__ == "__main__":
    unittest.main()
