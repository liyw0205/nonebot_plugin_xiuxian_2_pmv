from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.blessed_flag_replace_application import (
    BlessedFlagReplaceApplication,
)
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_blessed_flag_replace
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class BlessedFlagReplaceApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,blessed_spot_flag INTEGER)"
            )
            conn.execute("CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY,blessed_spot INTEGER)")
            conn.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER,update_time TEXT,action_time TEXT,UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 1))
            conn.execute("INSERT INTO BuffInfo VALUES (%s,%s)", ("u", 2))
            conn.execute(
                "INSERT INTO back VALUES (%s,%s,%s,%s,NULL,NULL)", ("u", 9001, 3, 2)
            )
        with db_backend.transaction(self.player) as conn:
            speed = db_backend.quote_ident("药材速度")
            conn.execute(f"CREATE TABLE mix_elixir_info (user_id TEXT PRIMARY KEY,{speed} TEXT)")
            conn.execute(
                f"INSERT INTO mix_elixir_info (user_id,{speed}) VALUES (%s,%s)",
                ("u", "20"),
            )
        with DatabaseUnitOfWork(self.game) as uow:
            apply_blessed_flag_replace(uow)
        self.application = BlessedFlagReplaceApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.game) as conn:
            level = conn.execute("SELECT blessed_spot FROM BuffInfo WHERE user_id=%s", ("u",)).fetchone()
            item = conn.execute(
                "SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("u", 9001),
            ).fetchone()
        with db_backend.connection(self.player) as conn:
            speed = conn.execute(
                f"SELECT {db_backend.quote_ident('药材速度')} FROM mix_elixir_info WHERE user_id=%s",
                ("u",),
            ).fetchone()
        return int(level[0]), tuple(map(int, item)), int(speed[0])

    def replace(self, operation_id="replace-1", **overrides):
        params = dict(
            operation_id=operation_id,
            user_id="u",
            item_id=9001,
            target_level=4,
            herb_speed=50,
            expected_level=2,
            expected_herb_speed=20,
            expected_quantity=3,
        )
        params.update(overrides)
        return self.application.replace(**params)

    def test_replace_is_atomic_and_idempotent(self) -> None:
        first, second = self.replace(), self.replace()
        self.assertEqual((first.status, first.previous_level, first.current_level, first.herb_speed, first.quantity), ("applied", 2, 4, 50, 1))
        self.assertEqual(second.status, "duplicate")
        self.assertEqual((4, (2, 1), 50), self.state())

    def test_payload_conflict_and_snapshot_changes_do_not_mutate(self) -> None:
        self.replace()
        before = self.state()
        self.assertEqual("state_changed", self.replace(target_level=5).status)
        self.assertEqual(before, self.state())

    def test_trigger_failure_rolls_back_both_databases(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER reject_replace BEFORE INSERT ON blessed_flag_replace_operations "
                "BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        before = self.state()
        with self.assertRaises(sqlite3.IntegrityError):
            self.replace("rollback")
        self.assertEqual(before, self.state())

    def test_request_does_not_create_missing_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game, player = Path(directory) / "game.db", Path(directory) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,blessed_spot_flag INTEGER)")
                conn.execute("CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,blessed_spot INTEGER)")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES ('u',1)")
                conn.execute("INSERT INTO BuffInfo VALUES ('u',1)")
                conn.execute("INSERT INTO back VALUES ('u',9001,1)")
            with db_backend.transaction(player) as conn:
                speed = db_backend.quote_ident("药材速度")
                conn.execute(f"CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,{speed} TEXT)")
                conn.execute(f"INSERT INTO mix_elixir_info VALUES ('u','1')")
            with self.assertRaises(sqlite3.OperationalError):
                BlessedFlagReplaceApplication(game, player).replace(
                    "missing-schema", "u", 9001, 2, 3,
                    expected_level=1, expected_herb_speed=1, expected_quantity=1,
                )
            with DatabaseUnitOfWork(game) as uow:
                self.assertIsNone(uow.query_one("SELECT name FROM sqlite_master WHERE name='blessed_flag_replace_operations'"))

    def test_migration_is_routed_only_to_game_database(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("back.012", routed["game_db"])
        for key in routed:
            if key != "game_db":
                self.assertNotIn("back.012", routed[key])


if __name__ == "__main__":
    unittest.main()
