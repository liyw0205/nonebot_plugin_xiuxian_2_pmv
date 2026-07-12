from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.stone_training_service import (
    StoneTrainingService,
)
from tests.test_db_backend import db_backend


class StoneTrainingServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 1000))
        self.service = StoneTrainingService(self.game_database, self.player_database)
        self.default_data = {
            "tianti_level": "初境", "tianti_hp": 10, "last_settle_time": None,
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }
        self.service._manager._default = lambda: dict(self.default_data)
        self.service._manager._clean_user_data = lambda data: {**self.default_data, **data}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def stone(self) -> int:
        with db_backend.connection(self.game_database) as conn:
            return int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])

    def tianti_hp(self):
        with db_backend.connection(self.player_database) as conn:
            if not conn.table_exists("tianti_info"):
                return None
            row = conn.execute("SELECT tianti_hp FROM tianti_info WHERE user_id=%s", ("user",)).fetchone()
            return int(row[0]) if row else None

    def train(self, operation_id="stone-training-1", requested_stone=100, cap=1000):
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.stone_training_service.get_tianti_cap",
            return_value=cap,
        ):
            return self.service.train(operation_id, "user", requested_stone)

    def test_charges_stone_and_updates_tianti_atomically(self) -> None:
        result = self.train()

        self.assertEqual((result.status, result.stone_cost, result.hp_gain, result.new_hp), ("trained", 100, 10, 20))
        self.assertEqual(self.stone(), 900)
        self.assertEqual(self.tianti_hp(), 20)

    def test_near_cap_only_charges_actual_gain(self) -> None:
        result = self.train(cap=13)

        self.assertEqual((result.stone_cost, result.hp_gain, result.new_hp), (30, 3, 13))
        self.assertEqual(self.stone(), 970)

    def test_at_cap_does_not_charge(self) -> None:
        result = self.train(cap=10)

        self.assertEqual(result.status, "at_cap")
        self.assertEqual(self.stone(), 1000)
        self.assertIsNone(self.tianti_hp())

    def test_insufficient_stone_changes_neither_database(self) -> None:
        result = self.train(requested_stone=1001)

        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.stone(), 1000)
        self.assertIsNone(self.tianti_hp())

    def test_duplicate_does_not_charge_or_train_twice(self) -> None:
        first = self.train("stone-repeat")
        second = self.train("stone-repeat")

        self.assertEqual((first.status, second.status), ("trained", "duplicate"))
        self.assertEqual(self.stone(), 900)
        self.assertEqual(self.tianti_hp(), 20)

    def test_changed_duplicate_request_is_rejected(self) -> None:
        self.train("stone-conflict")
        result = self.train("stone-conflict", requested_stone=200)

        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.stone(), 900)
        self.assertEqual(self.tianti_hp(), 20)

    def test_player_write_failure_rolls_back_stone(self) -> None:
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY)")
            conn.execute(
                "CREATE TRIGGER fail_tianti_write BEFORE INSERT ON tianti_info "
                "BEGIN SELECT RAISE(ABORT, 'write failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.train("stone-write-fail")

        self.assertEqual(self.stone(), 1000)

    def test_operation_write_failure_rolls_back_both_databases(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE tianti_stone_training_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, requested_stone INTEGER NOT NULL, "
                "stone_cost INTEGER NOT NULL, hp_gain INTEGER NOT NULL, new_hp INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_operation_write BEFORE INSERT ON tianti_stone_training_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation write failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.train("operation-write-fail")

        self.assertEqual(self.stone(), 1000)
        self.assertIsNone(self.tianti_hp())


if __name__ == "__main__":
    unittest.main()
