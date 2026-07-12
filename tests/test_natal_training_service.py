from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure.training_service import NatalTrainingService
from tests.test_db_backend import db_backend


class NatalTrainingServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 10_000_000))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE natal_treasure (user_id TEXT PRIMARY KEY, form INTEGER, level INTEGER, exp INTEGER, max_exp INTEGER)")
            conn.execute("INSERT INTO natal_treasure VALUES (%s, %s, %s, %s, %s)", ("user", 1, 0, 0, 100))
        self.service = NatalTrainingService(self.game, self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def train(self, operation_id, amount=5):
        return self.service.train(operation_id, "user", amount, base_cost=1_000_000,
                                  growth_rate=0.5, max_level=10,
                                  max_exp_base=100, max_exp_growth=50)

    def state(self):
        with db_backend.connection(self.game) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT level, exp, max_exp FROM natal_treasure WHERE user_id=%s", ("user",)).fetchone()
        return stone, tuple(int(value) for value in row)

    def test_training_charges_and_adds_exp_atomically(self) -> None:
        result = self.train("train", 5)
        self.assertEqual((result.status, result.stone_cost, result.exp), ("trained", 5_000_000, 5))
        self.assertEqual(self.state(), (5_000_000, (0, 5, 100)))

    def test_training_caps_at_level_boundary_and_levels_up(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE natal_treasure SET exp=%s WHERE user_id=%s", (98, "user"))
        result = self.train("level", 10)
        self.assertEqual((result.exp_added, result.stone_cost, result.level, result.exp), (2, 2_000_000, 1, 0))
        self.assertEqual(self.state(), (8_000_000, (1, 0, 150)))

    def test_insufficient_stone_changes_neither_database(self) -> None:
        result = self.train("short", 20)
        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.state(), (10_000_000, (0, 0, 100)))

    def test_duplicate_does_not_charge_twice_and_conflict_is_rejected(self) -> None:
        first = self.train("repeat", 5)
        duplicate = self.train("repeat", 5)
        conflict = self.train("repeat", 6)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("trained", "duplicate", "state_changed"))
        self.assertEqual(self.state(), (5_000_000, (0, 5, 100)))

    def test_player_write_failure_rolls_back_stone(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TRIGGER fail_natal BEFORE UPDATE ON natal_treasure BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.train("rollback", 5)
        self.assertEqual(self.state(), (10_000_000, (0, 0, 100)))


if __name__ == "__main__":
    unittest.main()
