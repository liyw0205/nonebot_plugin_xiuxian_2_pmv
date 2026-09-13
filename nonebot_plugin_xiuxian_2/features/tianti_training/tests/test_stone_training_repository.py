from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import json

from ..repository import StoneTrainingSqlRepository, TiantiProfileReader


class StoneTrainingSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        (root / "炼体").mkdir()
        (root / "炼体" / "炼体境界.json").write_text(
            json.dumps({"初境": {"rank": 1, "need_hp": 0}}, ensure_ascii=False), encoding="utf-8"
        )
        import sqlite3
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('user', 1000)")

        self.default = {
            "tianti_level": "初境", "tianti_hp": 10, "last_settle_time": None,
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }
        manager = type("Manager", (), {
            "_default": lambda _self: dict(self.default),
            "_clean_user_data": lambda _self, data: {**self.default, **data},
        })()
        self.repository = StoneTrainingSqlRepository(
            self.game, self.player, data_manager=manager,
            cap_provider=lambda _data: 1000,
            profile_reader=TiantiProfileReader(root),
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def call(self, operation_id="op-1", requested_stone=100, cap=1000):
        self.repository._cap_provider = lambda _data: cap
        return self.repository.train(operation_id, "user", requested_stone)

    def read(self, database: Path, sql: str):
        import sqlite3
        with sqlite3.connect(database) as conn:
            row = conn.execute(sql).fetchone()
            return row[0] if row else None

    def test_success_and_replay_are_atomic(self):
        import sqlite3
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY, tianti_level TEXT, tianti_hp TEXT)")
            conn.execute("INSERT INTO tianti_info VALUES ('user', '初境', '10')")
        first = self.call()
        second = self.call()
        self.assertEqual((first.status, first.stone_cost, first.hp_gain, first.new_hp), ("trained", 100, 10, 20))
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(self.read(self.game, "SELECT stone FROM user_xiuxian"), 900)
        self.assertEqual(self.read(self.player, "SELECT tianti_hp FROM tianti_info"), "20")

    def test_rejects_insufficient_stone_without_creating_player_row(self):
        result = self.call(requested_stone=1001)
        self.assertEqual(result.status, "stone_insufficient")
        self.assertIsNone(self.read(self.player, "SELECT tianti_hp FROM tianti_info"))
        self.assertEqual(self.read(self.game, "SELECT stone FROM user_xiuxian"), 1000)

    def test_player_write_failure_rolls_back_game_database(self):
        import sqlite3
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY)")
            conn.execute("CREATE TRIGGER fail_tianti_write BEFORE INSERT ON tianti_info BEGIN SELECT RAISE(ABORT, 'write failed'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.call("write-fail")
        self.assertEqual(self.read(self.game, "SELECT stone FROM user_xiuxian"), 1000)


if __name__ == "__main__":
    unittest.main()
