import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..repository import TiantiBreakthroughSqlRepository


class _Profile:
    def default_data(self):
        return {"tianti_level": "one", "tianti_hp": 100, "last_settle_time": None, "medicine_last_time": None, "medicine_end_time": None, "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [], "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {}}

    def clean(self, row):
        data = self.default_data()
        data.update({k: v for k, v in row.items() if v is not None})
        return data

    def next_level(self, level):
        return ("two", {"need_hp": 80, "min_xx_level": "rank"}) if level == "one" else (None, {})

    def cultivation_rank(self, level):
        return 2


class BreakthroughRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with sqlite3.connect(self.database) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY, tianti_level TEXT, tianti_hp TEXT, last_settle_time TEXT, medicine_last_time TEXT, medicine_end_time TEXT, medicine_effect TEXT, medicine_name TEXT, opened_qiaoxue TEXT, opened_qiaoxue_detail TEXT, qiaoxue_stage_opened TEXT)")
            conn.execute("INSERT INTO tianti_info (user_id, tianti_level, tianti_hp) VALUES ('u', 'one', '100')")
            conn.execute("CREATE TABLE tianti_breakthrough_operations (operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, cultivation_rank INTEGER NOT NULL, roll_success INTEGER NOT NULL, old_level TEXT NOT NULL, new_level TEXT NOT NULL, hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, success INTEGER NOT NULL)")
        self.repo = TiantiBreakthroughSqlRepository(self.database, profile_reader=_Profile())

    def tearDown(self):
        self.tmp.cleanup()

    def test_success_and_replay(self):
        first = self.repo.breakthrough("op", "u", cultivation_rank=1, roll_success=True)
        second = self.repo.breakthrough("op", "u", cultivation_rank=1, roll_success=True)
        self.assertEqual((first.status, first.new_level, first.hp_cost, first.new_hp), ("completed", "two", 5, 95))
        self.assertEqual(second.status, "duplicate")

    def test_insufficient_hp_does_not_write_operation(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE tianti_info SET tianti_hp=50")
        result = self.repo.breakthrough("low", "u", cultivation_rank=1, roll_success=True)
        self.assertEqual(result.status, "hp_insufficient")
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM tianti_breakthrough_operations").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
