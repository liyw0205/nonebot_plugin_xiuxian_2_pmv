import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..repository import TiantiQiaoxueSqlRepository


class _Profile:
    def default_data(self):
        return {"tianti_level": "one", "tianti_hp": 100, "last_settle_time": None, "medicine_last_time": None, "medicine_end_time": None, "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [], "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {}}

    def clean(self, row):
        data = self.default_data()
        data.update({key: value for key, value in row.items() if value is not None})
        data["opened_qiaoxue"] = [] if not data.get("opened_qiaoxue") else data["opened_qiaoxue"]
        data["opened_qiaoxue_detail"] = [] if not data.get("opened_qiaoxue_detail") else data["opened_qiaoxue_detail"]
        return data

    def levels(self):
        return {"one": {"rank": 1}}

    def qiaoxue_pool(self):
        return [{"name": "窍一", "group": "天罡", "effect_type": "base_per_min_ratio", "effect_value": 0.1}, {"name": "窍二", "group": "地煞", "effect_type": "hp_gain_pct", "effect_value": 0.2}]


class QiaoxueRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with sqlite3.connect(self.database) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY, tianti_level TEXT, tianti_hp TEXT, last_settle_time TEXT, medicine_last_time TEXT, medicine_end_time TEXT, medicine_effect TEXT, medicine_name TEXT, opened_qiaoxue TEXT, opened_qiaoxue_detail TEXT, qiaoxue_stage_opened TEXT)")
            conn.execute("INSERT INTO tianti_info (user_id, tianti_level, tianti_hp, opened_qiaoxue, opened_qiaoxue_detail) VALUES ('u', 'one', '100', '[]', '[]')")
            conn.execute("CREATE TABLE tianti_qiaoxue_operations (operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, roll INTEGER NOT NULL, qiaoxue_json TEXT NOT NULL, hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, opened_count INTEGER NOT NULL, unlock_limit INTEGER NOT NULL)")
        self.repo = TiantiQiaoxueSqlRepository(self.database, profile_reader=_Profile())

    def tearDown(self):
        self.tmp.cleanup()

    def test_open_and_replay(self):
        first = self.repo.open_qiaoxue("op", "u", 0)
        second = self.repo.open_qiaoxue("op", "u", 0)
        self.assertEqual((first.status, first.qiaoxue["name"], first.hp_cost, first.new_hp), ("opened", "窍一", 10, 90))
        self.assertEqual(second.status, "duplicate")

    def test_hp_insufficient_does_not_write(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE tianti_info SET tianti_hp=0")
        result = self.repo.open_qiaoxue("low", "u", 0)
        self.assertEqual(result.status, "hp_insufficient")
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM tianti_qiaoxue_operations").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
