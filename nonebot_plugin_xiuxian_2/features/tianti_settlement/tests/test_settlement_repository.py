import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..repository import TiantiSettlementSqlRepository


class Profile:
    def default_data(self):
        return {"tianti_level": "one", "tianti_hp": 10, "last_settle_time": None, "medicine_last_time": None, "medicine_end_time": None, "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [], "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {}}

    def levels(self):
        return {"one": {"rank": 1, "hp_gain_per_min": 2, "need_hp": 0}}

    def clean(self, row):
        data = self.default_data()
        data.update({key: value for key, value in row.items() if value is not None})
        return data

    def cap(self, data):
        return 1000


class SettlementRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with sqlite3.connect(self.database) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY, tianti_level TEXT, tianti_hp TEXT, last_settle_time TEXT, medicine_last_time TEXT, medicine_end_time TEXT, medicine_effect TEXT, medicine_name TEXT, opened_qiaoxue TEXT, opened_qiaoxue_detail TEXT, qiaoxue_stage_opened TEXT)")
            conn.execute("INSERT INTO tianti_info (user_id, tianti_level, tianti_hp, last_settle_time, opened_qiaoxue, opened_qiaoxue_detail, qiaoxue_stage_opened) VALUES ('u', 'one', '10', '2026-09-14 09:00:00', '[]', '[]', '{}')")
            conn.execute("CREATE TABLE tianti_settlement_operations (operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_level INTEGER NOT NULL, result_status TEXT NOT NULL, detail_json TEXT NOT NULL)")
        self.repo = TiantiSettlementSqlRepository(self.database, profile_reader=Profile())

    def tearDown(self):
        self.tmp.cleanup()

    def test_settlement_and_replay(self):
        first = self.repo.settle("op", "u", datetime(2026, 9, 14, 10), sect_fairyland_level=0)
        second = self.repo.settle("op", "u", datetime(2026, 9, 14, 10), sect_fairyland_level=0)
        self.assertEqual((first["status"], first["detail"]["mins"], first["detail"]["real_gain"]), ("settled", 60, 120))
        self.assertEqual(second["status"], "duplicate")

    def test_missing_schema_fails_without_state_change(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.db"
            with self.assertRaisesRegex(RuntimeError, "run migrations"):
                TiantiSettlementSqlRepository(database, profile_reader=Profile()).settle("op", "u", datetime(2026, 9, 14, 10))


if __name__ == "__main__":
    unittest.main()
