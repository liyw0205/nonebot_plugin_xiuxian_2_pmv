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
        self.assertEqual(first["detail"]["status"], "ok")
        self.assertEqual(second["status"], "duplicate")

    def test_active_bath_and_bonus_details_match_command_contract(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE tianti_info SET medicine_end_time=?, medicine_effect=?, medicine_name=? WHERE user_id='u'",
                ("2026-09-14 10:30:00", "1.5", "灵药浴"),
            )
        repo = TiantiSettlementSqlRepository(
            self.database,
            profile_reader=Profile(),
            spirit_vein_multiplier=lambda: 1.2,
        )
        result = repo.settle("active-bath", "u", datetime(2026, 9, 14, 10), sect_fairyland_level=2)
        detail = result["detail"]
        self.assertEqual(detail["status"], "ok")
        self.assertEqual(detail["real_gain"], 237)
        self.assertEqual(detail["bath"]["name"], "灵药浴")
        self.assertEqual(detail["bath"]["effect"], 1.5)
        self.assertFalse(detail["bath_expired"])
        self.assertEqual(detail["sect_bonus"], 0.1)
        self.assertAlmostEqual(detail["spirit_vein_bonus"], 0.2)

    def test_expired_bath_is_cleared_and_reported(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE tianti_info SET medicine_last_time=?, medicine_end_time=?, medicine_effect=?, medicine_name=? WHERE user_id='u'",
                ("2026-09-14 03:30:00", "2026-09-14 09:30:00", "1.5", "过期药浴"),
            )
        result = self.repo.settle("expired-bath", "u", datetime(2026, 9, 14, 10))
        self.assertTrue(result["detail"]["bath_expired"])
        self.assertIsNone(result["detail"]["bath"])
        with sqlite3.connect(self.database) as conn:
            row = conn.execute(
                "SELECT medicine_last_time, medicine_end_time, medicine_effect, medicine_name FROM tianti_info WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(row, (None, None, "0.0", ""))

    def test_first_settlement_initializes_missing_profile_and_empty_window_does_not_advance(self):
        now = datetime(2026, 9, 14, 10)
        first = self.repo.settle("first", "new-user", now)
        self.assertEqual((first["status"], first["detail"]["status"]), ("settled", "init"))
        second = self.repo.settle("too-soon", "new-user", now.replace(second=30))
        self.assertEqual((second["status"], second["detail"]["status"]), ("settled", "empty"))
        with sqlite3.connect(self.database) as conn:
            last_time = conn.execute(
                "SELECT last_settle_time FROM tianti_info WHERE user_id='new-user'"
            ).fetchone()[0]
        self.assertEqual(last_time, "2026-09-14 10:00:00")

    def test_missing_schema_fails_without_state_change(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.db"
            with self.assertRaisesRegex(RuntimeError, "run migrations"):
                TiantiSettlementSqlRepository(database, profile_reader=Profile()).settle("op", "u", datetime(2026, 9, 14, 10))


if __name__ == "__main__":
    unittest.main()
