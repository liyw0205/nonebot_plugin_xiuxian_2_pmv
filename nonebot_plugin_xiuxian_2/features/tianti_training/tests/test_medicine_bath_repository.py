import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..repository import TiantiMedicineBathSqlRepository


class Profile:
    def default_data(self):
        return {
            "tianti_level": "one", "tianti_hp": 10, "last_settle_time": None,
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }

    def levels(self):
        return {"one": {"rank": 1, "hp_gain_per_min": 2, "need_hp": 0}}

    def clean(self, row):
        data = self.default_data()
        data.update({key: value for key, value in row.items() if value is not None})
        return data

    def cap(self, data):
        return 1000


class MedicineBathRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, UNIQUE(user_id, goods_id))")
            conn.execute("INSERT INTO back VALUES ('u', 1, 5, 5)")
            conn.execute("CREATE TABLE tianti_medicine_bath_operations (operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_json TEXT NOT NULL, result_json TEXT NOT NULL)")
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY, tianti_level TEXT, tianti_hp TEXT, last_settle_time TEXT, medicine_last_time TEXT, medicine_end_time TEXT, medicine_effect TEXT, medicine_name TEXT, opened_qiaoxue TEXT, opened_qiaoxue_detail TEXT, qiaoxue_stage_opened TEXT)")
            conn.execute("INSERT INTO tianti_info (user_id, tianti_level, tianti_hp, opened_qiaoxue, opened_qiaoxue_detail, qiaoxue_stage_opened) VALUES ('u', 'one', '10', '[]', '[]', '{}')")
        self.repo = TiantiMedicineBathSqlRepository(self.game, self.player, profile_reader=Profile())
        self.plan = ({"item_id": 1, "name": "herb", "amount": 2},)
        self.now = datetime(2026, 9, 14, 10)

    def tearDown(self):
        self.tmp.cleanup()

    def test_success_consumes_inventory_and_replays(self):
        first = self.repo.apply_bath("op", "u", self.plan, 1.5, "slot", self.now, 60)
        second = self.repo.apply_bath("op", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual((first.status, first.effect, first.end_time), ("applied", 1.5, "2026-09-14 11:00:00"))
        self.assertEqual(second.status, "duplicate")
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1").fetchone()[0], 3)
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(conn.execute("SELECT medicine_effect FROM tianti_info WHERE user_id='u'").fetchone()[0], "1.5")

    def test_insufficient_inventory_does_not_write(self):
        result = self.repo.apply_bath("short", "u", ({"item_id": 1, "name": "herb", "amount": 6},), 1.5, "slot", self.now, 60)
        self.assertEqual(result.status, "item_insufficient")
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1").fetchone()[0], 5)

    def test_player_write_failure_rolls_back_inventory(self):
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TRIGGER fail_bath BEFORE UPDATE OF medicine_effect ON tianti_info BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.apply_bath("fail", "u", self.plan, 1.5, "slot", self.now, 60)
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1").fetchone()[0], 5)
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM tianti_medicine_bath_operations").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
