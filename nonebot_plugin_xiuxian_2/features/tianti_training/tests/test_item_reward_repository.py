import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..repository import TiantiItemRewardSqlRepository


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


class ItemRewardRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, UNIQUE(user_id, goods_id))")
            conn.execute("INSERT INTO back VALUES ('u', 1, 3, 3)")
            conn.execute("CREATE TABLE tianti_item_reward_operations (operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, item_id INTEGER NOT NULL, quantity INTEGER NOT NULL, minutes INTEGER NOT NULL, detail_json TEXT NOT NULL)")
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY, tianti_level TEXT, tianti_hp TEXT, last_settle_time TEXT, medicine_last_time TEXT, medicine_end_time TEXT, medicine_effect TEXT, medicine_name TEXT, opened_qiaoxue TEXT, opened_qiaoxue_detail TEXT, qiaoxue_stage_opened TEXT)")
            conn.execute("INSERT INTO tianti_info (user_id, tianti_level, tianti_hp, opened_qiaoxue, opened_qiaoxue_detail, qiaoxue_stage_opened) VALUES ('u', 'one', '10', '[]', '[]', '{}')")
        self.repo = TiantiItemRewardSqlRepository(self.game, self.player, profile_reader=Profile())

    def tearDown(self):
        self.tmp.cleanup()

    def test_apply_and_replay(self):
        first = self.repo.apply_item_reward("op", "u", 1, 2, 30, settled_at=datetime(2026, 9, 14, 10))
        second = self.repo.apply_item_reward("op", "u", 1, 2, 30, settled_at=datetime(2026, 9, 14, 10))
        self.assertEqual((first.status, first.minutes, first.detail["real_gain"]), ("applied", 60, 120))
        self.assertEqual(second.status, "duplicate")

    def test_insufficient_item_does_not_write(self):
        result = self.repo.apply("short", "u", 1, 4, 30, settled_at=datetime(2026, 9, 14, 10))
        self.assertEqual(result.status, "item_insufficient")
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 3)


if __name__ == "__main__":
    unittest.main()
