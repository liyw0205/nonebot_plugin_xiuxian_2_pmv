import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.transaction_service import LoveSandUseService
from tests.test_db_backend import db_backend


class LoveSandUseServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.impart, self.player = root / "game.db", root / "impart.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s)", ("u", 20016, 3, 3))
        with db_backend.transaction(self.impart) as conn:
            conn.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)")
            conn.execute("INSERT INTO xiuxian_impart VALUES (%s,%s)", ("u", 7))
        self.service = LoveSandUseService(self.game, self.impart, self.player)

    def tearDown(self): self.temp.cleanup()

    def test_success_duplicate_conflict_and_state_change(self):
        first = self.service.apply("op", "u", 20016, 2, 40, 3, 7)
        duplicate = self.service.apply("op", "u", 20016, 2, 40, 3, 7)
        conflict = self.service.apply("op", "u", 20016, 1, 20, 1, 47)
        stale = self.service.apply("stale", "u", 20016, 1, 10, 2, 47)
        self.assertEqual((first.status, duplicate.status, conflict.status, stale.status), ("applied", "duplicate", "operation_conflict", "state_changed"))
        with db_backend.connection(self.game) as conn: self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 1)
        with db_backend.connection(self.impart) as conn: self.assertEqual(conn.execute("SELECT stone_num FROM xiuxian_impart").fetchone()[0], 47)
        with db_backend.connection(self.player) as conn: self.assertEqual(tuple(conn.execute('SELECT "思恋流沙使用","思恋结晶获取" FROM statistics').fetchone()), (2, 40))

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE love_sand_operations(operation_id TEXT PRIMARY KEY,payload TEXT,gained INTEGER,stone_num INTEGER,item_remaining INTEGER)")
            conn.execute("CREATE TRIGGER fail_love_sand BEFORE INSERT ON love_sand_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError): self.service.apply("fail", "u", 20016, 1, 30, 3, 7)
        with db_backend.connection(self.game) as conn: self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 3)
        with db_backend.connection(self.impart) as conn: self.assertEqual(conn.execute("SELECT stone_num FROM xiuxian_impart").fetchone()[0], 7)


if __name__ == "__main__": unittest.main()
