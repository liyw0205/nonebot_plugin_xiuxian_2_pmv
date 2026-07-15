import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.transaction_service import CardDisassembleService
from tests.test_db_backend import db_backend


class CardDisassembleServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "impart.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE impart_cards (user_id TEXT, card_name TEXT, quantity INTEGER, PRIMARY KEY(user_id, card_name))")
            conn.execute("CREATE TABLE xiuxian_impart (user_id TEXT PRIMARY KEY, stone_num INTEGER)")
            conn.execute("INSERT INTO impart_cards VALUES (%s,%s,%s)", ("u", "卡", 4))
            conn.execute("INSERT INTO xiuxian_impart VALUES (%s,%s)", ("u", 10))
        self.service = CardDisassembleService(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def state(self):
        with db_backend.connection(self.db) as conn:
            card = conn.execute("SELECT quantity FROM impart_cards WHERE user_id=%s AND card_name=%s", ("u", "卡")).fetchone()[0]
            stone = conn.execute("SELECT stone_num FROM xiuxian_impart WHERE user_id=%s", ("u",)).fetchone()[0]
            return int(card), int(stone)

    def test_success_duplicate_and_conflict(self):
        first = self.service.disassemble("op", "u", "卡", 2, 4, 10)
        duplicate = self.service.disassemble("op", "u", "卡", 2, 4, 10)
        # different expected snapshot on same op must still replay
        conflict = self.service.disassemble("op", "u", "卡", 2, 1, 99)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "duplicate"))
        self.assertEqual(self.state(), (2, 14))

    def test_rejections_and_stale_snapshot_change_nothing(self):
        self.assertEqual(self.service.disassemble("stale", "u", "卡", 1, 3, 10).status, "state_changed")
        self.assertEqual(self.service.disassemble("keep", "u", "卡", 4, 4, 10).status, "card_missing")
        self.assertEqual(self.service.disassemble("missing", "x", "卡", 1, 0, 0).status, "user_missing")
        self.assertEqual(self.state(), (4, 10))

    def test_operation_failure_rolls_back_card_and_stones(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE impart_card_disassemble_operations (operation_id TEXT PRIMARY KEY,payload TEXT,card_quantity INTEGER,stone_quantity INTEGER)")
            conn.execute("CREATE TRIGGER fail_disassemble BEFORE INSERT ON impart_card_disassemble_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.disassemble("fail", "u", "卡", 2, 4, 10)
        self.assertEqual(self.state(), (4, 10))


if __name__ == "__main__":
    unittest.main()
