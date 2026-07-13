import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.partner_token_service import PartnerTokenUseService
from tests.test_db_backend import db_backend


class PartnerTokenUseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER DEFAULT 0,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO back VALUES(%s,%s,%s,%s)", ("u", 9001, 5, 2))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE partner_two_exp_usage(user_id TEXT PRIMARY KEY,used_count INTEGER NOT NULL)")
            conn.execute("INSERT INTO partner_two_exp_usage VALUES(%s,%s)", ("u", 3))
        self.service = PartnerTokenUseService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def apply(self, operation="op", *, requested=2, items=5, used=3):
        return self.service.apply(operation, "u", 9001, requested_count=requested, expected_item_count=items, expected_used_count=used)

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id='u'").fetchone()
        with db_backend.connection(self.player) as conn:
            used = conn.execute("SELECT used_count FROM partner_two_exp_usage WHERE user_id='u'").fetchone()[0]
        return tuple(item), int(used)

    def test_success_duplicate_and_conflict(self):
        result = self.apply()
        self.assertEqual(("applied", 2, 1, 3), (result.status, result.used_tokens, result.used_count, result.item_remaining))
        self.assertEqual(((3, 0), 1), self.state())
        self.assertEqual("duplicate", self.apply().status)
        self.assertEqual("operation_conflict", self.apply(requested=1).status)
        self.assertEqual(((3, 0), 1), self.state())

    def test_state_change_and_failure_rollback(self):
        self.assertEqual("state_changed", self.apply(items=4).status)
        self.assertEqual(((5, 2), 3), self.state())
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE partner_token_operations(operation_id TEXT PRIMARY KEY,payload TEXT,used_tokens INTEGER,used_count INTEGER,item_remaining INTEGER,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_partner_token BEFORE INSERT ON partner_token_operations BEGIN SELECT RAISE(ABORT,'forced'); END")
        with self.assertRaises(Exception):
            self.apply("failure")
        self.assertEqual(((5, 2), 3), self.state())


if __name__ == "__main__":
    unittest.main()
