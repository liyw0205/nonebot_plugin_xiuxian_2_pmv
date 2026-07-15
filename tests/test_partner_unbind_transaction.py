import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import PartnerUnbindService
from tests.test_db_backend import db_backend


class PartnerUnbindTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.executemany("INSERT INTO user_xiuxian VALUES(%s)", [("a",), ("b",)])
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE partner(user_id TEXT PRIMARY KEY,partner_id TEXT,bind_time TEXT,affection INTEGER)")
            conn.executemany("INSERT INTO partner VALUES(%s,%s,%s,%s)", [("a", "b", "2026-07-01 00:00:00", 8), ("b", "a", "2026-07-01 00:00:00", 9)])
        self.service = PartnerUnbindService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def apply(self, operation="op", *, affection=8, checked_at="2026-07-13 00:00:00"):
        return self.service.apply(operation, "a", "b", expected_user_bind_time="2026-07-01 00:00:00", expected_partner_bind_time="2026-07-01 00:00:00", expected_user_affection=affection, expected_partner_affection=9, checked_at=checked_at, minimum_days=7)

    def state(self):
        with db_backend.connection(self.player) as conn:
            return [tuple(row) for row in conn.execute("SELECT user_id,partner_id,bind_time,affection FROM partner ORDER BY user_id").fetchall()]

    def test_success_duplicate_and_conflict(self):
        self.assertEqual("applied", self.apply().status)
        self.assertEqual([("a", None, None, 0), ("b", None, None, 0)], self.state())
        self.assertEqual("duplicate", self.apply().status)
        self.assertEqual("operation_conflict", self.apply(affection=7).status)

    def test_state_change_minimum_period_and_failure_rollback(self):
        self.assertEqual("state_changed", self.apply(affection=7).status)
        self.assertEqual("too_early", self.apply("early", checked_at="2026-07-05 00:00:00").status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE partner_unbind_operations(operation_id TEXT PRIMARY KEY,payload TEXT,partner_id TEXT,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_partner_unbind BEFORE INSERT ON partner_unbind_operations BEGIN SELECT RAISE(ABORT,'forced'); END")
        with self.assertRaises(Exception):
            self.apply("failure")
        self.assertEqual([("a", "b", "2026-07-01 00:00:00", 8), ("b", "a", "2026-07-01 00:00:00", 9)], self.state())


if __name__ == "__main__":
    unittest.main()
