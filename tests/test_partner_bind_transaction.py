import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import PartnerBindService
from tests.test_db_backend import db_backend


class PartnerBindTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.executemany("INSERT INTO user_xiuxian VALUES(%s)", [("invitee",), ("inviter",)])
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE partner(user_id TEXT PRIMARY KEY,partner_id TEXT,bind_time TEXT,affection INTEGER)")
            conn.executemany("INSERT INTO partner VALUES(%s,NULL,NULL,0)", [("invitee",), ("inviter",)])
        self.service = PartnerBindService(self.game, self.player)
        self.bind_time = "2026-07-13 12:00:00"

    def tearDown(self):
        self.temp.cleanup()

    def apply(self, operation="op", *, expected_invitee=None, bind_time=None):
        return self.service.apply(operation, "invitee", "inviter", bind_time=bind_time or self.bind_time, expected_invitee_partner=expected_invitee, expected_inviter_partner=None)

    def state(self):
        with db_backend.connection(self.player) as conn:
            return [tuple(row) for row in conn.execute("SELECT user_id,partner_id,bind_time,affection FROM partner ORDER BY user_id").fetchall()]

    def test_success_duplicate_and_conflict(self):
        self.assertEqual("applied", self.apply().status)
        self.assertEqual([("invitee", "inviter", self.bind_time, 0), ("inviter", "invitee", self.bind_time, 0)], self.state())
        self.assertEqual("duplicate", self.apply().status)
        self.assertEqual("operation_conflict", self.apply(bind_time="2026-07-13 12:00:01").status)

    def test_state_change_and_failure_rollback(self):
        self.assertEqual("state_changed", self.apply(expected_invitee="other").status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE partner_bind_operations(operation_id TEXT PRIMARY KEY,payload TEXT,bind_time TEXT,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_partner_bind BEFORE INSERT ON partner_bind_operations BEGIN SELECT RAISE(ABORT,'forced'); END")
        with self.assertRaises(Exception):
            self.apply("failure")
        self.assertEqual([("invitee", None, None, 0), ("inviter", None, None, 0)], self.state())


if __name__ == "__main__":
    unittest.main()
