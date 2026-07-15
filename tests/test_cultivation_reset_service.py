import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_lunhui.transaction_service import (
    CultivationResetService,
)
from tests.test_db_backend import db_backend


class CultivationResetServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,"
                "level_up_rate INTEGER,power INTEGER,hp INTEGER,mp INTEGER,atk INTEGER)"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", "感气境中期", 900, 7, 99, 300, 500, 80),
            )
        self.service = CultivationResetService(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def test_atomic_reset_idempotency_and_conflict(self):
        result = self.service.reset("op", "u", "感气境中期", 900)
        self.assertEqual("applied", result.status)
        self.assertEqual("duplicate", self.service.reset("op", "u", "感气境中期", 900).status)
        # payload is request identity [user_id] only; expected_* is concurrency, not key.
        self.assertEqual("duplicate", self.service.reset("op", "u", "感气境中期", 901).status)
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT level,exp,level_up_rate,power,hp,mp,atk FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(("江湖好手", 100, 0, 0, 50, 100, 10), tuple(row))

    def test_rejection_state_change_and_rollback(self):
        self.assertEqual("state_changed", self.service.reset("stale", "u", "感气境中期", 899).status)
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET level='筑基境初期' WHERE user_id='u'")
        self.assertEqual("level_rejected", self.service.reset("level", "u", "筑基境初期", 900).status)
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE cultivation_reset_operations (operation_id TEXT PRIMARY KEY,payload TEXT,reset_exp INTEGER)")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET level='感气境中期' WHERE user_id='u'")
            conn.execute(
                "CREATE TRIGGER fail_reset BEFORE INSERT ON cultivation_reset_operations "
                "BEGIN SELECT RAISE(ABORT,'fail'); END"
            )
        with self.assertRaises(Exception):
            self.service.reset("fail", "u", "感气境中期", 900)
        with db_backend.connection(self.database) as conn:
            row = conn.execute("SELECT level,exp FROM user_xiuxian WHERE user_id='u'").fetchone()
        self.assertEqual(("感气境中期", 900), tuple(row))
