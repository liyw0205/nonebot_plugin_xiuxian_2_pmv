from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import BlessedSpotService
from tests.test_db_backend import db_backend


class BlessedSpotServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER,blessed_spot_flag INTEGER,blessed_spot_name TEXT)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s)", ("u", 500, 0, ""))
        self.service = BlessedSpotService(self.game, self.player)

    def tearDown(self):
        self.tmp.cleanup()

    def state(self):
        with db_backend.connection(self.game) as conn:
            user = tuple(conn.execute("SELECT stone,blessed_spot_flag,blessed_spot_name FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone())
        with db_backend.connection(self.player) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("mix_elixir_info",),
            ).fetchone()
            row = conn.execute(
                'SELECT "收取时间","灵田数量","炼丹记录" FROM mix_elixir_info WHERE user_id=%s',
                ("u",),
            ).fetchone() if exists else None
        return user, tuple(row) if row else None

    def test_open_initializes_both_databases_and_is_idempotent(self):
        first = self.service.open("open-1", "u", 300, "洞府", "2026-07-13 12:00:00")
        second = self.service.open("open-1", "u", 300, "洞府", "2026-07-13 12:00:00")
        self.assertEqual("applied", first.status)
        self.assertEqual("duplicate", second.status)
        self.assertEqual(((200, 1, "洞府"), ("2026-07-13 12:00:00", "1", "{}")), self.state())

    def test_conflict_and_insufficient_stone_do_not_mutate(self):
        self.assertEqual("stone_insufficient", self.service.open("poor", "u", 600, "洞府", "now").status)
        before = self.state()
        self.service.open("same", "u", 300, "洞府", "now")
        applied = self.state()
        self.assertEqual("state_changed", self.service.open("same", "u", 301, "洞府", "now").status)
        self.assertNotEqual(before, applied)
        self.assertEqual(applied, self.state())

    def test_operation_failure_rolls_back_cross_database_changes(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE blessed_spot_operations (operation_id TEXT PRIMARY KEY,action TEXT,payload TEXT,result_json TEXT)")
            conn.execute("CREATE TRIGGER reject_open BEFORE INSERT ON blessed_spot_operations BEGIN SELECT RAISE(ABORT,'reject'); END")
        before = self.state()
        with self.assertRaises(db_backend.IntegrityError):
            self.service.open("rollback", "u", 300, "洞府", "now")
        self.assertEqual(before, self.state())


    def test_field_upgrade_is_atomic_and_idempotent(self):
        self.service.open("open", "u", 300, "洞府", "now")
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (500, "u"))
        first = self.service.upgrade_field("upgrade", "u", 1, 200)
        second = self.service.upgrade_field("upgrade", "u", 1, 200)
        self.assertEqual(("applied", 1, 2), (first.status, first.previous_level, first.current_level))
        self.assertEqual("duplicate", second.status)
        self.assertEqual(((300, 1, "洞府"), ("now", "2", "{}")), self.state())

    def test_rename_checks_snapshot_and_replays_result(self):
        self.service.open("open", "u", 300, "旧名", "now")
        first = self.service.rename("rename", "u", "旧名", "新名")
        second = self.service.rename("rename", "u", "旧名", "新名")
        self.assertEqual("applied", first.status)
        self.assertEqual(("duplicate", "新名"), (second.status, second.name))
        self.assertEqual("state_changed", self.service.rename("other", "u", "旧名", "另名").status)
        self.assertEqual("新名", self.state()[0][2])

if __name__ == "__main__":
    unittest.main()
