from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.transaction_service import InfiltrateFailureService
from tests.test_db_backend import db_backend


class InfiltrateFailureServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 1000))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER,infiltrate_date TEXT,infiltrate_active_count INTEGER,infiltrate_random_count INTEGER,intrude_date TEXT,intrude_count INTEGER,patrol_guard INTEGER)")
            conn.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u", 1, "", 0, 0, "", 0, 0))
            conn.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("t", 1, "", 0, 0, "", 0, 1))
        self.service = InfiltrateFailureService(self.game, self.player)

    def tearDown(self):
        self.temp_dir.cleanup()

    def settle(self, operation_id="op", **overrides):
        values = dict(day="2026-07-13", field="infiltrate_active_count", loss=200, guard=True)
        values.update(overrides)
        return self.service.settle(operation_id, "u", "t", values["day"], values["field"], 3, 3, values["loss"], values["guard"])

    def state(self):
        with db_backend.connection(self.game) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            visitor = tuple(conn.execute("SELECT infiltrate_date,infiltrate_active_count FROM dongfu_status WHERE user_id=%s", ("u",)).fetchone())
            target = tuple(conn.execute("SELECT intrude_date,intrude_count,patrol_guard FROM dongfu_status WHERE user_id=%s", ("t",)).fetchone())
        return stone, visitor, target

    def test_success_and_duplicate_settle_once(self):
        self.assertEqual(self.settle("same").status, "settled")
        self.assertEqual(self.settle("same").status, "duplicate")
        self.assertEqual(self.state(), (800, ("2026-07-13", 1), ("2026-07-13", 1, 0)))

    def test_limit_does_not_charge_or_change_state(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE dongfu_status SET infiltrate_date=%s,infiltrate_active_count=%s WHERE user_id=%s", ("2026-07-13", 3, "u"))
        self.assertEqual(self.settle("limit").status, "daily_limit")
        self.assertEqual(self.state(), (1000, ("2026-07-13", 3), ("", 0, 1)))

    def test_operation_failure_rolls_back_all_changes(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE dongfu_infiltrate_failure_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,infiltrate_left INTEGER NOT NULL,intrude_left INTEGER NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_infiltrate BEFORE INSERT ON dongfu_infiltrate_failure_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.state(), (1000, ("", 0), ("", 0, 1)))


if __name__ == "__main__":
    unittest.main()
