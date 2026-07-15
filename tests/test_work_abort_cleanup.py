from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.transaction_service import (
    WorkAbortCleanupService,
)
from tests.test_db_backend import db_backend


class WorkAbortCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "game.db"
        self.offer = {"tasks": {"镇妖": {"time": 5}}, "status": 2, "refresh_time": "old"}
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.execute("CREATE TABLE work_offer_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT,updated_at TEXT)")
            conn.execute("CREATE TABLE work_active_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT,updated_at TEXT)")
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("u", 5_000_000))
            conn.execute("INSERT INTO user_cd VALUES(%s,%s,%s,%s)", ("u", 2, "start", "镇妖"))
            conn.execute("INSERT INTO work_offer_snapshots VALUES(%s,%s,%s)", ("u", json.dumps(self.offer), "old"))
            conn.execute("INSERT INTO work_active_snapshots VALUES(%s,%s,%s)", ("u", "{}", "start"))
        self.service = WorkAbortCleanupService(self.db)
        self.cd = {"type": 2, "create_time": "start", "scheduled_time": "镇妖"}

    def tearDown(self):
        self.temp.cleanup()

    def state(self):
        with db_backend.connection(self.db) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0]
            cd = conn.execute("SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=%s", ("u",)).fetchone()
            offers = conn.execute("SELECT COUNT(*) FROM work_offer_snapshots").fetchone()[0]
            active = conn.execute("SELECT COUNT(*) FROM work_active_snapshots").fetchone()[0]
        return int(stone), tuple(cd), int(offers), int(active)

    def test_active_abort_penalty_cd_and_snapshots_are_atomic(self):
        result = self.service.cleanup(
            "abort", "u", "active_abort", self.cd, self.offer, 5_000_000, 4_000_000
        )
        self.assertEqual((result.status, result.penalty, result.stone_remaining), ("applied", 4_000_000, 1_000_000))
        self.assertEqual(self.state(), (1_000_000, (0, "0", None), 0, 0))

    def test_duplicate_conflict_and_state_change(self):
        self.assertEqual(
            self.service.cleanup("same", "u", "active_abort", self.cd, self.offer, 5_000_000, 4_000_000).status,
            "applied",
        )
        self.assertEqual(
            self.service.cleanup("same", "u", "active_abort", self.cd, self.offer, 5_000_000, 4_000_000).status,
            "duplicate",
        )
        self.assertEqual(
            self.service.cleanup("same", "u", "active_abort", self.cd, self.offer, 5_000_000, 1).status,
            "operation_conflict",
        )
        self.assertEqual(
            self.service.cleanup("stale", "u", "reset", self.cd, None).status,
            "state_changed",
        )

    def test_offer_abort_and_admin_reset_clear_state_without_penalty(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id='u'")
        cd = {"type": 0, "create_time": "0", "scheduled_time": None}
        result = self.service.cleanup("offer", "u", "offer_abort", cd, self.offer)
        self.assertEqual((result.status, result.penalty), ("applied", 0))
        self.assertEqual(self.state()[0], 5_000_000)
        result = self.service.cleanup("reset", "u", "reset", cd, None)
        self.assertEqual(result.status, "applied")

    def test_operation_failure_rolls_back_everything(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "CREATE TABLE work_abort_cleanup_operations(operation_id TEXT PRIMARY KEY,payload TEXT,"
                "reason TEXT,penalty INTEGER,stone_remaining INTEGER,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_cleanup BEFORE INSERT ON work_abort_cleanup_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.cleanup("fail", "u", "active_abort", self.cd, self.offer, 5_000_000, 4_000_000)
        self.assertEqual(self.state(), (5_000_000, (2, "start", "镇妖"), 1, 1))


if __name__ == "__main__":
    unittest.main()
