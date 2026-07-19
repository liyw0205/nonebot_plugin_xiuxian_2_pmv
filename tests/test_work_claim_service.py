from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.transaction_service import WorkClaimService
from tests.test_db_backend import db_backend


class WorkClaimServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("u", 3))
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("INSERT INTO user_cd VALUES(%s,%s,%s,%s)", ("u", 0, "0", None))
        self.service = WorkClaimService(self.database)
        self.offer = {
            "tasks": {"采药": {"time": 5}, "镇妖": {"time": 8}},
            "status": 1,
            "refresh_time": "2026-07-13 10:00:00",
            "user_level": "筑基",
        }

    def tearDown(self):
        self.temp.cleanup()

    def claim(self, operation="claim", **changes):
        values = {"count": 3, "offer": self.offer, "index": 2, "started": "2026-07-13 10:05:00"}
        values.update(changes)
        return self.service.claim(
            operation, "u", values["count"], values["offer"], values["index"], values["started"]
        )

    def state(self):
        with db_backend.connection(self.database) as conn:
            count = conn.execute("SELECT work_num FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0]
            work = conn.execute(
                "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=%s", ("u",)
            ).fetchone()
            try:
                snapshot = conn.execute(
                    "SELECT snapshot FROM work_active_snapshots WHERE user_id=%s", ("u",)
                ).fetchone()
            except db_backend.OperationalError:
                snapshot = None
        return int(count), tuple(work), snapshot

    def test_success_does_not_consume_refresh_count(self):
        """接取不扣刷新次数 work_num。"""
        result = self.claim()
        self.assertEqual((result.status, result.task_name, result.remaining_count), ("applied", "镇妖", 3))
        count, work, snapshot = self.state()
        self.assertEqual((count, work), (3, (2, "2026-07-13 10:05:00", "镇妖")))
        self.assertIn("selected_task", snapshot[0])

    def test_claim_with_zero_refresh_count_still_works(self):
        """刷新次数为 0 时仍可接取已刷出的悬赏（修复最后一次接不了）。"""
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET work_num=0 WHERE user_id=%s", ("u",))
        result = self.claim("zero", count=0)
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.remaining_count, 0)
        count, work, _ = self.state()
        self.assertEqual(count, 0)
        self.assertEqual(work[0], 2)

    def test_duplicate_conflict_and_stale_count(self):
        self.assertEqual(self.claim("same").status, "applied")
        # mutable started_at / count must not break same-op replay
        self.assertEqual(self.claim("same", started="later", count=9).status, "duplicate")
        self.assertEqual(self.claim("same", index=1).status, "operation_conflict")
        self.assertEqual(self.claim("stale", count=2).status, "state_changed")
        self.assertEqual(self.state()[0], 3)
        prior = self.service.get_result("same")
        self.assertIsNotNone(prior)
        self.assertEqual(prior.status, "duplicate")
        self.assertEqual(prior.task_name, "镇妖")

    def test_operation_failure_rolls_back_everything(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE work_claim_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,task_name TEXT,"
                "started_at TEXT,remaining_count INTEGER,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_claim BEFORE INSERT ON work_claim_operations "
                "BEGIN SELECT RAISE(ABORT,'fail'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.claim("rollback")
        self.assertEqual(self.state()[:2], (3, (0, "0", None)))


if __name__ == "__main__":
    unittest.main()
