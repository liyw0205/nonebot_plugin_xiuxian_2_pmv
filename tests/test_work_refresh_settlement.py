from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.refresh_settlement_service import (
    WorkRefreshSettlementService,
)
from tests.test_db_backend import db_backend


class WorkRefreshSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("u", 3))
            conn.execute("INSERT INTO user_cd VALUES(%s,%s,%s,%s)", ("u", 0, "0", None))
        self.service = WorkRefreshSettlementService(self.db)
        self.cd = {"type": 0, "create_time": "0", "scheduled_time": None}
        self.offer = {
            "tasks": {"采药": {"rate": 80, "award": 10, "time": 5, "item_id": 0}},
            "status": 1,
            "refresh_time": "2026-07-14 10:00:00",
            "user_level": "筑基",
        }

    def tearDown(self):
        self.temp.cleanup()

    def refresh(self, operation="refresh", **changes):
        values = {"count": 3, "cd": self.cd, "old": None, "offer": self.offer, "force": False}
        values.update(changes)
        return self.service.refresh(
            operation, "u", values["count"], values["cd"], values["old"],
            values["offer"], values["force"],
        )

    def state(self):
        with db_backend.connection(self.db) as conn:
            count = conn.execute("SELECT work_num FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0]
            try:
                offer = conn.execute("SELECT snapshot FROM work_offer_snapshots WHERE user_id=%s", ("u",)).fetchone()
            except db_backend.OperationalError:
                offer = None
        return int(count), offer[0] if offer else None

    def test_fixed_offer_and_count_are_committed_atomically(self):
        result = self.refresh()
        self.assertEqual((result.status, result.remaining_count), ("applied", 2))
        self.assertEqual(self.state()[0], 2)
        self.assertIn("采药", __import__("json").loads(self.state()[1])["tasks"])

    def test_duplicate_conflict_and_stale_count(self):
        self.assertEqual(self.refresh("same").status, "applied")
        # mutable offer blob must not break same-op replay (identity is user+force)
        changed = dict(self.offer)
        changed["refresh_time"] = "later"
        self.assertEqual(self.refresh("same", offer=changed).status, "duplicate")
        # force flag is request identity
        self.assertEqual(self.refresh("same", force=True, offer=changed).status, "operation_conflict")
        self.assertEqual(self.refresh("stale", count=3, old=self.offer, force=True).status, "state_changed")
        prior = self.service.get_result("same")
        self.assertIsNotNone(prior)
        self.assertEqual(prior.status, "duplicate")
        self.assertEqual(prior.remaining_count, 2)

    def test_force_refresh_replaces_expected_legacy_offer(self):
        old = dict(self.offer)
        old["refresh_time"] = "old"
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "CREATE TABLE work_offer_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT,updated_at TEXT)"
            )
            import json
            conn.execute("INSERT INTO work_offer_snapshots VALUES(%s,%s,%s)", ("u", json.dumps(old), "old"))
        result = self.refresh("force", old=old, force=True)
        self.assertEqual(result.status, "applied")
        self.assertNotIn('"refresh_time": "old"', self.state()[1])

    def test_operation_failure_rolls_back_offer_and_count(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "CREATE TABLE work_refresh_operations(operation_id TEXT PRIMARY KEY,payload TEXT,"
                "remaining_count INTEGER,offer_snapshot TEXT,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_refresh BEFORE INSERT ON work_refresh_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.refresh("fail")
        self.assertEqual(self.state(), (3, None))


if __name__ == "__main__":
    unittest.main()
