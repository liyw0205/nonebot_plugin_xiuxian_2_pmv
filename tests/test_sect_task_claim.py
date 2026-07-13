from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.membership_service import SectMembershipService
from tests.test_db_backend import db_backend


class SectTaskClaimTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_task INTEGER)")
            conn.execute("CREATE TABLE sects (sect_id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE sect_task_state (user_id TEXT, sect_id INTEGER, task_key TEXT, task_data TEXT, period TEXT, status TEXT, progress INTEGER, target INTEGER, accepted_at TEXT, updated_at TEXT, completed_at TEXT, PRIMARY KEY (user_id, period))")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("user", 1, 0))
            conn.execute("INSERT INTO sects VALUES (%s)", (1,))
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            task = conn.execute("SELECT task_key, task_data, status FROM sect_task_state WHERE user_id=%s", ("user",)).fetchone()
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s", ("sect_task_claim_operations",)).fetchone()
            operations = conn.execute("SELECT COUNT(*) FROM sect_task_claim_operations").fetchone()[0] if exists else 0
        return tuple(task) if task else None, int(operations)

    def test_claim_and_duplicate_preserve_original_snapshot(self) -> None:
        first = self.service.claim_task("claim-1", "user", 1, "2026-07-13", "试炼", {"type": 1}, 3)
        duplicate = self.service.claim_task("claim-1", "user", 1, "2026-07-13", "采购", {"type": 2}, 3)
        self.assertEqual(first.status, "claimed")
        self.assertEqual((duplicate.status, duplicate.task_key, duplicate.task_data), ("duplicate", "试炼", {"type": 1}))
        self.assertEqual(self.state(), (("试炼", '{"type": 1}', "accepted"), 1))

    def test_business_and_snapshot_rejections_do_not_write(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET sect_task=3 WHERE user_id=%s", ("user",))
        self.assertEqual(self.service.claim_task("limit", "user", 1, "2026-07-13", "试炼", {}, 3).status, "daily_limit")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET sect_task=0, sect_id=2 WHERE user_id=%s", ("user",))
        self.assertEqual(self.service.claim_task("changed", "user", 1, "2026-07-13", "试炼", {}, 3).status, "sect_changed")
        self.assertEqual(self.state(), (None, 0))

    def test_failure_rolls_back_task_and_operation(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_task_claim_operations(conn)
            conn.execute("CREATE TRIGGER fail_claim BEFORE INSERT ON sect_task_claim_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.claim_task("fail", "user", 1, "2026-07-13", "试炼", {}, 3)
        self.assertEqual(self.state(), (None, 0))


if __name__ == "__main__":
    unittest.main()
