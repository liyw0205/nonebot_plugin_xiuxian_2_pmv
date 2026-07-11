from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.membership_service import SectMembershipService
from tests.test_db_backend import db_backend


class SectTaskSettlementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, sect_id INTEGER, stone INTEGER, hp INTEGER, exp INTEGER, sect_task INTEGER, sect_contribution INTEGER)")
            conn.execute("CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_used_stone INTEGER, sect_scale INTEGER, sect_materials INTEGER)")
            conn.execute("CREATE TABLE sect_task_state (user_id TEXT, sect_id INTEGER, task_key TEXT, task_data TEXT, period TEXT, status TEXT, progress INTEGER, target INTEGER, accepted_at TEXT, updated_at TEXT, completed_at TEXT, PRIMARY KEY (user_id, period))")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s, %s, %s)", ("user", 1, 1000, 500, 2000, 0, 20))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s, %s)", (1, 50, 100, 200))
            conn.execute("INSERT INTO sect_task_state VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", ("user", 1, "试炼", "{}", "2026-07-11", "accepted", 0, 1, "now", "now", None))
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute("SELECT stone, hp, exp, sect_task, sect_contribution FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()
            sect = conn.execute("SELECT sect_used_stone, sect_scale, sect_materials FROM sects WHERE sect_id=1").fetchone()
            task = conn.execute("SELECT status, progress, completed_at FROM sect_task_state WHERE user_id=%s", ("user",)).fetchone()
            operations = conn.execute("SELECT COUNT(*) FROM sect_task_settlement_operations").fetchone()[0] if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s", ("sect_task_settlement_operations",)).fetchone() else 0
        return tuple(map(int, user)), tuple(map(int, sect)), (str(task[0]), int(task[1]), task[2] is not None), int(operations)

    def test_hp_task_settles_assets_and_state_atomically(self) -> None:
        result = self.service.settle_task("task-hp", "user", 1, "2026-07-11", "hp", 100, 300, 25)
        self.assertEqual(result.status, "settled")
        self.assertEqual(self.state(), ((1000, 400, 2300, 1, 45), (75, 125, 450), ("completed", 1, True), 1))

    def test_stone_task_uses_same_boundary(self) -> None:
        result = self.service.settle_task("task-stone", "user", 1, "2026-07-11", "stone", 150, 300, 25)
        self.assertEqual(result.status, "settled")
        self.assertEqual(self.state(), ((850, 500, 2300, 1, 45), (75, 125, 450), ("completed", 1, True), 1))

    def test_duplicate_does_not_settle_twice(self) -> None:
        self.service.settle_task("task-repeat", "user", 1, "2026-07-11", "stone", 150, 300, 25)
        result = self.service.settle_task("task-repeat", "user", 1, "2026-07-11", "stone", 999, 999, 999)
        self.assertEqual(result.status, "duplicate")
        self.assertEqual((result.cost, result.exp_reward, result.sect_reward), (150, 300, 25))
        self.assertEqual(self.state(), ((850, 500, 2300, 1, 45), (75, 125, 450), ("completed", 1, True), 1))

    def test_insufficient_asset_keeps_task_retryable(self) -> None:
        result = self.service.settle_task("task-poor", "user", 1, "2026-07-11", "stone", 1001, 300, 25)
        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.state(), ((1000, 500, 2000, 0, 20), (50, 100, 200), ("accepted", 0, False), 0))

    def test_database_failure_rolls_back_assets_and_task(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_task_settlement_operations(conn)
            conn.execute("CREATE TRIGGER fail_task_settlement BEFORE INSERT ON sect_task_settlement_operations BEGIN SELECT RAISE(ABORT, 'settlement failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.settle_task("task-fail", "user", 1, "2026-07-11", "hp", 100, 300, 25)
        self.assertEqual(self.state(), ((1000, 500, 2000, 0, 20), (50, 100, 200), ("accepted", 0, False), 0))


if __name__ == "__main__":
    unittest.main()
