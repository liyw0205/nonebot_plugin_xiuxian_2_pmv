from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import (
    apply_activity_task_claim,
    apply_activity_task_claim_legacy_receipts,
)
from nonebot_plugin_xiuxian_2.features.activity_reward.task_claim_application import ActivityTaskClaimApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema

ROOT = Path(__file__).resolve().parents[1]


class ActivityTaskClaimMigrationTests(unittest.TestCase):
    @staticmethod
    def _seed_legacy_receipt(activity: Path) -> None:
        with DatabaseUnitOfWork(activity) as uow:
            uow.execute(
                "CREATE TABLE activity_task_claim_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TEXT NOT NULL)"
            )
            payload = json.dumps([
                "u1", "festival-1", [["daily-1", "daily", "2026-09-30", 2, "灵石x50", "签到两次"]],
                50, [], 100,
            ], ensure_ascii=True, separators=(",", ":"))
            result = json.dumps([["签到两次", "灵石x50"]], ensure_ascii=True, separators=(",", ":"))
            uow.execute(
                "INSERT INTO activity_task_claim_operations VALUES(?,?,?,?)",
                ("legacy-task-claim", payload, result, "2026-09-30T00:00:00"),
            )

    def test_startup_import_preserves_replay_and_reserves_claimed_period(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            activity = root / "activity" / "activity.db"
            self._seed_legacy_receipt(activity)
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
                apply_activity_task_claim(uow)
                apply_activity_task_claim_legacy_receipts(uow)
                apply_activity_task_claim_legacy_receipts(uow)
                self.assertEqual(
                    1,
                    uow.query_one("SELECT count(*) AS n FROM activity_task_reward_claim_operations")["n"],
                )
                self.assertEqual(
                    1,
                    uow.query_one("SELECT count(*) AS n FROM activity_task_reward_claim_reservations")["n"],
                )
            result = ActivityTaskClaimApplication(game, activity).get_result("legacy-task-claim", "u1")
            self.assertEqual((result.status, result.rewards), ("duplicate", (("签到两次", "灵石x50"),)))

    def test_conflicting_historical_receipt_aborts_backfill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            activity = root / "activity" / "activity.db"
            self._seed_legacy_receipt(activity)
            with DatabaseUnitOfWork(game) as uow:
                apply_activity_task_claim(uow)
                uow.execute(
                    "INSERT INTO activity_task_reward_claim_operations"
                    "(operation_id,payload,request_json,result_json,result_status,status) "
                    "VALUES('legacy-task-claim','conflict','{}','[]','applied','applied')"
                )
            with self.assertRaisesRegex(RuntimeError, "receipt conflict"):
                with DatabaseUnitOfWork(game) as uow:
                    apply_activity_task_claim_legacy_receipts(uow)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertEqual(
                    0,
                    uow.query_one("SELECT count(*) AS n FROM activity_task_reward_claim_reservations")["n"],
                )

    def test_missing_startup_schema_is_not_created_by_request(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            activity = root / "activity" / "activity.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
            application = ActivityTaskClaimApplication(game, activity)
            with self.assertRaisesRegex(RuntimeError, "activity_reward.004 schema_missing"):
                application.claim("op", "u1", "festival", [
                    ("task", "daily", "2026-09-30", 1, "灵石x1", ({"type": "stone", "quantity": 1},), "任务")
                ], 10)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertIsNone(uow.query_one(
                    "SELECT name FROM sqlite_master WHERE name='activity_task_reward_claim_operations'"
                ))
            self.assertFalse(activity.exists())

    def test_backup_restore_imports_historical_task_receipt_before_serving(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "xiuxian"
            self._seed_legacy_receipt(data_dir / "activity" / "activity.db")
            result = subprocess.run(
                [sys.executable, "scripts/recovery_smoke.py", "--data-dir", str(data_dir)],
                cwd=ROOT,
                env={
                    **os.environ,
                    "XIUXIAN_DATA_DIR": str(data_dir),
                    "PYTHONDONTWRITEBYTECODE": "1",
                    "XIUXIAN_AUTO_DOWNLOAD_RESOURCES": "false",
                    "XIUXIAN_WEB_STATUS": "false",
                },
                text=True,
                capture_output=True,
                check=True,
            )
            report = json.loads(result.stdout)
            self.assertIn("legacy_activity", report["restore"])
            self.assertIn("activity_reward.005", report["applied_migrations_by_database"]["game_db"])
            with DatabaseUnitOfWork(data_dir / "xiuxian.db", read_only=True) as uow:
                self.assertEqual(
                    1,
                    uow.query_one(
                        "SELECT count(*) AS n FROM activity_task_reward_claim_operations"
                    )["n"],
                )


if __name__ == "__main__":
    unittest.main()
