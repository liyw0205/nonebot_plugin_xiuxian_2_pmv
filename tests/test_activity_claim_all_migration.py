from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.activity_reward.claim_all_application import ActivityClaimAllApplication
from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import (
    apply_activity_claim_all,
    apply_activity_claim_all_legacy_receipts,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class ActivityClaimAllMigrationTests(unittest.TestCase):
    @staticmethod
    def _seed_legacy(database: Path) -> None:
        with DatabaseUnitOfWork(database) as uow:
            apply_activity_claim_all(uow)
            for operation_id, complete in (("legacy-done", True), ("legacy-pending", False)):
                result = json.dumps({
                    "ok": True, "text": "task reward",
                    "steps": [{"name": name, "ok": name == "tasks", "text": "task reward" if name == "tasks" else "no reward"}
                              for name in ActivityClaimAllApplication.step_names()],
                }) if complete else ""
                uow.execute(
                    "INSERT INTO activity_claim_all_operations(operation_id,user_id,status,result_json,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (operation_id, "u1", "completed" if complete else "pending", result, "t0", "t1"),
                )
                for ordinal, name in enumerate(ActivityClaimAllApplication.step_names()):
                    done = complete or ordinal == 0
                    uow.execute(
                        "INSERT INTO activity_claim_all_steps(operation_id,step_name,ordinal,status,attempts,ok,result_text,error_text,updated_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?)",
                        (operation_id, name, ordinal, "completed" if done else "pending", 1 if done else 0,
                         1 if ordinal == 0 else 0 if complete else None,
                         "task reward" if ordinal == 0 else "no reward" if complete else "", "", "t1"),
                    )

    def test_backfill_preserves_replay_and_unfinished_step_progress(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            legacy = Path(directory) / "activity" / "activity.db"
            self._seed_legacy(legacy)
            with DatabaseUnitOfWork(game) as uow:
                apply_activity_claim_all(uow)
                apply_activity_claim_all_legacy_receipts(uow)
                apply_activity_claim_all_legacy_receipts(uow)
                self.assertEqual(uow.query_one("SELECT count(*) AS n FROM activity_claim_all_steps")["n"], 8)

            calls = []
            runners = {
                name: (lambda child_id, step=name: (calls.append((step, child_id)), (False, "no reward"))[1])
                for name in ActivityClaimAllApplication.step_names()
            }
            application = ActivityClaimAllApplication(game)
            replay = application.run("legacy-done", "u1", runners)
            resumed = application.run("legacy-pending", "u1", runners)
            self.assertEqual((replay.status, replay.text), ("duplicate", "task reward"))
            self.assertEqual((resumed.status, resumed.text), ("applied", "task reward"))
            self.assertEqual([name for name, _ in calls], ["pass", "boss_milestone", "boss_rank"])
            self.assertEqual([child for _, child in calls], [
                "legacy-pending:pass", "legacy-pending:boss-milestone", "legacy-pending:boss-rank",
            ])
            with DatabaseUnitOfWork(legacy, read_only=True) as uow:
                self.assertEqual(uow.query_one(
                    "SELECT status FROM activity_claim_all_operations WHERE operation_id='legacy-pending'"
                )["status"], "pending")

    def test_missing_startup_schema_is_rejected_without_creating_tables(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            with self.assertRaisesRegex(RuntimeError, "activity_reward.002 schema_missing"):
                ActivityClaimAllApplication(game).run("op", "u1", {
                    name: lambda _: (False, "no reward") for name in ActivityClaimAllApplication.step_names()
                })
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertIsNone(uow.query_one(
                    "SELECT name FROM sqlite_master WHERE name='activity_claim_all_operations'"
                ))

    def test_conflicting_existing_receipt_aborts_backfill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            legacy = Path(directory) / "activity" / "activity.db"
            self._seed_legacy(legacy)
            with DatabaseUnitOfWork(game) as uow:
                apply_activity_claim_all(uow)
                uow.execute(
                    "INSERT INTO activity_claim_all_operations(operation_id,user_id,status) VALUES(?,?,?)",
                    ("legacy-pending", "someone-else", "pending"),
                )
            with self.assertRaisesRegex(RuntimeError, "receipt conflict"):
                with DatabaseUnitOfWork(game) as uow:
                    apply_activity_claim_all_legacy_receipts(uow)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertIsNone(uow.query_one(
                    "SELECT 1 FROM activity_claim_all_operations WHERE operation_id='legacy-done'"
                ))
                self.assertEqual(uow.query_one(
                    "SELECT user_id FROM activity_claim_all_operations WHERE operation_id='legacy-pending'"
                )["user_id"], "someone-else")

    def test_recovery_smoke_restores_legacy_source_before_backfill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "xiuxian"
            legacy = data_dir / "activity" / "activity.db"
            self._seed_legacy(legacy)
            result = subprocess.run(
                [sys.executable, "scripts/recovery_smoke.py", "--data-dir", str(data_dir)],
                cwd=Path(__file__).resolve().parents[1],
                env={**os.environ, "XIUXIAN_DATA_DIR": str(data_dir), "PYTHONDONTWRITEBYTECODE": "1",
                     "XIUXIAN_AUTO_DOWNLOAD_RESOURCES": "false", "XIUXIAN_WEB_STATUS": "false"},
                text=True, capture_output=True, check=True,
            )
            receipt = json.loads(result.stdout)
            self.assertIn("legacy_activity", receipt["restore_dry_run"])
            self.assertIn("legacy_activity", receipt["restore"])
            self.assertIn("activity_reward.003", receipt["applied_migrations_by_database"]["game_db"])
            self.assertNotIn("activity_reward.003", receipt["migrations_by_database"]["player_db"])
            self.assertTrue(receipt["reconcile"]["clean"])
            with DatabaseUnitOfWork(data_dir / "xiuxian.db", read_only=True) as uow:
                self.assertEqual(uow.query_one("SELECT count(*) AS n FROM activity_claim_all_operations")["n"], 2)


if __name__ == "__main__":
    unittest.main()
