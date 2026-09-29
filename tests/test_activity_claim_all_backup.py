from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import apply_activity_claim_all
from nonebot_plugin_xiuxian_2.infrastructure.database import BackupService, DatabaseCatalog, DatabaseUnitOfWork


class ActivityClaimAllBackupTests(unittest.TestCase):
    @staticmethod
    def _catalog(root: Path) -> DatabaseCatalog:
        return DatabaseCatalog.from_paths({key: root / f"{key}.db" for key in DatabaseCatalog.KEYS})

    def test_backup_includes_legacy_activity_wal_and_restores_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "activity" / "activity.db"
            with DatabaseUnitOfWork(legacy) as uow:
                apply_activity_claim_all(uow)
            writer = sqlite3.connect(legacy)
            try:
                writer.execute("PRAGMA journal_mode=WAL")
                writer.execute(
                    "INSERT INTO activity_claim_all_operations(operation_id,user_id,status) VALUES(?,?,?)",
                    ("old-claim", "u1", "pending"),
                )
                writer.commit()
                backup_service = BackupService(self._catalog(root))
                backup = backup_service.create(root / "backups")
            finally:
                writer.close()

            manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual([item["file"] for item in manifest["files"]], ["legacy_activity"])
            with DatabaseUnitOfWork(backup / "activity.db", read_only=True) as uow:
                self.assertEqual(uow.query_one(
                    "SELECT user_id FROM activity_claim_all_operations WHERE operation_id='old-claim'"
                )["user_id"], "u1")
            with DatabaseUnitOfWork(legacy) as uow:
                uow.execute("UPDATE activity_claim_all_operations SET user_id='u2' WHERE operation_id='old-claim'")
            self.assertEqual(backup_service.restore(backup, dry_run=True)["restored"], ["legacy_activity"])
            self.assertEqual(backup_service.restore(backup)["restored"], ["legacy_activity"])
            with DatabaseUnitOfWork(legacy, read_only=True) as uow:
                self.assertEqual(uow.query_one(
                    "SELECT user_id FROM activity_claim_all_operations WHERE operation_id='old-claim'"
                )["user_id"], "u1")

    def test_backup_does_not_create_missing_legacy_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with DatabaseUnitOfWork(root / "game_db.db"):
                pass
            backup = BackupService(self._catalog(root)).create(root / "backups")
            manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
            self.assertNotIn("legacy_activity", [item.get("file") for item in manifest["files"]])
            self.assertFalse((root / "activity" / "activity.db").exists())


if __name__ == "__main__":
    unittest.main()
