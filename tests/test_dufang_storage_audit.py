from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.audit_dufang_storage import audit_data_dir
from nonebot_plugin_xiuxian_2.infrastructure.database.backup_capacity import backup_reserve_bytes


class DufangStorageAuditTests(unittest.TestCase):
    def test_reports_sizes_table_estimates_and_pending_recovery_without_writes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dufang-storage-audit-") as temp_dir:
            root = Path(temp_dir) / "data" / "xiuxian"
            root.mkdir(parents=True)
            backup_dir = Path(temp_dir) / "backup-target"
            backup_dir.mkdir()
            previous_backup = backup_dir / "previous-backup.bin"
            previous_backup.write_bytes(b"existing-backup")
            game_db = root / "xiuxian.db"
            player_db = root / "player.db"
            with sqlite3.connect(game_db) as connection:
                connection.executescript(
                    "CREATE TABLE dufang_bets(bet_id TEXT,status TEXT);"
                    "CREATE TABLE dufang_player_outbox(event_id TEXT,status TEXT,payload_json TEXT);"
                    "CREATE TABLE dufang_share_operations(operation_id TEXT,completed INTEGER,total INTEGER);"
                    "CREATE TABLE operation_ledger(operation_id TEXT,status TEXT,result_json TEXT);"
                    "INSERT INTO dufang_bets VALUES('pending-bet','pending');"
                    "INSERT INTO dufang_bets VALUES('settled-bet','win');"
                    "INSERT INTO dufang_player_outbox VALUES('event-1','pending','{\"user_id\":\"u1\"}');"
                    "INSERT INTO dufang_share_operations VALUES('share-1',1,2);"
                    "INSERT INTO operation_ledger VALUES('op-1','needs_reconcile','{}');"
                )
            with sqlite3.connect(player_db) as connection:
                connection.executescript(
                    "CREATE TABLE dufang_player_operation_receipts(operation_id TEXT,event_type TEXT,payload_json TEXT);"
                    "INSERT INTO dufang_player_operation_receipts VALUES('op-1','bet','{}');"
                )
            config = root / "config.json"
            config.write_bytes(b"{}")

            before = (game_db.read_bytes(), player_db.read_bytes())
            report = audit_data_dir(root, backup_dir=backup_dir, reserve_bytes=1)

            files = {item["key"]: item for item in report["files"]}
            self.assertTrue(report["read_only"])
            self.assertEqual(files["game_db"]["tables"]["dufang_bets"]["row_count"], 2)
            self.assertGreater(
                files["game_db"]["tables"]["dufang_player_outbox"]["text_bytes_estimate"], 0
            )
            self.assertEqual(report["pending_work"]["dufang_bets_pending"], 1)
            self.assertEqual(report["pending_work"]["dufang_player_outbox_pending"], 1)
            self.assertEqual(report["pending_work"]["dufang_share_operations_incomplete"], 1)
            self.assertEqual(report["pending_work"]["ledger_nonterminal"], 1)
            self.assertEqual(files["player_db"]["tables"]["dufang_player_operation_receipts"]["row_count"], 1)
            self.assertGreater(report["backup_capacity"]["minimum_sqlite_backup_estimate_bytes"], 0)
            payload_estimate = report["backup_capacity"]["minimum_sqlite_backup_estimate_bytes"] + 2
            self.assertEqual(report["backup_capacity"]["extra_file_estimate_bytes"], 2)
            self.assertEqual(
                report["backup_capacity"]["reserve_bytes"],
                backup_reserve_bytes(payload_estimate, additional_bytes=1),
            )
            self.assertEqual(
                report["backup_capacity"]["existing_backup_inventory"]["bytes"], len(b"existing-backup")
            )
            self.assertEqual(report["backup_capacity"]["existing_backup_inventory"]["file_count"], 1)
            self.assertTrue(report["backup_capacity"]["existing_backup_inventory"]["scan_complete"])
            self.assertFalse(report["archive_ready"])
            self.assertEqual((game_db.read_bytes(), player_db.read_bytes()), before)
            self.assertFalse(Path(f"{game_db}-wal").exists())
            self.assertFalse(Path(f"{player_db}-wal").exists())
            self.assertEqual(previous_backup.read_bytes(), b"existing-backup")

    def test_missing_data_directory_is_not_created(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dufang-storage-missing-") as temp_dir:
            missing = Path(temp_dir) / "not-created"
            report = audit_data_dir(missing)
            self.assertFalse(missing.exists())
            self.assertTrue(all(not item["exists"] for item in report["files"]))
            self.assertEqual(report["backup_capacity"]["minimum_sqlite_backup_estimate_bytes"], 0)
            self.assertFalse(report["backup_capacity"]["estimate_complete"])
            self.assertFalse(report["backup_capacity"]["capacity_estimate_sufficient"])

    def test_unreadable_database_fails_closed_for_backup_capacity(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dufang-storage-corrupt-") as temp_dir:
            root = Path(temp_dir) / "xiuxian"
            root.mkdir()
            (root / "xiuxian.db").write_bytes(b"not a sqlite database")
            with sqlite3.connect(root / "player.db"):
                pass

            report = audit_data_dir(root)

            files = {item["key"]: item for item in report["files"]}
            self.assertEqual(files["game_db"]["status"], "unreadable")
            self.assertFalse(report["backup_capacity"]["estimate_complete"])
            self.assertFalse(report["backup_capacity"]["capacity_estimate_sufficient"])


if __name__ == "__main__":
    unittest.main()
