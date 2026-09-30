from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ....infrastructure.database import DatabaseUnitOfWork
from ..impart_stone_batch_repository import AdminImpartStoneBatchSqlRepository
from ..migrations import (
    apply_admin_impart_stone_batch,
    apply_admin_impart_stone_operations,
    apply_admin_stone_adjustment,
)


class AdminImpartStoneBatchRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.impart = root / "impart.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT)")
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id) VALUES(?)",
                (("a",), ("b",), ("c",)),
            )
            apply_admin_stone_adjustment(uow)
            apply_admin_impart_stone_operations(uow)
            apply_admin_impart_stone_batch(uow)
        with DatabaseUnitOfWork(self.impart) as uow:
            uow.execute("CREATE TABLE xiuxian_impart(user_id TEXT,stone_num INTEGER)")
            uow.executemany(
                "INSERT INTO xiuxian_impart(user_id,stone_num) VALUES(?,?)",
                (("a", 5), ("b", 1)),
            )
        self.repository = AdminImpartStoneBatchSqlRepository(self.game, self.impart)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def stone(self, user_id: str) -> int | None:
        with DatabaseUnitOfWork(self.impart, read_only=True) as uow:
            row = uow.query_one(
                "SELECT stone_num FROM xiuxian_impart WHERE user_id=?", (user_id,)
            )
        return int(row["stone_num"]) if row is not None else None

    def adjust(self, operation_id: str = "batch", delta: int = 2, **kwargs):
        return self.repository.adjust(
            operation_id, "admin", delta, **kwargs
        )

    def test_roster_is_frozen_in_database_and_processed_in_chunks(self) -> None:
        first = self.adjust("frozen", chunk_size=1)
        self.assertEqual((first.status, first.total, first.completed), ("applied", 3, 1))
        self.assertEqual(self.repository.find_running("admin", 2), "frozen")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("INSERT INTO user_xiuxian(user_id) VALUES('new-user')")

        resumed = self.adjust("frozen", delta=2, chunk_size=10)
        duplicate = self.adjust("frozen", delta=2)

        self.assertEqual((resumed.completed, resumed.total), (3, 3))
        self.assertEqual((resumed.applied_delta, resumed.affected_users, resumed.skipped_users), (6, 3, 0))
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual((self.stone("a"), self.stone("b"), self.stone("c")), (7, 3, 2))
        self.assertIsNone(self.stone("new-user"))

    def test_concurrent_matching_request_reuses_running_operation(self) -> None:
        first = self.adjust("first", chunk_size=1)
        other = self.adjust("second", chunk_size=1)

        self.assertEqual((first.status, first.completed), ("applied", 1))
        self.assertEqual((other.status, other.total, other.completed), ("in_progress", 3, 1))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_operations"
                )["total"],
                1,
            )

    def test_requested_chunk_size_cannot_exceed_repository_limit(self) -> None:
        with patch.object(AdminImpartStoneBatchSqlRepository, "max_chunk_size", 1):
            result = self.adjust("bounded", chunk_size=10000)

        self.assertEqual((result.total, result.completed), (3, 1))

    def test_low_disk_does_not_create_operation_or_targets(self) -> None:
        with patch(
            "nonebot_plugin_xiuxian_2.features.admin_asset.impart_stone_batch_repository.shutil.disk_usage",
            return_value=SimpleNamespace(free=0),
        ):
            result = self.adjust("low-space")

        self.assertEqual(result.status, "insufficient_space")
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_operations"
                )["total"],
                0,
            )

    def test_missing_impart_schema_fails_before_creating_a_batch(self) -> None:
        with DatabaseUnitOfWork(self.impart) as uow:
            uow.execute("DROP TABLE xiuxian_impart")

        result = self.adjust("missing-impart")

        self.assertEqual(result.status, "not_ready")
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_operations"
                )["total"],
                0,
            )

    def test_legacy_running_payload_and_progress_are_imported_on_resume(self) -> None:
        request = {"operator_id": "admin", "requested_delta": 2}
        payload = json.dumps(
            {"request": request, "users": ["a", "b"]},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT INTO admin_impart_stone_batch_operations"
                "(operation_id,payload,total,status) VALUES('legacy',?,2,'running')",
                (payload,),
            )
            uow.execute(
                "INSERT INTO admin_impart_stone_batch_progress"
                "(operation_id,user_id,status,applied_delta,result_json) "
                "VALUES('legacy','a','adjusted',2,'{}')"
            )
        with DatabaseUnitOfWork(self.impart) as uow:
            uow.execute("UPDATE xiuxian_impart SET stone_num=7 WHERE user_id='a'")

        result = self.adjust("legacy", chunk_size=10)

        self.assertEqual((result.status, result.total, result.completed), ("applied", 2, 2))
        self.assertEqual((result.applied_delta, result.affected_users, result.skipped_users), (4, 2, 0))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            targets = uow.query_all(
                "SELECT user_id,status FROM admin_impart_stone_batch_targets "
                "WHERE operation_id='legacy' ORDER BY user_id"
            )
        self.assertEqual([(row["user_id"], row["status"]) for row in targets], [
            ("a", "adjusted"), ("b", "adjusted")
        ])

    def test_completed_legacy_batch_keeps_saved_summary(self) -> None:
        payload = json.dumps(
            {"request": {"operator_id": "admin", "requested_delta": 2}, "users": ["a", "b"]},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT INTO admin_impart_stone_batch_operations"
                "(operation_id,payload,total,status) VALUES('legacy-done',?,2,'completed')",
                (payload,),
            )
            uow.executemany(
                "INSERT INTO admin_impart_stone_batch_progress"
                "(operation_id,user_id,status,applied_delta,result_json) VALUES('legacy-done',?,?,?,'{}')",
                (("a", "adjusted", 2), ("b", "invalid_state", 0)),
            )

        result = self.adjust("legacy-done")

        self.assertEqual((result.status, result.total, result.completed), ("duplicate", 2, 2))
        self.assertEqual((result.applied_delta, result.affected_users, result.skipped_users), (2, 1, 1))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_targets "
                    "WHERE operation_id='legacy-done'"
                )["total"],
                0,
            )

    def test_legacy_resume_low_disk_preserves_saved_progress(self) -> None:
        payload = json.dumps(
            {"request": {"operator_id": "admin", "requested_delta": 2}, "users": ["a", "b"]},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT INTO admin_impart_stone_batch_operations"
                "(operation_id,payload,total,status) VALUES('legacy-low-space',?,2,'running')",
                (payload,),
            )
            uow.execute(
                "INSERT INTO admin_impart_stone_batch_progress"
                "(operation_id,user_id,status,applied_delta,result_json) "
                "VALUES('legacy-low-space','a','adjusted',2,'{}')"
            )

        with patch(
            "nonebot_plugin_xiuxian_2.features.admin_asset.impart_stone_batch_repository.shutil.disk_usage",
            return_value=SimpleNamespace(free=0),
        ):
            result = self.adjust("legacy-low-space")

        self.assertEqual((result.status, result.total, result.completed), ("insufficient_space", 2, 1))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_targets "
                    "WHERE operation_id='legacy-low-space'"
                )["total"],
                0,
            )
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_progress "
                    "WHERE operation_id='legacy-low-space'"
                )["total"],
                1,
            )

    def test_oversized_legacy_payload_is_not_expanded_in_memory(self) -> None:
        payload = json.dumps(
            {"request": {"operator_id": "admin", "requested_delta": 2}, "users": ["a", "b"]},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT INTO admin_impart_stone_batch_operations"
                "(operation_id,payload,total,status) VALUES('legacy-large',?,2,'running')",
                (payload,),
            )
        with patch.object(AdminImpartStoneBatchSqlRepository, "max_legacy_payload_chars", 10):
            result = self.adjust("legacy-large")

        self.assertEqual((result.status, result.total), ("legacy_payload_too_large", 2))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_targets "
                    "WHERE operation_id='legacy-large'"
                )["total"],
                0,
            )

    def test_child_receipt_replays_when_batch_progress_write_fails(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TRIGGER reject_impart_batch_progress BEFORE INSERT ON "
                "admin_impart_stone_batch_progress BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "failed"):
            self.adjust("child-replay", chunk_size=1)
        self.assertEqual(self.stone("a"), 7)
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TRIGGER reject_impart_batch_progress")

        resumed = self.adjust("child-replay", chunk_size=1)

        self.assertEqual((resumed.completed, resumed.applied_delta), (1, 2))
        self.assertEqual(self.stone("a"), 7)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS total FROM economy_log "
                    "WHERE action='admin_impart_stone_add'"
                )["total"],
                1,
            )

    def test_migration_is_idempotent_and_preserves_legacy_tables(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            apply_admin_impart_stone_batch(uow)
            apply_admin_impart_stone_batch(uow)
            columns = {
                row["name"]
                for row in uow.query_all(
                    'PRAGMA table_info("admin_impart_stone_batch_progress")'
                )
            }
        self.assertTrue({"operation_id", "user_id", "status", "applied_delta", "result_json"}.issubset(columns))


if __name__ == "__main__":
    unittest.main()
