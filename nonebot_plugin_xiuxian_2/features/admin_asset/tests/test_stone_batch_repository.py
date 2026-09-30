import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_admin_stone_batch
from ..stone_batch_repository import AdminStoneBatchSqlRepository


class AdminStoneBatchRepositoryTests(unittest.TestCase):
    def _database(self, directory: str, users=(('a', 2), ('b', 10), ('c', None))) -> Path:
        database = Path(directory) / "game.db"
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id,stone) VALUES(?,?)", users
            )
            apply_admin_stone_batch(uow)
        return database

    def test_chunks_freeze_targets_and_replay_without_reapplying(self):
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            repository = AdminStoneBatchSqlRepository(database)
            first = repository.adjust("batch-1", "operator", -10, chunk_size=1)
            self.assertEqual((first.status, first.completed, first.total), ("applied", 1, 3))
            self.assertEqual(repository.find_running("operator", -10), "batch-1")

            with DatabaseUnitOfWork(database) as uow:
                uow.execute("INSERT INTO user_xiuxian(user_id,stone) VALUES('0-new',100)")

            second = repository.adjust("batch-1", "operator", -10, chunk_size=1)
            final = repository.adjust("batch-1", "operator", -10, chunk_size=1)
            duplicate = repository.adjust("batch-1", "operator", -10, chunk_size=1)
            self.assertEqual((second.completed, final.completed, final.total), (2, 3, 3))
            self.assertEqual((final.applied_delta, final.affected_users, final.skipped_users), (-30, 3, 0))
            self.assertEqual(duplicate.status, "duplicate")
            self.assertIsNone(repository.find_running("operator", -10))

            with DatabaseUnitOfWork(database) as uow:
                balances = {
                    row["user_id"]: row["stone"]
                    for row in uow.query_all("SELECT user_id,stone FROM user_xiuxian")
                }
                progress = uow.query_all(
                    "SELECT user_id,status,previous_stone,final_stone,applied_delta "
                    "FROM admin_stone_batch_progress WHERE operation_id=? ORDER BY user_id",
                    ("batch-1",),
                )
            self.assertEqual(balances, {"a": -8, "b": 0, "c": -10, "0-new": 100})
            self.assertEqual([row["status"] for row in progress], ["applied"] * 3)
            self.assertEqual([row["applied_delta"] for row in progress], [-10, -10, -10])

    def test_concurrent_duplicate_request_with_new_operation_id_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            repository = AdminStoneBatchSqlRepository(database)
            self.assertIsNone(repository.find_running("operator", -10))
            start = threading.Barrier(3)

            def adjust(operation_id: str):
                start.wait()
                return repository.adjust(operation_id, "operator", -10, chunk_size=1)

            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(adjust, "batch-1")
                second = pool.submit(adjust, "batch-2")
                start.wait()
                results = (first.result(), second.result())

            self.assertEqual(sorted(result.status for result in results), ["applied", "in_progress"])
            self.assertEqual({(result.completed, result.total) for result in results}, {(1, 3)})
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS count FROM admin_stone_batch_operations")["count"],
                    1,
                )
                self.assertEqual(
                    uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='a'")["stone"],
                    -8,
                )

    def test_running_batch_unique_index_rejects_duplicate_active_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            repository = AdminStoneBatchSqlRepository(database)
            repository.adjust("batch-1", "operator", -10, chunk_size=1)

            with DatabaseUnitOfWork(database, immediate=True) as uow:
                with self.assertRaises(sqlite3.IntegrityError):
                    uow.execute(
                        "INSERT INTO admin_stone_batch_operations("
                        "operation_id,operator_id,requested_delta,payload,total,status) "
                        "VALUES('batch-2','operator',-10,'{}',3,'running')"
                    )

    def test_deleted_target_is_skipped_and_payload_conflict_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory, users=(("a", 10), ("b", 20)))
            repository = AdminStoneBatchSqlRepository(database)
            first = repository.adjust("batch-2", "operator", 5, chunk_size=1)
            self.assertEqual(first.completed, 1)
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("DELETE FROM user_xiuxian WHERE user_id='b'")

            conflict = repository.adjust("batch-2", "operator", 6, chunk_size=1)
            self.assertEqual(conflict.status, "operation_conflict")
            result = repository.adjust("batch-2", "operator", 5, chunk_size=1)
            self.assertEqual((result.completed, result.affected_users, result.skipped_users), (2, 1, 1))
            self.assertEqual(result.status, "applied")

    def test_late_sql_failure_rolls_back_chunk_and_can_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory, users=(("a", 10), ("b", 20)))
            repository = AdminStoneBatchSqlRepository(database)
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TRIGGER reject_second_user BEFORE UPDATE ON user_xiuxian "
                    "WHEN OLD.user_id='b' BEGIN SELECT RAISE(ABORT,'reject batch'); END"
                )

            with self.assertRaisesRegex(Exception, "reject batch"):
                repository.adjust("batch-3", "operator", 5, chunk_size=2)
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS count FROM admin_stone_batch_operations")["count"],
                    0,
                )
                self.assertEqual(
                    [row["stone"] for row in uow.query_all("SELECT stone FROM user_xiuxian ORDER BY user_id")],
                    [10, 20],
                )
                uow.execute("DROP TRIGGER reject_second_user")

            first = repository.adjust("batch-3", "operator", 5, chunk_size=1)
            resumed = repository.adjust("batch-3", "operator", 5, chunk_size=1)
            self.assertEqual((first.completed, resumed.completed, resumed.applied_delta), (1, 2, 10))

    def test_missing_migration_and_low_disk_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('a',10)")
            missing = AdminStoneBatchSqlRepository(database).adjust("missing", "operator", 1)
            self.assertEqual(missing.status, "not_ready")

            with DatabaseUnitOfWork(database) as uow:
                apply_admin_stone_batch(uow)
            repository = AdminStoneBatchSqlRepository(database)
            with patch(
                "nonebot_plugin_xiuxian_2.features.admin_asset.stone_batch_repository.shutil.disk_usage",
                return_value=SimpleNamespace(free=0),
            ):
                low_space = repository.adjust("low-space", "operator", 1)
            self.assertEqual(low_space.status, "insufficient_space")
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(
                    uow.query_one(
                        "SELECT COUNT(*) AS count FROM admin_stone_batch_operations"
                    )["count"],
                    0,
                )


if __name__ == "__main__":
    unittest.main()
