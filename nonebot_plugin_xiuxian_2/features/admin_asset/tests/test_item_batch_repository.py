from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ....infrastructure.database import DatabaseUnitOfWork
from ..item_batch_repository import AdminItemBatchSqlRepository
from ..migrations import (
    apply_admin_item_batch,
    apply_admin_item_destroy,
    apply_admin_item_grant,
    apply_admin_stone_adjustment,
)


class AdminItemBatchRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT)")
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id) VALUES(?)",
                (("a",), ("b",), ("c",)),
            )
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER DEFAULT 0,"
                "PRIMARY KEY(user_id,goods_id))"
            )
            apply_admin_stone_adjustment(uow)
            apply_admin_item_grant(uow)
            apply_admin_item_destroy(uow)
            apply_admin_item_batch(uow)
        self.repository = AdminItemBatchSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def adjust(self, operation_id="batch", action="grant", **kwargs):
        values = {
            "operator_id": "admin",
            "item_id": 7,
            "item_name": "pill",
            "item_type": "medicine",
            "quantity": 2,
            "max_goods_num": 20 if action == "grant" else 0,
        }
        values.update(kwargs)
        return self.repository.adjust(action, operation_id, **values)

    def rows(self, query: str, params=()):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_all(query, params)

    def test_grant_roster_is_frozen_and_replay_is_idempotent(self) -> None:
        first = self.adjust("frozen", chunk_size=1)
        self.assertEqual((first.status, first.total, first.completed), ("applied", 3, 1))
        self.assertEqual(
            self.repository.find_running("grant", "admin", 7, "pill", "medicine", 2, 20),
            "frozen",
        )
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO user_xiuxian(user_id) VALUES('new-user')")

        resumed = self.adjust("frozen", chunk_size=1000)
        duplicate = self.adjust("frozen")

        self.assertEqual((resumed.total, resumed.completed, resumed.added), (3, 3, 6))
        self.assertEqual((resumed.granted_users, resumed.skipped_users), (3, 0))
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(
            self.rows("SELECT SUM(goods_num) AS total FROM back")[0]["total"], 6
        )
        self.assertEqual(self.rows("SELECT COUNT(*) AS total FROM economy_log")[0]["total"], 3)

    def test_chunk_size_is_capped_by_repository(self) -> None:
        with patch.object(AdminItemBatchSqlRepository, "max_chunk_size", 1):
            result = self.adjust("bounded", chunk_size=10000)
        self.assertEqual((result.total, result.completed), (3, 1))

    def test_matching_running_request_is_reused_and_conflicts_are_rejected(self) -> None:
        first = self.adjust("first", chunk_size=1)
        duplicate = self.adjust("second", chunk_size=1)
        conflict = self.adjust("first", quantity=3, chunk_size=1)
        self.assertEqual((first.status, first.completed), ("applied", 1))
        self.assertEqual((duplicate.status, duplicate.completed), ("in_progress", 1))
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(
            self.rows("SELECT COUNT(*) AS total FROM admin_item_batch_operations")[0]["total"],
            1,
        )

    def test_destroy_partially_removes_and_skips_users_without_the_item(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES('a',7,'pill','medicine',1,"
                "CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0)"
            )

        result = self.adjust("destroy-missing", action="destroy")
        self.assertEqual((result.completed, result.removed), (3, 1))
        self.assertEqual((result.affected_users, result.skipped_users), (1, 2))
        self.assertEqual(
            self.rows("SELECT goods_num FROM back WHERE user_id='a'")[0]["goods_num"], 0
        )

    def test_grant_capacity_and_destroy_use_actual_inventory(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES(?,7,'pill','medicine',?,"
                "CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0)",
                (("a", 19), ("b", 18)),
            )

        granted = self.adjust("capacity", max_goods_num=20)
        self.assertEqual((granted.completed, granted.added, granted.affected_users), (3, 4, 2))

        destroyed = self.adjust("destroy", action="destroy", item_type="medicine")
        self.assertEqual((destroyed.removed, destroyed.affected_users), (6, 3))
        self.assertEqual(
            [row["goods_num"] for row in self.rows("SELECT goods_num FROM back ORDER BY user_id")],
            [17, 18, 0],
        )

    def test_disk_preflight_and_missing_schema_do_not_create_operations(self) -> None:
        with patch(
            "nonebot_plugin_xiuxian_2.features.admin_asset.item_batch_repository.shutil.disk_usage",
            return_value=SimpleNamespace(free=0),
        ):
            result = self.adjust("low-space")
        self.assertEqual(result.status, "insufficient_space")
        self.assertEqual(
            self.rows("SELECT COUNT(*) AS total FROM admin_item_batch_operations")[0]["total"],
            0,
        )

        missing = Path(self.temp.name) / "missing.db"
        with DatabaseUnitOfWork(missing) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT)")
            uow.execute("INSERT INTO user_xiuxian VALUES('a')")
        result = AdminItemBatchSqlRepository(missing).adjust(
            "grant", "not-ready", "admin", 7, "pill", "medicine", 1, 20
        )
        self.assertEqual(result.status, "not_ready")
        with DatabaseUnitOfWork(missing, read_only=True) as uow:
            self.assertIsNone(
                uow.query_one(
                    "SELECT name FROM sqlite_master WHERE name='admin_item_batch_operations'"
                )
            )

    def test_child_receipt_replay_recovers_progress_failure_without_double_grant(self) -> None:
        with patch.object(self.repository, "_record", side_effect=RuntimeError("progress write failed")):
            with self.assertRaisesRegex(RuntimeError, "progress write failed"):
                self.adjust("child-replay", chunk_size=1)

        resumed = self.adjust("child-replay", chunk_size=1)
        self.assertEqual((resumed.completed, resumed.added), (1, 2))
        self.assertEqual(
            self.rows("SELECT goods_num FROM back WHERE user_id='a'")[0]["goods_num"], 2
        )
        self.assertEqual(
            self.rows("SELECT COUNT(*) AS total FROM economy_log WHERE trace_id='child-replay'")[0]["total"],
            1,
        )

    def test_economy_log_failure_rolls_back_child_inventory_and_receipt(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_batch_log BEFORE INSERT ON economy_log "
                "WHEN NEW.trace_id='log-failure' BEGIN SELECT RAISE(ABORT,'reject log'); END"
            )
        with self.assertRaisesRegex(Exception, "reject log"):
            self.adjust("log-failure", chunk_size=1)
        self.assertEqual(self.rows("SELECT COUNT(*) AS total FROM back")[0]["total"], 0)
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) AS total FROM admin_item_grant_operations "
                "WHERE operation_id='admin-item-batch:log-failure:grant:a'"
            )[0]["total"],
            0,
        )

        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TRIGGER reject_batch_log")
        self.assertEqual(self.adjust("log-failure", chunk_size=1).added, 2)

    def test_legacy_running_grant_imports_progress_in_database_and_resumes(self) -> None:
        request = ["admin", 7, "pill", "medicine", 2, 20]
        payload = json.dumps(
            {"request": request, "users": ["a", "b", "c"]},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO admin_item_batch_grant_operations"
                "(operation_id,payload,total,completed,added,status,created_at,updated_at) "
                "VALUES('legacy',?,3,2,2,'running',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                (payload,),
            )
            uow.executemany(
                "INSERT INTO admin_item_batch_grant_progress(operation_id,user_id,added) "
                "VALUES('legacy',?,?)",
                (("a", 2), ("c", 0)),
            )
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES('a',7,'pill','medicine',2,"
                "CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0)"
            )

        self.assertEqual(
            self.repository.find_running("grant", "admin", 7, "pill", "medicine", 2, 20),
            "legacy",
        )
        result = self.adjust("legacy", chunk_size=10)
        self.assertEqual((result.total, result.completed, result.added), (3, 3, 4))
        targets = self.rows(
            "SELECT user_id,status,added_quantity FROM admin_item_batch_targets "
            "WHERE operation_id='legacy' ORDER BY user_id"
        )
        self.assertEqual(
            [(row["user_id"], row["status"], row["added_quantity"]) for row in targets],
            [("a", "legacy_completed", 2), ("b", "granted", 2), ("c", "legacy_completed", 0)],
        )

    def test_legacy_payload_over_limit_is_preserved_without_import(self) -> None:
        payload = json.dumps(
            {"request": ["admin", 7, "pill", "medicine", 2, 20], "users": ["a", "b", "c"]},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO admin_item_batch_grant_operations"
                "(operation_id,payload,total,completed,added,status,created_at,updated_at) "
                "VALUES('legacy-large',?,3,1,2,'running',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                (payload,),
            )
        with patch.object(AdminItemBatchSqlRepository, "max_legacy_payload_chars", 1):
            result = self.adjust("legacy-large")
        self.assertEqual(result.status, "legacy_payload_too_large")
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) AS total FROM admin_item_batch_operations "
                "WHERE operation_id='legacy-large'"
            )[0]["total"],
            0,
        )
        self.assertEqual(
            self.rows(
                "SELECT status FROM admin_item_batch_grant_operations "
                "WHERE operation_id='legacy-large'"
            )[0]["status"],
            "running",
        )

    def test_legacy_roster_count_mismatch_never_becomes_executable(self) -> None:
        payload = json.dumps(
            {"request": ["admin", 7, "pill", "medicine", 2, 20], "users": ["a", "b"]},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO admin_item_batch_grant_operations"
                "(operation_id,payload,total,completed,added,status,created_at,updated_at) "
                "VALUES('legacy-corrupt',?,3,0,0,'running',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                (payload,),
            )

        first = self.adjust("legacy-corrupt", chunk_size=100)
        second = self.adjust("legacy-corrupt", chunk_size=100)
        self.assertEqual((first.status, second.status), ("progress_corrupt", "progress_corrupt"))
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) AS total FROM admin_item_batch_operations "
                "WHERE operation_id='legacy-corrupt'"
            )[0]["total"],
            0,
        )
        self.assertEqual(self.rows("SELECT COUNT(*) AS total FROM back")[0]["total"], 0)

    def test_completed_legacy_batch_keeps_its_saved_summary(self) -> None:
        payload = json.dumps(
            {"request": ["admin", 7, "pill", "medicine", 2, 20], "users": ["a", "b"]},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO admin_item_batch_grant_operations"
                "(operation_id,payload,total,completed,added,status,created_at,updated_at) "
                "VALUES('legacy-done',?,2,2,2,'completed',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                (payload,),
            )
            uow.executemany(
                "INSERT INTO admin_item_batch_grant_progress(operation_id,user_id,added) "
                "VALUES('legacy-done',?,?)",
                (("a", 2), ("b", 0)),
            )

        result = self.adjust("legacy-done")
        self.assertEqual((result.status, result.total, result.completed), ("duplicate", 2, 2))
        self.assertEqual((result.added, result.granted_users, result.skipped_users), (2, 1, 1))
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) AS total FROM admin_item_batch_targets "
                "WHERE operation_id='legacy-done'"
            )[0]["total"],
            0,
        )

    def test_item_batch_migration_is_idempotent(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            apply_admin_item_batch(uow)
        names = {row["name"] for row in self.rows("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue(
            {
                "admin_item_batch_operations",
                "admin_item_batch_targets",
                "admin_item_batch_grant_operations",
                "admin_item_batch_grant_progress",
            }.issubset(names)
        )


__all__ = ["AdminItemBatchRepositoryTests"]
