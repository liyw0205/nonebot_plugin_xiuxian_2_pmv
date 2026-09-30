from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ....infrastructure.database import DatabaseUnitOfWork
from ..accessory_batch_repository import AdminAccessoryBatchSqlRepository
from ..migrations import (
    apply_admin_accessory_batch,
    apply_admin_accessory_operations,
    apply_admin_stone_adjustment,
)


class AdminAccessoryBatchRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id) VALUES(?)",
                (("a",), ("b",), ("c",)),
            )
            apply_admin_stone_adjustment(uow)
            apply_admin_accessory_operations(uow)
            apply_admin_accessory_batch(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "CREATE TABLE player_accessory("
                "user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
            )
        self.repository = AdminAccessoryBatchSqlRepository(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def accessory(uid: str) -> dict:
        return {
            "uid": uid,
            "item_id": 10,
            "name": "测试饰品",
            "part": "项链",
            "set_type": "测试",
            "quality": 3,
            "affixes": [],
            "locked_affixes": [],
            "wash_count": 0,
        }

    def bag(self, user_id: str) -> list[dict]:
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            row = uow.query_one(
                "SELECT bag FROM player_accessory WHERE user_id=?", (user_id,)
            )
        return json.loads(row["bag"]) if row is not None else []

    def grant(self, operation_id: str, user_ids, create_accessory, **kwargs):
        return self.repository.grant(
            operation_id,
            "admin",
            user_ids,
            10,
            "测试饰品",
            3,
            1,
            10,
            create_accessory,
            **kwargs,
        )

    def test_chunks_freeze_targets_and_replay_without_regeneration(self) -> None:
        generated = Counter()

        def create(user_id: str) -> dict:
            generated[user_id] += 1
            return self.accessory(f"new-{user_id}")

        first = self.grant("batch-1", ("b", "a", "b"), create, chunk_size=1)
        self.assertEqual((first.status, first.completed, first.total), ("applied", 1, 2))
        self.assertEqual(self.repository.find_running("grant", "admin", 10, "测试饰品", 3, 1, 10), "batch-1")

        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("INSERT INTO user_xiuxian(user_id) VALUES('new-user')")

        resumed = self.grant(
            "batch-1",
            ("new-user", "a", "b"),
            create,
            chunk_size=10,
        )
        duplicate = self.grant(
            "batch-1",
            ("a", "b"),
            lambda _user_id: self.fail("completed operation regenerated accessories"),
        )

        self.assertEqual((resumed.completed, resumed.total), (2, 2))
        self.assertEqual((resumed.affected_users, resumed.skipped_users), (2, 0))
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(generated, Counter({"a": 1, "b": 1}))
        self.assertEqual(self.bag("a")[0]["uid"], "new-a")
        self.assertEqual(self.bag("b")[0]["uid"], "new-b")
        self.assertEqual(self.bag("new-user"), [])

    def test_legacy_payload_and_progress_are_imported_on_resume(self) -> None:
        request = {
            "action": "destroy",
            "operator_id": "admin",
            "item_id": 10,
            "item_name": "测试饰品",
            "quality": 0,
            "quantity": 1,
            "max_accessories": 0,
        }
        payload = json.dumps(
            {"request": request, "users": ["b", "a"]},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT INTO admin_accessory_batch_operations"
                "(operation_id,action,payload,total,status) VALUES(?,?,?,?,?)",
                ("legacy", "destroy", payload, 2, "running"),
            )
            uow.execute(
                "INSERT INTO admin_accessory_batch_progress"
                "(operation_id,user_id,status,affected_quantity,result_json) "
                "VALUES('legacy','a','item_missing',0,'{}')"
            )

        result = self.repository.destroy(
            "legacy", "admin", ("a", "b"), 10, "测试饰品", 1
        )

        self.assertEqual((result.status, result.total, result.completed), ("applied", 2, 2))
        self.assertEqual((result.affected_users, result.skipped_users), (0, 2))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT user_id,status FROM admin_accessory_batch_targets "
                "WHERE operation_id='legacy' ORDER BY user_id"
            )
        self.assertEqual([(row["user_id"], row["status"]) for row in rows], [
            ("a", "item_missing"), ("b", "item_missing")
        ])

    def test_completed_legacy_batch_keeps_its_saved_summary(self) -> None:
        request = {
            "action": "destroy",
            "operator_id": "admin",
            "item_id": 10,
            "item_name": "测试饰品",
            "quality": 0,
            "quantity": 2,
            "max_accessories": 0,
        }
        payload = json.dumps(
            {"request": request, "users": ["a", "b"]},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT INTO admin_accessory_batch_operations"
                "(operation_id,action,payload,total,status) VALUES(?,?,?,?,?)",
                ("legacy-done", "destroy", payload, 2, "completed"),
            )
            uow.executemany(
                "INSERT INTO admin_accessory_batch_progress"
                "(operation_id,user_id,status,affected_quantity,result_json) "
                "VALUES('legacy-done',?,?,?,'{}')",
                (("a", "destroyed", 2), ("b", "item_missing", 0)),
            )

        result = self.repository.destroy(
            "legacy-done", "admin", ("a", "b"), 10, "测试饰品", 2
        )

        self.assertEqual((result.status, result.total, result.completed), ("duplicate", 2, 2))
        self.assertEqual((result.affected_quantity, result.affected_users, result.skipped_users), (2, 1, 1))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM admin_accessory_batch_targets "
                    "WHERE operation_id='legacy-done'"
                )["count"],
                0,
            )

    def test_legacy_resume_low_disk_preserves_original_progress(self) -> None:
        request = {
            "action": "destroy",
            "operator_id": "admin",
            "item_id": 10,
            "item_name": "测试饰品",
            "quality": 0,
            "quantity": 1,
            "max_accessories": 0,
        }
        payload = json.dumps(
            {"request": request, "users": ["a", "b"]},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT INTO admin_accessory_batch_operations"
                "(operation_id,action,payload,total,status) VALUES(?,?,?,?,?)",
                ("legacy-low-space", "destroy", payload, 2, "running"),
            )
            uow.execute(
                "INSERT INTO admin_accessory_batch_progress"
                "(operation_id,user_id,status,affected_quantity,result_json) "
                "VALUES('legacy-low-space','a','item_missing',0,'{}')"
            )

        with patch(
            "nonebot_plugin_xiuxian_2.features.admin_asset.accessory_batch_repository.shutil.disk_usage",
            return_value=SimpleNamespace(free=0),
        ):
            result = self.repository.destroy(
                "legacy-low-space", "admin", ("a", "b"), 10, "测试饰品", 1
            )

        self.assertEqual((result.status, result.total, result.completed), (
            "insufficient_space", 2, 1
        ))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM admin_accessory_batch_targets "
                    "WHERE operation_id='legacy-low-space'"
                )["count"],
                0,
            )
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM admin_accessory_batch_progress "
                    "WHERE operation_id='legacy-low-space'"
                )["count"],
                1,
            )

    def test_low_disk_fails_before_persisting_a_batch(self) -> None:
        with patch(
            "nonebot_plugin_xiuxian_2.features.admin_asset.accessory_batch_repository.shutil.disk_usage",
            return_value=SimpleNamespace(free=0),
        ):
            result = self.grant("low-space", ("a", "b"), lambda uid: self.accessory(uid))

        self.assertEqual(result.status, "insufficient_space")
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM admin_accessory_batch_operations"
                )["count"],
                0,
            )

    def test_duplicate_running_request_does_not_start_a_second_plan(self) -> None:
        factory = lambda user_id: self.accessory(f"item-{user_id}")
        first = self.grant("first", ("a", "b"), factory, chunk_size=1)
        duplicate = self.grant("second", ("a", "b"), factory, chunk_size=1)

        self.assertEqual((first.status, first.completed), ("applied", 1))
        self.assertEqual((duplicate.status, duplicate.total, duplicate.completed), (
            "in_progress", 2, 1
        ))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM admin_accessory_batch_operations"
                )["count"],
                1,
            )

    def test_missing_player_schema_keeps_target_pending_for_resume(self) -> None:
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("DROP TABLE player_accessory")

        result = self.grant("not-ready", ("a",), lambda uid: self.accessory(uid))

        self.assertEqual((result.status, result.completed, result.total), ("not_ready", 0, 1))
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            target = uow.query_one(
                "SELECT status FROM admin_accessory_batch_targets "
                "WHERE operation_id='not-ready' AND user_id='a'"
            )
        self.assertEqual(target["status"], "pending")

    def test_target_write_failure_replays_child_receipt_without_duplicate_grant(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TRIGGER reject_target_update BEFORE UPDATE ON "
                "admin_accessory_batch_targets BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        generated = Counter()

        def create(user_id: str) -> dict:
            generated[user_id] += 1
            return self.accessory(f"fixed-{user_id}")

        with self.assertRaisesRegex(sqlite3.IntegrityError, "failed"):
            self.grant("replay", ("a",), create)
        self.assertEqual([item["uid"] for item in self.bag("a")], ["fixed-a"])
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TRIGGER reject_target_update")

        resumed = self.grant(
            "replay", ("a",), lambda _user_id: self.fail("child receipt was not replayed")
        )

        self.assertEqual((resumed.completed, resumed.affected_quantity), (1, 1))
        self.assertEqual(generated, Counter({"a": 1}))
        self.assertEqual([item["uid"] for item in self.bag("a")], ["fixed-a"])


if __name__ == "__main__":
    unittest.main()
