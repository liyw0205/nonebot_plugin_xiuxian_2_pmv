from __future__ import annotations

import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.transaction_service import (
    AdminAccessoryBatchAdjustmentService,
)
from tests.test_db_backend import db_backend


class AdminAccessoryBatchAdjustmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game_database = root / "game.db"
        self.player_database = root / "player.db"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s)",
                (("1",), ("2",), ("3",)),
            )
        self.service = AdminAccessoryBatchAdjustmentService(
            self.game_database, self.player_database
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def accessory(uid, item_id=10, name="测试饰品"):
        return {
            "uid": uid,
            "item_id": item_id,
            "name": name,
            "part": "项链",
            "set_type": "测试",
            "quality": 3,
            "affixes": [],
            "locked_affixes": [],
            "wash_count": 0,
        }

    def bag(self, user_id):
        with db_backend.connection(self.player_database) as conn:
            row = conn.execute(
                "SELECT bag FROM player_accessory WHERE user_id=%s", (str(user_id),)
            ).fetchone()
            return json.loads(row[0]) if row else []

    def grant(self, operation="batch-grant", user_ids=("1", "2"), **changes):
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_ids=user_ids,
            item_id=10,
            item_name="测试饰品",
            quality=3,
            quantity=1,
            max_accessories=10,
            create_accessory=lambda user_id: self.accessory(f"new-{user_id}"),
            chunk_size=100,
        )
        values.update(changes)
        return self.service.grant(**values)

    def test_grant_resumes_with_frozen_users_and_fixed_child_results(self) -> None:
        generated = Counter()

        def create(user_id):
            generated[user_id] += 1
            return self.accessory(f"new-{user_id}-{generated[user_id]}")

        first = self.grant(
            "resume", user_ids=("2", "1", "2"), create_accessory=create,
            chunk_size=1,
        )
        self.assertEqual(
            (first.status, first.total, first.completed, first.affected_quantity),
            ("applied", 2, 1, 1),
        )
        self.assertEqual(
            "resume",
            self.service.find_running(
                "grant", "admin", 10, "测试饰品", 3, 1, 10
            ),
        )

        resumed = self.grant(
            "resume", user_ids=("1", "2", "3"), create_accessory=create
        )
        duplicate = self.grant(
            "resume",
            user_ids=("1", "2"),
            create_accessory=lambda _user_id: self.fail("duplicate regenerated"),
        )

        self.assertEqual(
            (resumed.completed, resumed.affected_users, resumed.skipped_users),
            (2, 2, 0),
        )
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(generated, Counter({"1": 1, "2": 1}))
        self.assertEqual([item["uid"] for item in self.bag("1")], ["new-1-1"])
        self.assertEqual([item["uid"] for item in self.bag("2")], ["new-2-1"])
        self.assertEqual(self.bag("3"), [])
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                2,
                conn.execute(
                    "SELECT COUNT(*) FROM economy_log "
                    "WHERE action='admin_accessory_add'"
                ).fetchone()[0],
            )

    def test_request_conflict_does_not_replace_frozen_plan(self) -> None:
        self.assertEqual(
            "applied", self.grant("conflict", chunk_size=1).status
        )
        conflict = self.grant("conflict", quantity=2)

        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(conflict.completed, 1)
        self.assertIsNone(
            self.service.find_running(
                "grant", "other-admin", 10, "测试饰品", 3, 1, 10
            )
        )

    def test_destroy_tracks_partial_results_and_preserves_equipped(self) -> None:
        equipped = {"项链": self.accessory("equipped")}
        bags = {
            "1": [self.accessory("bag-1"), self.accessory("other", 11, "其他饰品")],
            "2": [self.accessory("other-2", 11, "其他饰品")],
        }
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                "CREATE TABLE player_accessory("
                "user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
            )
            conn.executemany(
                "INSERT INTO player_accessory VALUES(%s,%s,%s)",
                (
                    (
                        user_id,
                        json.dumps(equipped, ensure_ascii=False),
                        json.dumps(bag, ensure_ascii=False),
                    )
                    for user_id, bag in bags.items()
                ),
            )

        result = self.service.destroy(
            "batch-destroy", "admin", ("1", "2"), 10, "测试饰品", 3
        )

        self.assertEqual(
            (
                result.status,
                result.completed,
                result.affected_quantity,
                result.affected_users,
                result.skipped_users,
            ),
            ("applied", 2, 1, 1, 1),
        )
        with db_backend.connection(self.player_database) as conn:
            row = conn.execute(
                "SELECT equipped,bag FROM player_accessory WHERE user_id='1'"
            ).fetchone()
            self.assertEqual(json.loads(row[0]), equipped)
            self.assertEqual([item["uid"] for item in json.loads(row[1])], ["other"])

    def test_progress_failure_replays_child_without_duplicate_grant(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER reject_accessory_batch_progress BEFORE INSERT ON "
                "admin_accessory_batch_progress BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        generated = Counter()

        def create(user_id):
            generated[user_id] += 1
            return self.accessory(f"fixed-{user_id}")

        with self.assertRaises(db_backend.IntegrityError):
            self.grant(
                "progress-failure", user_ids=("1",), create_accessory=create
            )
        self.assertEqual([item["uid"] for item in self.bag("1")], ["fixed-1"])
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_accessory_batch_progress"
                ).fetchone()[0],
            )
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("DROP TRIGGER reject_accessory_batch_progress")

        resumed = self.grant(
            "progress-failure",
            user_ids=("1",),
            create_accessory=lambda _user_id: self.fail("child regenerated"),
        )

        self.assertEqual((resumed.completed, resumed.affected_quantity), (1, 1))
        self.assertEqual(generated, Counter({"1": 1}))
        self.assertEqual(len(self.bag("1")), 1)
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM economy_log "
                    "WHERE action='admin_accessory_add'"
                ).fetchone()[0],
            )

    def test_production_all_branches_use_batch_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("admin_accessory_batch_adjustment_service.grant(", source)
        self.assertIn("admin_accessory_batch_adjustment_service.destroy(", source)
        self.assertGreaterEqual(
            source.count("admin_accessory_batch_adjustment_service.find_running("), 2
        )


if __name__ == "__main__":
    unittest.main()
