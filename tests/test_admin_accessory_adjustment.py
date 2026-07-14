from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.accessory_adjustment_service import (
    AdminAccessoryAdjustmentService,
)
from tests.test_db_backend import db_backend


class AdminAccessoryAdjustmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game_database = root / "game.db"
        self.player_database = root / "player.db"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u')")
        self.equipped = {"项链": self.accessory("equipped", 10)}
        self.bag = [
            self.accessory("old", 10),
            self.accessory("other", 11, name="其他饰品"),
        ]
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                "CREATE TABLE player_accessory("
                "user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
            )
            conn.execute(
                "INSERT INTO player_accessory VALUES(%s,%s,%s)",
                (
                    "u",
                    json.dumps(self.equipped, ensure_ascii=False),
                    json.dumps(self.bag, ensure_ascii=False),
                ),
            )
        self.service = AdminAccessoryAdjustmentService(
            self.game_database, self.player_database
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def accessory(uid, item_id=10, quality=3, name="测试饰品"):
        return {
            "uid": uid,
            "item_id": item_id,
            "name": name,
            "part": "项链",
            "set_type": "测试",
            "quality": quality,
            "affixes": [],
            "locked_affixes": [],
            "wash_count": 0,
        }

    def read_state(self):
        with db_backend.connection(self.player_database) as conn:
            row = conn.execute(
                "SELECT equipped,bag FROM player_accessory WHERE user_id=%s", ("u",)
            ).fetchone()
            return json.loads(row[0]), json.loads(row[1])

    def grant(self, operation="grant", **changes):
        generated = iter(
            [self.accessory("new-1"), self.accessory("new-2")]
        )
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_id="u",
            item_id=10,
            item_name="测试饰品",
            quality=3,
            quantity=2,
            expected_equipped=self.equipped,
            expected_bag=self.bag,
            max_accessories=10,
            create_accessory=lambda: next(generated),
            target_name="道友",
        )
        values.update(changes)
        return self.service.grant(**values)

    def destroy(self, operation="destroy", **changes):
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_id="u",
            item_id=10,
            item_name="测试饰品",
            quantity=3,
            expected_equipped=self.equipped,
            expected_bag=self.bag,
            target_name="道友",
        )
        values.update(changes)
        return self.service.destroy(**values)

    def test_grant_is_atomic_idempotent_and_audited(self) -> None:
        first = self.grant()
        duplicate = self.grant(
            expected_equipped={},
            expected_bag=[],
            create_accessory=lambda: self.fail("duplicate regenerated accessory"),
        )

        self.assertEqual((first.status, duplicate.status), ("granted", "duplicate"))
        self.assertEqual(first.accessories, duplicate.accessories)
        equipped, bag = self.read_state()
        self.assertEqual(equipped, self.equipped)
        self.assertEqual(
            [item["uid"] for item in bag], ["old", "other", "new-1", "new-2"]
        )
        with db_backend.connection(self.game_database) as conn:
            log = conn.execute(
                "SELECT action,item_delta,trace_id FROM economy_log"
            ).fetchone()
            self.assertEqual(("admin_accessory_add", "grant"), (log[0], log[2]))
            self.assertEqual(2, json.loads(log[1])[0]["amount"])
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_accessory_operations"
                ).fetchone()[0],
            )

    def test_grant_rechecks_snapshot_capacity_payload_and_instances(self) -> None:
        self.assertEqual(
            "state_changed", self.grant("stale", expected_bag=[]).status
        )
        self.assertEqual(
            "inventory_full",
            self.grant("full", quantity=1, max_accessories=3).status,
        )
        duplicate_uid = self.accessory("same")
        generated = iter([duplicate_uid, duplicate_uid])
        self.assertEqual(
            "invalid_plan",
            self.grant(
                "duplicate-uid", create_accessory=lambda: next(generated)
            ).status,
        )
        self.assertEqual("granted", self.grant("conflict").status)
        self.assertEqual(
            "operation_conflict",
            self.grant("conflict", quantity=1).status,
        )

    def test_invalid_owned_accessory_state_is_rejected(self) -> None:
        invalid_bag = [self.accessory("same"), self.accessory("same")]
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                "UPDATE player_accessory SET bag=%s WHERE user_id=%s",
                (json.dumps(invalid_bag, ensure_ascii=False), "u"),
            )

        self.assertEqual(
            "invalid_state",
            self.grant("invalid-grant", expected_bag=invalid_bag).status,
        )
        self.assertEqual(
            "invalid_state",
            self.destroy("invalid-destroy", expected_bag=invalid_bag).status,
        )

    def test_destroy_is_partial_idempotent_and_preserves_equipped(self) -> None:
        first = self.destroy()
        duplicate = self.destroy(expected_equipped={}, expected_bag=[])

        self.assertEqual((first.status, duplicate.status), ("destroyed", "duplicate"))
        self.assertEqual((first.affected_quantity, duplicate.affected_quantity), (1, 1))
        equipped, bag = self.read_state()
        self.assertEqual(equipped, self.equipped)
        self.assertEqual([item["uid"] for item in bag], ["other"])
        with db_backend.connection(self.game_database) as conn:
            log = conn.execute(
                "SELECT action,item_delta FROM economy_log"
            ).fetchone()
            self.assertEqual("admin_accessory_cost", log[0])
            self.assertEqual(-1, json.loads(log[1])[0]["amount"])

    def test_destroy_rechecks_snapshot_missing_item_and_payload(self) -> None:
        self.assertEqual(
            "state_changed", self.destroy("stale", expected_bag=[]).status
        )
        self.assertEqual(
            "item_missing", self.destroy("missing", item_id=99).status
        )
        self.assertEqual("destroyed", self.destroy("conflict").status)
        self.assertEqual(
            "operation_conflict",
            self.destroy("conflict", quantity=1).status,
        )

    def test_operation_failure_rolls_back_player_audit_and_operation(self) -> None:
        with db_backend.connection(self.game_database) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS player_data", (str(self.player_database),)
            )
            self.service._ensure_schema(conn)
            conn.commit()
            conn.execute("DETACH DATABASE player_data")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_admin_accessory_operation BEFORE INSERT ON "
                "admin_accessory_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.grant("failed")

        self.assertEqual(self.read_state(), (self.equipped, self.bag))
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                0, conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0]
            )
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_accessory_operations"
                ).fetchone()[0],
            )

    def test_production_entry_has_no_direct_accessory_write(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertGreaterEqual(source.count("_grant_admin_accessory("), 4)
        self.assertGreaterEqual(source.count("_destroy_admin_accessory("), 4)
        self.assertNotIn("add_accessory_to_bag", source)
        self.assertNotIn("remove_accessory_from_bag", source)
        self.assertNotIn("player_data_manager", source)


if __name__ == "__main__":
    unittest.main()
