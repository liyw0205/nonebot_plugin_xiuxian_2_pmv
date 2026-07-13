import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.item_destroy_service import (
    AdminItemDestroyService,
)
from tests.test_db_backend import db_backend


class AdminItemDestroyTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u')")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER DEFAULT 0,UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES('u',10,'丹药','丹药',5,'','',4)")
        self.service = AdminItemDestroyService(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def destroy(self, operation="op", **changes):
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_id="u",
            item_id=10,
            item_name="丹药",
            item_type="丹药",
            quantity=2,
            expected_quantity=5,
            target_name="道友",
        )
        values.update(changes)
        return self.service.destroy(**values)

    def test_destroy_is_atomic_idempotent_and_audited(self):
        result = self.destroy()
        self.assertEqual(("destroyed", 2), (result.status, result.removed_quantity))
        self.assertEqual("duplicate", self.destroy(expected_quantity=3).status)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                (3, 2), tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone())
            )
            log = conn.execute(
                "SELECT action,item_delta,trace_id FROM economy_log"
            ).fetchone()
            self.assertEqual(("admin_item_cost", "op"), (log[0], log[2]))
            self.assertEqual(-2, json.loads(log[1])[0]["amount"])

    def test_partial_destroy_snapshot_and_payload_conflict(self):
        partial = self.destroy("partial", quantity=8)
        self.assertEqual(("destroyed", 5, 0), (
            partial.status, partial.removed_quantity, partial.final_quantity
        ))
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE back SET goods_num=5,bind_num=4")
        self.assertEqual(
            "state_changed", self.destroy("stale", expected_quantity=4).status
        )
        self.assertEqual("destroyed", self.destroy("conflict").status)
        self.assertEqual(
            "operation_conflict",
            self.destroy("conflict", quantity=1).status,
        )

    def test_destroy_preserves_legacy_bind_deduction_rules(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE back SET goods_num=5,bind_num=1")
        self.assertEqual("destroyed", self.destroy("low-bind").status)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                (3, 1), tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone())
            )

    def test_failure_rolls_back_backpack_log_and_operation(self):
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_admin_item_destroy BEFORE INSERT ON "
                "admin_item_destroy_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(Exception):
            self.destroy("fail")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(5, conn.execute("SELECT goods_num FROM back").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0])

    def test_production_entry_uses_service_for_target_and_self_only(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertGreaterEqual(source.count("admin_item_destroy_service.destroy("), 2)
        self.assertIn("remove_accessory_from_bag", source)


if __name__ == "__main__":
    unittest.main()
