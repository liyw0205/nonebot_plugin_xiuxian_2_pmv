import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.item_grant_service import (
    AdminItemGrantService,
)
from tests.test_db_backend import db_backend


class AdminItemGrantTransactionTests(unittest.TestCase):
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
            conn.execute(
                "INSERT INTO back VALUES('u',10,'旧名称','道具',3,'','','1')"
            )
        self.service = AdminItemGrantService(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def grant(self, operation="op", **changes):
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_id="u",
            item_id=10,
            item_name="测试丹药",
            item_type="丹药",
            quantity=2,
            expected_quantity=3,
            max_goods_num=10,
            target_name="道友",
        )
        values.update(changes)
        return self.service.grant(**values)

    def test_grant_is_atomic_idempotent_and_audited(self):
        self.assertEqual("granted", self.grant().status)
        self.assertEqual(
            "duplicate", self.grant(expected_quantity=5).status
        )
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                ("测试丹药", "丹药", 5, 1),
                tuple(
                    conn.execute(
                        "SELECT goods_name,goods_type,goods_num,bind_num FROM back"
                    ).fetchone()
                ),
            )
            log = conn.execute(
                "SELECT action,item_delta,trace_id FROM economy_log"
            ).fetchone()
            self.assertEqual(("admin_item_add", "op"), (log[0], log[2]))
            self.assertEqual(2, json.loads(log[1])[0]["amount"])
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_item_grant_operations"
                ).fetchone()[0],
            )

    def test_snapshot_capacity_and_payload_are_rechecked(self):
        self.assertEqual(
            "state_changed", self.grant("stale", expected_quantity=2).status
        )
        self.assertEqual("inventory_full", self.grant("full", quantity=8).status)
        self.assertEqual("granted", self.grant("conflict").status)
        self.assertEqual(
            "operation_conflict",
            self.grant("conflict", quantity=1).status,
        )

    def test_failure_rolls_back_backpack_log_and_operation(self):
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_admin_item_grant BEFORE INSERT ON "
                "admin_item_grant_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(Exception):
            self.grant("fail")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(3, conn.execute("SELECT goods_num FROM back").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0])

    def test_production_entry_uses_service_for_target_and_self_only(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertGreaterEqual(source.count("admin_item_grant_service.grant("), 2)
        self.assertIn('target and str(target).lower() == "all"', source)


if __name__ == "__main__":
    unittest.main()
