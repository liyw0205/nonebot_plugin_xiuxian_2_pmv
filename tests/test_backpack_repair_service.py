from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.backpack_repair_service import (
    BackpackRepairService,
)
from tests.test_db_backend import db_backend


class BackpackRepairServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "xiuxian.sqlite3"
        self.service = BackpackRepairService(self.database)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT)"
            )
            conn.execute(
                "CREATE TABLE back("
                "user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,bind_num INTEGER DEFAULT 0,"
                "state INTEGER DEFAULT 0,create_time TEXT,update_time TEXT,"
                "action_time TEXT,UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "CREATE TABLE BuffInfo("
                "user_id TEXT PRIMARY KEY,faqi_buff INTEGER DEFAULT 0,"
                "armor_buff INTEGER DEFAULT 0)"
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def add_user(self, user_id, user_name, faqi=0, armor=0) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO user_xiuxian VALUES(%s,%s)",
                (user_id, user_name),
            )
            conn.execute(
                "INSERT INTO BuffInfo VALUES(%s,%s,%s)",
                (user_id, faqi, armor),
            )

    def add_item(
        self,
        user_id,
        item_id,
        name,
        quantity,
        bind_quantity=0,
        state=0,
    ) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back("
                "user_id,goods_id,goods_name,goods_type,goods_num,bind_num,state) "
                "VALUES(%s,%s,%s,'装备',%s,%s,%s)",
                (
                    user_id,
                    item_id,
                    name,
                    quantity,
                    bind_quantity,
                    state,
                ),
            )

    def item_state(self, user_id, item_id):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT goods_num,bind_num,state,goods_name FROM back "
                "WHERE user_id=%s AND goods_id=%s",
                (user_id, item_id),
            ).fetchone()
        return tuple(row) if row else None

    def task_index(self, operation_id):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT next_index FROM backpack_repair_tasks "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
        return int(row[0]) if row else None

    def test_repairs_in_batches_and_replays_completed_result(self) -> None:
        self.add_user("u1", "甲", faqi=200, armor=999)
        self.add_user("u2", "乙", armor=300)
        self.add_item("u1", 100, "旧名称", 150, 200)
        self.add_item("u2", 300, "旧甲", 0, 0, 0)
        catalog = {"100": "灵石袋", "200": "青锋剑", "300": "玄甲"}

        first = self.service.run(
            "repair-1", catalog, 100, batch_size=1
        )
        second = self.service.run("repair-1", batch_size=1)
        duplicate = self.service.run("repair-1", batch_size=1)

        self.assertEqual("applied", first.status)
        self.assertEqual((first.completed, first.total, first.done), (1, 2, False))
        self.assertEqual((second.status, second.completed, second.done), ("applied", 2, True))
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(
            (
                second.total,
                second.completed,
                second.quantity_fixed,
                second.bind_fixed,
                second.name_fixed,
                second.equipment_fixed,
                second.missing_definitions,
                second.details,
            ),
            (
                duplicate.total,
                duplicate.completed,
                duplicate.quantity_fixed,
                duplicate.bind_fixed,
                duplicate.name_fixed,
                duplicate.equipment_fixed,
                duplicate.missing_definitions,
                duplicate.details,
            ),
        )
        self.assertEqual((100, 100, 0, "灵石袋"), self.item_state("u1", 100))
        self.assertEqual((1, 1, 1, "青锋剑"), self.item_state("u1", 200))
        self.assertEqual((1, 1, 1, "玄甲"), self.item_state("u2", 300))
        self.assertEqual(
            (1, 1, 2, 2, 1),
            (
                second.quantity_fixed,
                second.bind_fixed,
                second.name_fixed,
                second.equipment_fixed,
                second.missing_definitions,
            ),
        )

    def test_new_operation_resumes_existing_task_snapshot(self) -> None:
        self.add_user("u1", "甲")
        self.add_user("u2", "乙")
        self.add_item("u1", 100, "旧", 5)
        self.add_item("u2", 100, "旧", 5)

        first = self.service.run(
            "repair-original", {"100": "原名称"}, 100, batch_size=1
        )
        self.add_user("u3", "丙")
        self.add_item("u3", 100, "旧", 5)
        resumed = self.service.run(
            "repair-new-message", {"100": "变化后的名称"}, 100, batch_size=1
        )

        self.assertFalse(first.done)
        self.assertEqual("repair-original", resumed.operation_id)
        self.assertTrue(resumed.done)
        self.assertEqual(2, resumed.total)
        self.assertEqual("原名称", self.item_state("u2", 100)[3])
        self.assertEqual("旧", self.item_state("u3", 100)[3])

    def test_same_operation_with_changed_request_is_rejected(self) -> None:
        self.add_user("u1", "甲")
        applied = self.service.run("repair-conflict", {"100": "名称"}, 100)
        conflict = self.service.run("repair-conflict", {"100": "新名称"}, 100)

        self.assertTrue(applied.done)
        self.assertEqual("operation_conflict", conflict.status)

    def test_batch_failure_rolls_back_repairs_and_cursor(self) -> None:
        self.add_user("u1", "甲")
        self.add_user("u2", "乙")
        self.add_item("u1", 100, "旧", 5)
        self.add_item("u2", 100, "旧", 500)
        first = self.service.run("repair-fail", {"100": "新"}, 100, batch_size=1)
        self.assertEqual(1, first.completed)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_backpack_repair_progress "
                "BEFORE UPDATE ON backpack_repair_tasks "
                "WHEN OLD.next_index=1 "
                "BEGIN SELECT RAISE(ABORT, 'progress failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.run("repair-fail", batch_size=1)

        self.assertEqual((500, 0, 0, "旧"), self.item_state("u2", 100))
        self.assertEqual(1, self.task_index("repair-fail"))

    def test_empty_database_task_is_persisted_and_replayed(self) -> None:
        first = self.service.run("repair-empty", {}, 100)
        duplicate = self.service.run("repair-empty", {}, 100)

        self.assertEqual((first.status, first.done), ("applied", True))
        self.assertEqual((duplicate.status, duplicate.done), ("duplicate", True))
        self.assertEqual(0, self.task_index("repair-empty"))

    def test_real_handler_uses_repair_task_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_back/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source.split("@check_user_back.handle", 1)[1].split(
            "@compare_items.handle", 1
        )[0]

        self.assertIn("backpack_repair_service.run(", handler)
        self.assertNotIn("check_and_adjust_goods_quantity(", handler)
        self.assertNotIn("sql_message.send_back(", handler)
        self.assertNotIn("sql_message.update_back_equipment(", handler)


if __name__ == "__main__":
    unittest.main()
