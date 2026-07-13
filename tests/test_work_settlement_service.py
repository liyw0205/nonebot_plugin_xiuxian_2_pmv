from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.settlement_service import WorkSettlementService
from tests.test_db_backend import db_backend


class WorkSettlementServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, exp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("user", 90))
            conn.execute("CREATE TABLE user_cd (user_id TEXT PRIMARY KEY, type INTEGER, create_time TEXT, scheduled_time TEXT)")
            conn.execute("INSERT INTO user_cd VALUES (%s,%s,%s,%s)", ("user", 2, "2026-07-13 01:00:00", "任务"))
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id, goods_id))")
        self.service = WorkSettlementService(self.database)
        self.expected = {"create_time": "2026-07-13 01:00:00", "scheduled_time": "任务"}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def settle(self, operation_id="settle", **overrides):
        values = dict(exp_gain=20, item={"id": 1, "name": "奖励", "type": "特殊物品"}, max_exp=100, max_goods_num=99)
        values.update(overrides)
        return self.service.settle(operation_id, "user", self.expected, values["exp_gain"], values["item"], values["max_exp"], values["max_goods_num"])

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute("SELECT exp FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()
            work = conn.execute("SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=%s", ("user",)).fetchone()
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("user", 1)).fetchone()
        return int(user[0]), tuple(work), tuple(map(int, item)) if item else None

    def test_success_caps_exp_marks_work_done_and_awards_item(self) -> None:
        result = self.settle()
        self.assertEqual((result.status, result.exp, result.item_awarded), ("applied", 10, True))
        self.assertEqual(self.state(), (100, (0, "0", None), (1, 1)))

    def test_inventory_failure_and_stale_work_leave_everything_unchanged(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("user", 1, "奖励", "特殊物品", 99, "", "", 99))
        self.assertEqual(self.settle("full").status, "inventory_full")
        self.assertEqual(self.state(), (90, (2, "2026-07-13 01:00:00", "任务"), (99, 99)))
        self.assertEqual(self.settle("stale", item=None, max_goods_num=100).status, "applied")
        self.assertEqual(self.settle("changed", item=None, max_goods_num=100).status, "state_changed")

    def test_duplicate_reuses_first_result_and_conflicting_retry_is_rejected(self) -> None:
        first = self.settle("repeat")
        duplicate = self.settle("repeat")
        conflict = self.settle("repeat", exp_gain=10)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(self.state(), (100, (0, "0", None), (1, 1)))

    def test_operation_write_failure_rolls_back_everything(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE work_settlement_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, exp INTEGER NOT NULL, item_awarded INTEGER NOT NULL, created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_settlement BEFORE INSERT ON work_settlement_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.state(), (90, (2, "2026-07-13 01:00:00", "任务"), None))


if __name__ == "__main__":
    unittest.main()
