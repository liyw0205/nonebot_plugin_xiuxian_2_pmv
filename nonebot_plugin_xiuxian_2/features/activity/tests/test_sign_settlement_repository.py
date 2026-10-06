from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..migrations import apply_activity_state_schema
from ..sign_settlement_repository import ActivitySignSettlementSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class ActivitySignSettlementRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone REAL)"
            )
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            uow.execute(
                "INSERT INTO activity_user(user_id,sign_days,last_sign_date,total_sign_days) "
                "VALUES('u',2,'2026-10-05',5)"
            )
        self.repository = ActivitySignSettlementSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def settle(self, operation_id: str = "sign-1", **changes):
        values = {
            "operation_id": operation_id,
            "user_id": "u",
            "sign_date": "2026-10-06",
            "expected_sign_days": 2,
            "expected_total_sign_days": 5,
            "daily_rewards": ({"type": "stone", "quantity": 50},
                              {"type": "道具", "id": 101, "name": "签到令", "quantity": 2}),
            "milestone_rewards": ({"type": "stone", "quantity": 30},),
            "max_goods_num": 100,
            "daily_reward_text": "灵石x50,签到令x2",
            "milestone_reward_text": "灵石x30",
        }
        values.update(changes)
        return self.repository.settle(**values)

    def test_settlement_and_receipt_are_atomic_and_replay_once(self) -> None:
        first = self.settle()
        replay = self.settle()
        self.assertEqual(("applied", 3, 6),
                         (first.status, first.sign_days, first.total_sign_days))
        self.assertEqual(("duplicate", 3, 6),
                         (replay.status, replay.sign_days, replay.total_sign_days))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(
                (90.0, 3, "2026-10-06", 6),
                tuple(uow.query_one(
                    "SELECT user_xiuxian.stone,activity_user.sign_days,"
                    "activity_user.last_sign_date,activity_user.total_sign_days "
                    "FROM user_xiuxian JOIN activity_user USING(user_id) WHERE user_id='u'"
                ).values()),
            )
            self.assertEqual(2, uow.query_one(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=101"
            )["goods_num"])
            self.assertEqual(1, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_sign_log"
            )["count"])

    def test_operation_conflict_and_late_write_failure_do_not_duplicate_rewards(self) -> None:
        self.assertEqual("applied", self.settle().status)
        self.assertEqual("operation_conflict", self.settle(
            sign_date="2026-10-07"
        ).status)
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TRIGGER fail_sign_receipt BEFORE INSERT ON "
                "activity_sign_settlement_operations BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
            )
            uow.execute("DELETE FROM activity_sign_settlement_operations")
            uow.execute("DELETE FROM activity_sign_log")
            uow.execute(
                "UPDATE activity_user SET sign_days=2,last_sign_date='2026-10-05',"
                "total_sign_days=5 WHERE user_id='u'"
            )
            uow.execute("UPDATE user_xiuxian SET stone=10 WHERE user_id='u'")
            uow.execute("DELETE FROM back")
        with self.assertRaisesRegex(Exception, "receipt failed"):
            self.settle("sign-2")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(10.0, uow.query_one(
                "SELECT stone FROM user_xiuxian WHERE user_id='u'"
            )["stone"])
            self.assertEqual(2, uow.query_one(
                "SELECT sign_days FROM activity_user WHERE user_id='u'"
            )["sign_days"])
            self.assertEqual(0, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_sign_log"
            )["count"])
            self.assertEqual(0, uow.query_one(
                "SELECT COUNT(*) AS count FROM back"
            )["count"])


if __name__ == "__main__":
    unittest.main()
