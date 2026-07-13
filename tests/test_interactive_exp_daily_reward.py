from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_Interactive.exp_daily_reward_service import (
    InteractiveExpDailyRewardService,
)
from tests.test_db_backend import db_backend


class InteractiveExpDailyRewardServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,level TEXT)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s,%s)", ("u1", 100_000, "练气境初期"))
        self.service = InteractiveExpDailyRewardService(self.database)
        self.day = date(2026, 7, 14)
        self.grant_operation = self._operation_for(True)
        self.refuse_operation = self._operation_for(False)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _operation_for(self, granted: bool) -> str:
        for index in range(100):
            operation_id = f"exp-{granted}-{index}"
            if self.service._fixed_roll(operation_id)[0] is granted:
                return operation_id
        raise AssertionError("unable to find deterministic operation")

    def settle(self, operation_id=None, **overrides):
        values = {
            "operation_id": operation_id or self.grant_operation,
            "user_id": "u1",
            "expected_exp": 100_000,
            "expected_level": "练气境初期",
            "rank_value": 69,
            "business_date": self.day,
        }
        values.update(overrides)
        return self.service.settle(**values)

    def test_fixed_reward_claim_and_operation_are_atomic(self) -> None:
        result = self.settle()
        self.assertTrue(result.granted)
        self.assertGreater(result.exp_reward, 0)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(result.exp, conn.execute("SELECT exp FROM user_xiuxian").fetchone()[0])
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM interactive_exp_daily_claims").fetchone()[0])
            operation = conn.execute(
                "SELECT exp_reward FROM interactive_exp_daily_reward_operations"
            ).fetchone()
            self.assertEqual(result.exp_reward, operation[0])

    def test_duplicate_reuses_fixed_result_and_conflicting_payload_is_rejected(self) -> None:
        first = self.settle()
        duplicate = self.settle(expected_exp=first.exp)
        conflict = self.settle(expected_exp=first.exp, expected_level="筑基境初期")
        self.assertEqual(
            ("applied", "duplicate", "operation_conflict"),
            (first.status, duplicate.status, conflict.status),
        )
        self.assertEqual(first.exp_reward, duplicate.exp_reward)

    def test_daily_claim_blocks_another_reward_operation(self) -> None:
        first = self.settle()
        second = self.settle("another-grant", expected_exp=first.exp)
        self.assertEqual("already_claimed", second.status)

    def test_refusal_is_fixed_and_does_not_consume_daily_eligibility(self) -> None:
        refused = self.settle(self.refuse_operation)
        duplicate = self.settle(self.refuse_operation)
        granted = self.settle(self.grant_operation)
        self.assertEqual(
            ("applied", False, "duplicate", "applied"),
            (refused.status, refused.granted, duplicate.status, granted.status),
        )

    def test_state_change_and_operation_failure_leave_state_unchanged(self) -> None:
        self.assertEqual("state_changed", self.settle(expected_exp=99_999).status)

        def fail(checkpoint: str) -> None:
            if checkpoint == "after_claim":
                raise RuntimeError("injected failure")

        self.service = InteractiveExpDailyRewardService(self.database, failure_hook=fail)
        with self.assertRaisesRegex(RuntimeError, "injected failure"):
            self.settle(self.grant_operation)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(100_000, conn.execute("SELECT exp FROM user_xiuxian").fetchone()[0])
            has_claim = conn.table_exists("interactive_exp_daily_claims") and conn.execute(
                "SELECT 1 FROM interactive_exp_daily_claims"
            ).fetchone()
            self.assertFalse(has_claim)

    def test_next_business_date_can_claim_again(self) -> None:
        first = self.settle()
        next_result = self.settle(
            "next-day-exp", expected_exp=first.exp, business_date=self.day + timedelta(days=1)
        )
        self.assertNotEqual("already_claimed", next_result.status)


if __name__ == "__main__":
    unittest.main()
