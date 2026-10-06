from __future__ import annotations

import tempfile
import unittest
from unittest.mock import patch

from ..application import ActivityApplication
from ..migrations import apply_activity
from ..repository import ActivityRepository
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class ActivityApplicationTest(unittest.TestCase):
    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_activity(uow)
                OperationLedger().ensure_schema(uow)
            app = ActivityApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertEqual("rejected", first.status)
            self.assertTrue(second.replayed)

    def test_reward_repositories_forward_operation_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                OperationLedger().ensure_schema(uow)
            application = ActivityApplication(database, repository=ActivityRepository(database))
            with (
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_activity_tasks",
                    return_value=(True, "任务奖励"),
                ) as tasks,
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_activity_pass_rewards",
                    return_value=(True, "战令奖励"),
                ) as pass_claim,
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_boss.claim_boss_rewards",
                    return_value=(True, "首领奖励"),
                ) as boss,
            ):
                self.assertTrue(application.execute(
                    operation_id="activity:task-claim:u:1", user_id="u",
                    payload={"action": "claim_activity_tasks", "query": ""},
                ).ok)
                self.assertTrue(application.execute(
                    operation_id="activity:pass-claim:u:2", user_id="u",
                    payload={"action": "claim_activity_pass_rewards", "query": ""},
                ).ok)
                self.assertTrue(application.execute(
                    operation_id="activity:boss-claim:u:3", user_id="u",
                    payload={"action": "activity_boss.claim_boss_rewards", "query": "进度"},
                ).ok)

            tasks.assert_called_once_with("u", "", "activity:task-claim:u:1")
            pass_claim.assert_called_once_with("u", "", "activity:pass-claim:u:2")
            boss.assert_called_once_with("u", "进度", "activity:boss-claim:u:3")


if __name__ == "__main__":
    unittest.main()
