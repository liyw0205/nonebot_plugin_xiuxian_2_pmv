from __future__ import annotations

import tempfile
import unittest
from types import SimpleNamespace
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
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_sign",
                    return_value=(True, "签到成功"),
                ) as sign,
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
                self.assertTrue(application.execute(
                    operation_id="activity:sign:u:4", user_id="u",
                    payload={"action": "claim_sign"},
                ).ok)

            tasks.assert_called_once_with("u", "", "activity:task-claim:u:1")
            pass_claim.assert_called_once_with("u", "", "activity:pass-claim:u:2")
            boss.assert_called_once_with("u", "进度", "activity:boss-claim:u:3")
            sign.assert_called_once()
            self.assertEqual("u", sign.call_args.args[0])
            self.assertEqual("activity:sign:u:4", sign.call_args.args[1])
            self.assertEqual(database, str(sign.call_args.kwargs["settlement_repository"].database))

    def test_sign_projection_failure_replays_with_a_stable_event_receipt(self) -> None:
        from ....xiuxian.xiuxian_activity import service

        settlement = SimpleNamespace(
            get_result=lambda operation_id: SimpleNamespace(sign_days=3)
        )
        with (
            patch.object(service, "load_config", return_value={"festival_name": "测试活动"}),
            patch.object(
                service,
                "activity_runtime_state",
                return_value={"ok": True, "features": ["sign"], "stage_name": "签到"},
            ),
            patch.object(service, "today_str", return_value="2026-10-06"),
            patch.object(
                service,
                "record_activity_event",
                side_effect=[RuntimeError("projection unavailable"), []],
            ) as record_event,
        ):
            with self.assertRaisesRegex(RuntimeError, "projection unavailable"):
                service.claim_sign(
                    "u", "activity:sign:u:message-1", settlement_repository=settlement
                )
            ok, text = service.claim_sign(
                "u", "activity:sign:u:message-1", settlement_repository=settlement
            )

        self.assertTrue(ok)
        self.assertIn("累计签到：3 天", text)
        self.assertEqual(2, record_event.call_count)
        self.assertEqual(record_event.call_args_list[0], record_event.call_args_list[1])
        self.assertEqual("activity-sign:u:2026-10-06", record_event.call_args.kwargs["event_id"])
        self.assertIn("2026-10-06T12:00:00", record_event.call_args.kwargs["occurred_at"])


if __name__ == "__main__":
    unittest.main()
