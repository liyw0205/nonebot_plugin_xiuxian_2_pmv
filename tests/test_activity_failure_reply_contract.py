"""Real activity matchers must expose structured rejection reasons to players."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_activity as commands
from nonebot_plugin_xiuxian_2.features.activity.application import ActivityApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service


class ActivityFailureReplyContractTests(unittest.TestCase):
    def _invoke(self, matcher, *, has_args: bool):
        handler = matcher.handlers[0].call
        event = SimpleNamespace(message_id="failure-reason")
        send = AsyncMock()
        args = SimpleNamespace(extract_plain_text=lambda: "不存在的目标")
        with (
            patch.object(commands, "assign_bot", AsyncMock()),
            patch.object(commands, "_ensure_user", AsyncMock(return_value=(True, {"user_id": "u"}, ""))),
            patch.object(
                commands,
                "_run_activity_action",
                return_value=(
                    False,
                    "活动回执保存失败，本次奖励已撤销",
                    {
                        "status": "rejected",
                        "reason": "activity_receipt_write_rejected",
                        "subject": "actor",
                        "message": "活动回执保存失败，本次奖励已撤销",
                    },
                ),
            ),
            patch.object(commands, "handle_send", send),
        ):
            if has_args:
                asyncio.run(handler("bot", event, args))
            else:
                asyncio.run(handler("bot", event))
        return send.call_args.args[2]

    def test_all_mutating_activity_claim_matchers_keep_reason_and_subject(self):
        for matcher, has_args in (
            (commands.activity_claim_cmd, False),
            (commands.activity_task_claim_cmd, True),
            (commands.activity_pass_claim_cmd, True),
            (commands.activity_sign_cmd, False),
            (commands.activity_stage_claim_cmd, True),
            (commands.activity_rank_claim_cmd, True),
            (commands.activity_exchange_cmd, True),
            (commands.activity_buy_cmd, True),
        ):
            with self.subTest(command=matcher):
                text = self._invoke(matcher, has_args=has_args)
                self.assertIn("发起方", text)
                self.assertIn("活动回执保存失败，本次奖励已撤销", text)
                self.assertNotIn("未知错误", text)

    def test_detailed_service_result_keeps_activity_gate_reason(self):
        with (
            patch.object(service, "load_config", return_value={}),
            patch.object(
                service,
                "activity_runtime_state",
                return_value={"ok": False, "reason": "活动已结束"},
            ),
        ):
            result = service.claim_activity_tasks("u", detailed_result=True)
        self.assertEqual(
            {
                "status": "rejected",
                "reason": "activity_unavailable",
                "subject": "actor",
                "message": "活动已结束",
            },
            result,
        )

    def test_legacy_service_tuple_remains_compatible(self):
        with (
            patch.object(service, "load_config", return_value={}),
            patch.object(
                service,
                "activity_runtime_state",
                return_value={"ok": False, "reason": "活动已结束"},
            ),
        ):
            result = service.claim_activity_tasks("u")
        self.assertEqual((False, "活动已结束"), result)

    def test_application_ledger_replays_the_structured_rejection(self):
        class Repository:
            precision_atomic_actions = frozenset()

            def execute(self, operation_id, user_id, action, request):
                return {
                    "status": "rejected",
                    "reason": "inventory_full",
                    "subject": "actor",
                    "message": "背包空间不足，奖励未领取",
                }

        with tempfile.TemporaryDirectory(prefix="activity-reason-ledger-") as directory:
            database = f"{directory}/activity.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
            application = ActivityApplication(database, repository=Repository())
            first = application.execute(
                operation_id="activity:reason:1",
                user_id="u",
                payload={"action": "claim_activity_tasks", "query": "任务"},
            )
            second = application.execute(
                operation_id="activity:reason:1",
                user_id="u",
                payload={"action": "claim_activity_tasks", "query": "任务"},
            )
            self.assertFalse(first.ok)
            self.assertFalse(second.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(first.data, second.data)
            with sqlite3.connect(database) as connection:
                raw = json.loads(
                    connection.execute(
                        "SELECT result_json FROM operation_ledger"
                    ).fetchone()[0]
                )
            self.assertEqual("inventory_full", raw["data"]["reason"])
            self.assertEqual("actor", raw["data"]["subject"])


if __name__ == "__main__":
    unittest.main()
