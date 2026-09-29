from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.activity_reward import create_blueprint
from nonebot_plugin_xiuxian_2.features.activity.application import ActivityApplication
from nonebot_plugin_xiuxian_2.features.activity_reward.application import ActivityRewardApplication
from nonebot_plugin_xiuxian_2.features.activity_reward.claim_all_application import ActivityClaimAllApplication
from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import apply_activity_claim_all
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database import OperationLedger
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class _Repository:
    def __init__(self, ok=True):
        self.ok = ok
        self.calls = 0

    def claim_all(self, operation_id, user_id):
        self.calls += 1
        return self.ok, "奖励已领取" if self.ok else "暂无可领取奖励"


class ActivityRewardApplicationTests(unittest.TestCase):
    @staticmethod
    def _application(directory: str, repository: _Repository) -> ActivityRewardApplication:
        database = Path(directory) / "game.db"
        with DatabaseUnitOfWork(database) as uow:
            apply_platform_schema(uow)
            apply_activity_claim_all(uow)
        return ActivityRewardApplication(database, repository=repository)

    def test_success_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = _Repository()
            app = self._application(directory, repo)
            first = app.claim_all(operation_id="activity-1", user_id="u-1")
            replay = app.claim_all(operation_id="activity-1", user_id="u-1")
            self.assertTrue(first.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(repo.calls, 1)
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                row = uow.query_one("SELECT status FROM operation_ledger WHERE operation_id=?", ("activity-1",))
            self.assertEqual(row["status"], "applied")

    def test_rejection_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = _Repository(False)
            app = self._application(directory, repo)
            result = app.claim_all(operation_id="activity-2", user_id="u-1")
            replay = app.claim_all(operation_id="activity-2", user_id="u-1")
            self.assertFalse(result.ok)
            self.assertEqual(result.code, "not_claimable")
            self.assertEqual(replay.code, "not_claimable")
            self.assertEqual(repo.calls, 1)

    def test_real_web_route_retries_a_failed_child_without_duplicate_reward(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
                apply_activity_claim_all(uow)
                uow.execute("CREATE TABLE reward_receipts(child_id TEXT PRIMARY KEY)")
            attempts = {"pass": 0}

            def runners_factory(user_id):
                self.assertEqual(user_id, "u1")

                def task(child_id):
                    with DatabaseUnitOfWork(database, immediate=True) as uow:
                        uow.execute("INSERT OR IGNORE INTO reward_receipts VALUES(?)", (child_id,))
                    return True, "task reward"

                def battle_pass(child_id):
                    attempts["pass"] += 1
                    if attempts["pass"] == 1:
                        raise RuntimeError("temporary")
                    return False, "no reward"

                return {"tasks": task, "pass": battle_pass,
                        "boss_milestone": lambda child_id: (False, "no reward"),
                        "boss_rank": lambda child_id: (False, "no reward")}

            web = Flask(__name__)
            web.secret_key = "test"
            web.register_blueprint(create_blueprint(
                application=ActivityRewardApplication(database, runners_factory=runners_factory),
                permission=lambda _: True,
            ))
            client = web.test_client()
            with client.session_transaction() as session:
                session["_csrf_token"] = "csrf"
            headers = {"Idempotency-Key": "real-claim", "X-CSRF-Token": "csrf"}
            url = "/api/v1/activity/rewards/claim"
            first = client.post(url, headers=headers, json={"user_id": "u1"})
            second = client.post(url, headers=headers, json={"user_id": "u1"})
            replay = client.post(url, headers=headers, json={"user_id": "u1"})
            self.assertEqual((first.status_code, second.status_code, replay.status_code), (409, 200, 200))
            self.assertEqual(first.get_json()["data"]["code"], "retryable_failure")
            self.assertTrue(second.get_json()["data"]["ok"])
            self.assertTrue(replay.get_json()["data"]["replayed"])
            self.assertEqual(attempts["pass"], 2)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(uow.query_one("SELECT count(*) AS n FROM reward_receipts")["n"], 1)
                self.assertEqual(uow.query_one(
                    "SELECT attempts FROM activity_claim_all_steps WHERE operation_id='real-claim' AND step_name='tasks'"
                )["attempts"], 1)

    def test_started_outer_receipt_resumes_through_feature_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
                apply_activity_claim_all(uow)
                OperationLedger().begin(uow, "stranded", ActivityRewardApplication.action, {"user_id": "u1"})
            application = ActivityRewardApplication(database, runners_factory=lambda _: {
                name: (lambda child_id: (False, "no reward"))
                for name in ActivityClaimAllApplication.step_names()
            })
            result = application.claim_all(operation_id="stranded", user_id="u1")
            self.assertEqual(result.status, "rejected")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(uow.query_one(
                    "SELECT status FROM operation_ledger WHERE operation_id='stranded' AND action='activity.claim_all'"
                )["status"], "rejected")

    def test_default_command_repository_uses_feature_coordinator(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
                apply_activity_claim_all(uow)
            calls = []

            def factory(user_id):
                self.assertEqual(user_id, "u1")
                return {name: (lambda child_id, step=name: (calls.append(child_id), (step == "tasks", step))[1])
                        for name in ActivityClaimAllApplication.step_names()}

            with patch("nonebot_plugin_xiuxian_2.compatibility.legacy_activity_claim_steps.build_legacy_activity_claim_runners", factory):
                application = ActivityApplication(database)
                first = application.execute(operation_id="cmd-claim", user_id="u1", payload={"action": "claim_activity_rewards"})
                replay = application.execute(operation_id="cmd-claim", user_id="u1", payload={"action": "claim_activity_rewards"})
            self.assertTrue(first.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(len(calls), 4)

    def test_web_then_command_reuses_the_same_feature_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
                apply_activity_claim_all(uow)
            calls = []

            def factory(user_id):
                self.assertEqual(user_id, "u1")
                return {name: (lambda child_id, step=name: (calls.append(child_id), (step == "tasks", step))[1])
                        for name in ActivityClaimAllApplication.step_names()}

            with patch("nonebot_plugin_xiuxian_2.compatibility.legacy_activity_claim_steps.build_legacy_activity_claim_runners", factory):
                web_result = ActivityRewardApplication(database).claim_all(operation_id="shared", user_id="u1")
                command_result = ActivityApplication(database).execute(
                    operation_id="shared", user_id="u1", payload={"action": "claim_activity_rewards"},
                )
            self.assertTrue(web_result.ok)
            self.assertTrue(command_result.ok)
            self.assertEqual(len(calls), 4)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(uow.query_one(
                    "SELECT count(*) AS n FROM activity_claim_all_operations WHERE operation_id='shared'"
                )["n"], 1)


if __name__ == "__main__":
    unittest.main()
