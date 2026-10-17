from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.activity_reward import create_blueprint
from nonebot_plugin_xiuxian_2.features.activity_reward.application import ActivityRewardApplication
from nonebot_plugin_xiuxian_2.features.activity_reward.claim_all_application import ActivityClaimAllApplication
from nonebot_plugin_xiuxian_2.features.activity_reward.manifest import FEATURE
from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import apply_activity_claim_all
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger


ROOT = Path(__file__).resolve().parents[1]


class ActivityRewardWebTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _create_schema(self, database: Path | None = None) -> Path:
        database = database or self.database
        with DatabaseUnitOfWork(database) as uow:
            OperationLedger().ensure_schema(uow)
            apply_activity_claim_all(uow)
        return database

    @staticmethod
    def _client(application, *, permission=None):
        web = Flask(__name__)
        web.secret_key = "test"
        web.register_blueprint(
            create_blueprint(
                application=application,
                permission=permission or (lambda required: required == "admin"),
            )
        )
        client = web.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        return client

    @staticmethod
    def _headers(operation_id: str = "claim-1") -> dict[str, str]:
        return {"Idempotency-Key": operation_id, "X-CSRF-Token": "csrf"}

    def _runner_factory(self, calls, runner):
        def factory(user_id):
            self.assertEqual(user_id, "u1")
            return {
                name: (lambda child_id, step=name: runner(step, child_id))
                for name in ActivityClaimAllApplication.step_names()
            }

        return factory

    def test_permission_and_csrf_are_checked_before_claim_or_database_access(self) -> None:
        class Application:
            def __init__(self, database):
                self.database = str(database)

            def claim_all(self, **kwargs):
                raise AssertionError("permission/CSRF failures must not reach the application")

        missing_database = Path(self.temp.name) / "never-created.db"
        client = self._client(Application(missing_database), permission=lambda _required: False)
        denied = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers(),
            json={"user_id": "u1"},
        )
        self.assertEqual(denied.status_code, 403)
        self.assertEqual(denied.get_json()["error"]["code"], "forbidden")
        self.assertFalse(missing_database.exists())

        self._create_schema()
        client = self._client(Application(self.database), permission=lambda required: required == "admin")
        csrf_missing = client.post(
            "/api/v1/activity/rewards/claim",
            headers={"Idempotency-Key": "csrf-claim"},
            json={"user_id": "u1"},
        )
        self.assertEqual(csrf_missing.status_code, 403)
        self.assertEqual(csrf_missing.get_json()["error"]["code"], "csrf_failed")

    def test_invalid_target_and_operation_are_rejected_before_claim(self) -> None:
        self._create_schema()
        calls = []
        app = ActivityRewardApplication(
            self.database,
            runners_factory=self._runner_factory(calls, lambda *_: (False, "none")),
        )
        client = self._client(app)

        missing_user = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers(),
            json={"user_id": " "},
        )
        missing_key = client.post(
            "/api/v1/activity/rewards/claim",
            headers={"X-CSRF-Token": "csrf"},
            json={"user_id": "u1"},
        )
        self.assertEqual(missing_user.status_code, 400)
        self.assertEqual(missing_user.get_json()["error"]["code"], "validation_error")
        self.assertEqual(missing_key.status_code, 400)
        self.assertEqual(calls, [])

    def test_missing_database_and_old_schema_fail_closed_without_request_ddl(self) -> None:
        missing_database = Path(self.temp.name) / "missing.db"
        app = ActivityRewardApplication(missing_database)
        client = self._client(app)
        missing = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers(),
            json={"user_id": "u1"},
        )
        self.assertEqual(missing.status_code, 503)
        self.assertEqual(missing.get_json()["error"]["code"], "migrations_required")
        self.assertFalse(missing_database.exists())

        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE operation_ledger(operation_id TEXT)")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            before = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
        old_schema = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("old-schema"),
            json={"user_id": "u1"},
        )
        self.assertEqual(old_schema.status_code, 503)
        self.assertEqual(old_schema.get_json()["error"]["code"], "migrations_required")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            after = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
        self.assertEqual(after, before)

    def test_claim_completes_six_steps_and_replays_without_running_them_again(self) -> None:
        self._create_schema()
        calls = []

        def run(step, child_id):
            calls.append((step, child_id))
            return (step == "tasks", "任务奖励" if step == "tasks" else "暂无")

        app = ActivityRewardApplication(
            self.database, runners_factory=self._runner_factory(calls, run)
        )
        client = self._client(app)
        first = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("six-steps"),
            json={"user_id": "u1"},
        )
        replay = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("six-steps"),
            json={"user_id": "u1"},
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.get_json()["data"]["data"]["text"], "任务奖励")
        self.assertEqual(replay.status_code, 200)
        self.assertTrue(replay.get_json()["data"]["replayed"])
        self.assertEqual(len(calls), 6)
        self.assertEqual(
            [operation_id for _, operation_id in calls],
            [
                "six-steps:tasks",
                "six-steps:pass",
                "six-steps:boss-milestone",
                "six-steps:boss-rank",
                "six-steps:stage",
                "six-steps:rank",
            ],
        )
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            ledger = uow.query_one(
                "SELECT status FROM operation_ledger WHERE operation_id='six-steps'"
            )
            claim = uow.query_one(
                "SELECT status FROM activity_claim_all_operations WHERE operation_id='six-steps'"
            )
            steps = uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_claim_all_steps "
                "WHERE operation_id='six-steps' AND status='completed'"
            )
            audits = uow.query_one(
                "SELECT COUNT(*) AS count FROM operation_audit WHERE operation_id='six-steps'"
            )
        self.assertEqual(ledger["status"], "applied")
        self.assertEqual(claim["status"], "completed")
        self.assertEqual(steps["count"], 6)
        self.assertEqual(audits["count"], 1)

    def test_operation_id_cannot_be_reused_for_another_player(self) -> None:
        self._create_schema()
        calls = []

        def run(step, child_id):
            calls.append((step, child_id))
            return True, "奖励已领取"

        app = ActivityRewardApplication(
            self.database,
            runners_factory=self._runner_factory(calls, run),
        )
        client = self._client(app)
        first = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("conflict-claim"),
            json={"user_id": "u1"},
        )
        conflict = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("conflict-claim"),
            json={"user_id": "u2"},
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.get_json()["error"]["code"], "conflict")
        self.assertEqual(len(calls), 6)

    def test_retryable_partial_failure_reuses_completed_step_receipt(self) -> None:
        self._create_schema()
        calls = []
        pass_attempts = 0

        def run(step, child_id):
            nonlocal pass_attempts
            calls.append((step, child_id))
            if step == "pass":
                pass_attempts += 1
                if pass_attempts == 1:
                    raise RuntimeError("temporary failure")
            return (step == "tasks", "任务奖励" if step == "tasks" else "暂无")

        app = ActivityRewardApplication(
            self.database, runners_factory=self._runner_factory(calls, run)
        )
        client = self._client(app)
        first = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("retry-claim"),
            json={"user_id": "u1"},
        )
        resumed = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("retry-claim"),
            json={"user_id": "u1"},
        )
        replay = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("retry-claim"),
            json={"user_id": "u1"},
        )

        self.assertEqual(first.status_code, 409)
        self.assertEqual(first.get_json()["error"]["code"], "retryable_failure")
        self.assertEqual(resumed.status_code, 200)
        self.assertEqual(resumed.get_json()["data"]["data"]["text"], "任务奖励")
        self.assertEqual(replay.status_code, 200)
        self.assertTrue(replay.get_json()["data"]["replayed"])
        self.assertEqual([step for step, _ in calls].count("tasks"), 1)
        self.assertEqual([step for step, _ in calls].count("pass"), 2)
        self.assertEqual(len(calls), 7)

    def test_no_claimable_rewards_is_a_stable_structured_rejection(self) -> None:
        self._create_schema()
        calls = []
        app = ActivityRewardApplication(
            self.database,
            runners_factory=self._runner_factory(
                calls, lambda step, child_id: (calls.append((step, child_id)), (False, "暂无"))[1]
            ),
        )
        client = self._client(app)
        first = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("no-rewards"),
            json={"user_id": "u1"},
        )
        replay = client.post(
            "/api/v1/activity/rewards/claim",
            headers=self._headers("no-rewards"),
            json={"user_id": "u1"},
        )

        self.assertEqual(first.status_code, 409)
        self.assertEqual(first.get_json()["error"]["code"], "not_claimable")
        self.assertEqual(replay.status_code, 409)
        self.assertEqual(replay.get_json()["error"]["code"], "not_claimable")
        self.assertEqual(len(calls), 6)

    def test_legacy_operation_id_body_fallback_remains_compatible(self) -> None:
        self._create_schema()
        calls = []
        app = ActivityRewardApplication(
            self.database,
            runners_factory=self._runner_factory(calls, lambda *_: (False, "暂无")),
        )
        client = self._client(app)
        response = client.post(
            "/api/v1/activity/rewards/claim",
            headers={"X-CSRF-Token": "csrf"},
            json={"user_id": "u1", "operation_id": "legacy-body-op"},
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["error"]["details"]["operation_id"], "legacy-body-op")

    def test_manifest_and_activity_panel_expose_only_the_admin_claim_workflow(self) -> None:
        route = next(route for route in FEATURE.routes if route.path == "/api/v1/activity/rewards/claim")
        self.assertEqual(route.permission, "admin")
        template = (ROOT / "nonebot_plugin_xiuxian_2/adapters/web/templates/pages/admin.html").read_text(encoding="utf-8")
        script = (ROOT / "nonebot_plugin_xiuxian_2/adapters/web/static/pages/admin.js").read_text(encoding="utf-8")
        self.assertIn('data-action="activity-claim" disabled', template)
        self.assertIn("/api/v1/activity/rewards/claim", script)
        self.assertIn("pendingActivityClaim.operationId", script)
        self.assertIn("pendingActivityClaim.userId !== activityUserId.value.trim()", script)
        self.assertIn("error.code === 'retryable_failure'", script)
        self.assertIn("activityClaim.disabled = !userSelected", script)
        self.assertIn("activityUserId.disabled = busy", script)
        self.assertIn("activityView.disabled = busy", script)
        self.assertIn("await refreshActivityState(message)", script)
        self.assertIn("refreshActivityState", script)


if __name__ == "__main__":
    unittest.main()
