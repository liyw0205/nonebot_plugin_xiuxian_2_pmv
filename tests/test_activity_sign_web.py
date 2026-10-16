from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot
from flask import Flask

nonebot.init()

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.activity import create_blueprint
from nonebot_plugin_xiuxian_2.features.activity.application import ActivityApplication
from nonebot_plugin_xiuxian_2.features.activity.manifest import FEATURE
from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_event_receipts,
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger


class ActivitySignWebTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            apply_activity_event_receipts(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone REAL)")
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('u1',10)")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _client(self, *, permission=None, application=None):
        web = Flask(__name__)
        web.secret_key = "test"
        web.register_blueprint(
            create_blueprint(
                application=application or ActivityApplication(self.database),
                permission=permission or (lambda required: required in {"user", "admin"}),
            )
        )
        client = web.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        return client

    @staticmethod
    def _headers(operation_id: str = "sign-1") -> dict[str, str]:
        return {"Idempotency-Key": operation_id, "X-CSRF-Token": "csrf"}

    def test_admin_route_delegates_to_existing_sign_action_and_replays(self) -> None:
        client = self._client()
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_sign",
            return_value=(True, "活动签到成功\n累计签到：3 天"),
        ) as claim_sign:
            first = client.post(
                "/api/v1/activity/sign-in", headers=self._headers(), json={"user_id": "u1"}
            )
            replay = client.post(
                "/api/v1/activity/sign-in", headers=self._headers(), json={"user_id": "u1"}
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.get_json()["data"]["status"], "applied")
        self.assertEqual(replay.status_code, 200)
        self.assertTrue(replay.get_json()["data"]["replayed"])
        claim_sign.assert_called_once()
        self.assertEqual(claim_sign.call_args.args[:2], ("u1", "sign-1"))

    def test_business_rejection_is_returned_as_conflict_without_retrying(self) -> None:
        client = self._client()
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_sign",
            return_value=(False, "今日已经领取过活动签到"),
        ) as claim_sign:
            response = client.post(
                "/api/v1/activity/sign-in", headers=self._headers(), json={"user_id": "u1"}
            )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["error"]["message"], "今日已经领取过活动签到")
        claim_sign.assert_called_once()

    def test_route_uses_real_settlement_and_does_not_grant_twice_on_replay(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service

        client = self._client()
        config = {
            "festival_name": "测试节",
            "extensions": {"sign_reply_mode": "normal"},
        }

        def parse_reward(text):
            if not text:
                return []
            return [
                {"type": "stone", "quantity": 50},
                {"type": "道具", "id": 101, "name": "签到令", "quantity": 2},
            ]

        with (
            patch.object(service, "DB_PATH", self.database),
            patch.object(service, "load_config", return_value=config),
            patch.object(
                service,
                "activity_runtime_state",
                return_value={"ok": True, "features": ["sign"]},
            ),
            patch.object(service, "today_str", return_value="2026-10-17"),
            patch.object(
                service,
                "_reward_by_day",
                return_value={"name": "首日签到", "reward": "灵石x50,签到令x2"},
            ),
            patch.object(service, "_milestone_by_days", return_value={}),
            patch.object(service, "parse_reward", side_effect=parse_reward),
            patch.object(service, "XiuConfig", return_value=SimpleNamespace(max_goods_num=100)),
            patch.object(service, "_record_sign_activity_event", return_value=[]) as record_event,
        ):
            first = client.post(
                "/api/v1/activity/sign-in",
                headers=self._headers("real-sign"),
                json={"user_id": "u1"},
            )
            replay = client.post(
                "/api/v1/activity/sign-in",
                headers=self._headers("real-sign"),
                json={"user_id": "u1"},
            )

        self.assertEqual(first.status_code, 200)
        self.assertIn("测试节签到成功", first.get_json()["data"]["data"]["message"])
        self.assertEqual(replay.status_code, 200)
        self.assertTrue(replay.get_json()["data"]["replayed"])
        record_event.assert_called_once()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(
                (1, "2026-10-17", 1),
                tuple(
                    uow.query_one(
                        "SELECT sign_days,last_sign_date,total_sign_days FROM activity_user "
                        "WHERE user_id='u1'"
                    ).values()
                ),
            )
            self.assertEqual(
                60.0,
                uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u1'")["stone"],
            )
            self.assertEqual(
                2,
                uow.query_one(
                    "SELECT goods_num FROM back WHERE user_id='u1' AND goods_id=101"
                )["goods_num"],
            )
            self.assertEqual(
                1, uow.query_one("SELECT COUNT(*) AS count FROM activity_sign_log")["count"]
            )
            self.assertEqual(
                1,
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM activity_sign_settlement_operations"
                )["count"],
            )

    def test_sign_action_recovers_started_ledger_from_business_receipt(self) -> None:
        operation_id = "interrupted-sign"
        with DatabaseUnitOfWork(self.database) as uow:
            OperationLedger().begin(
                uow,
                operation_id,
                "activity.claim_sign",
                {"user_id": "u1"},
            )
            uow.execute(
                "INSERT INTO activity_sign_settlement_operations"
                "(operation_id,payload,sign_days,total_sign_days) VALUES(?,?,?,?)",
                (operation_id, '["u1","2026-10-17",100]', 3, 6),
            )

        def recover(user_id, received_operation_id, *, settlement_repository):
            self.assertEqual(user_id, "u1")
            receipt = settlement_repository.get_result(received_operation_id)
            self.assertEqual(("duplicate", 3, 6), (receipt.status, receipt.sign_days, receipt.total_sign_days))
            return True, "签到请求已处理，无需重复发奖。"

        application = ActivityApplication(self.database)
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_sign",
            side_effect=recover,
        ) as claim_sign:
            outcome = application.execute(
                operation_id=operation_id,
                user_id="u1",
                payload={"action": "claim_sign"},
            )

        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.data["message"], "签到请求已处理，无需重复发奖。")
        claim_sign.assert_called_once()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(
                "applied",
                uow.query_one(
                    "SELECT status FROM operation_ledger WHERE operation_id=? AND action=?",
                    (operation_id, "activity.claim_sign"),
                )["status"],
            )
            self.assertEqual(
                1,
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM activity_sign_settlement_operations "
                    "WHERE operation_id=?",
                    (operation_id,),
                )["count"],
            )

    def test_missing_or_legacy_schema_fails_closed_without_creating_state(self) -> None:
        old_database = Path(self.temp.name) / "old.db"
        with DatabaseUnitOfWork(old_database) as uow:
            uow.execute(
                "CREATE TABLE activity_user(user_id TEXT PRIMARY KEY,sign_days INTEGER)"
            )
        client = self._client(application=ActivityApplication(old_database))

        response = client.post(
            "/api/v1/activity/sign-in", headers=self._headers(), json={"user_id": "u1"}
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"]["code"], "migrations_required")
        with DatabaseUnitOfWork(old_database, read_only=True) as uow:
            names = {
                row["name"]
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
        self.assertEqual(names, {"activity_user"})

        missing_database = Path(self.temp.name) / "missing.db"
        missing_client = self._client(application=ActivityApplication(missing_database))
        missing = missing_client.post(
            "/api/v1/activity/sign-in", headers=self._headers(), json={"user_id": "u1"}
        )
        self.assertEqual(missing.status_code, 503)
        self.assertFalse(missing_database.exists())

    def test_write_requires_admin_csrf_user_id_and_idempotency_key(self) -> None:
        user_client = self._client(permission=lambda required: required == "user")
        forbidden = user_client.post(
            "/api/v1/activity/sign-in", headers=self._headers(), json={"user_id": "u1"}
        )
        self.assertEqual(forbidden.status_code, 403)

        client = self._client()
        no_csrf = client.post(
            "/api/v1/activity/sign-in",
            headers={"Idempotency-Key": "sign-2"},
            json={"user_id": "u1"},
        )
        no_user_id = client.post(
            "/api/v1/activity/sign-in", headers=self._headers("sign-3"), json={"user_id": " "}
        )
        no_operation_id = client.post(
            "/api/v1/activity/sign-in",
            headers={"X-CSRF-Token": "csrf"},
            json={"user_id": "u1", "operation_id": "body-only"},
        )
        self.assertEqual(no_csrf.status_code, 403)
        self.assertEqual(no_user_id.status_code, 400)
        self.assertEqual(no_operation_id.status_code, 400)

    def test_manifest_and_admin_panel_declare_the_operator_interaction(self) -> None:
        self.assertIn(
            ("/api/v1/activity/sign-in", ("POST",), "admin"),
            [(route.path, route.methods, route.permission) for route in FEATURE.routes],
        )
        root = Path(__file__).resolve().parents[1]
        template = (root / "nonebot_plugin_xiuxian_2/adapters/web/templates/pages/admin.html").read_text()
        script = (root / "nonebot_plugin_xiuxian_2/adapters/web/static/pages/admin.js").read_text()
        self.assertIn('data-action="activity-sign" disabled', template)
        self.assertIn("/api/v1/activity/sign-in", script)
        self.assertIn("pendingActivitySign.operationId", script)
        self.assertIn("await load()", script)


if __name__ == "__main__":
    unittest.main()
