from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import (
    service as activity_service,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.config_event_service import (
    ActivityConfigEventService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import activity as web_activity
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core as web_core
from tests.test_db_backend import db_backend


class ActivityConfigEventServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "activity.db"
        self.service = ActivityConfigEventService(self.database)
        self.base_config = {
            "enabled": True,
            "name": "节日活动",
            "gameplay_activities": [
                {
                    "key": "collect",
                    "name": "集字",
                    "type": "collect_words",
                    "enabled": True,
                },
                {
                    "key": "points",
                    "name": "积分",
                    "type": "event_points",
                    "enabled": True,
                },
            ],
            "extensions": {"activity_pass": {"enabled": True}},
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_imports_legacy_config_once_as_database_truth(self) -> None:
        first = self.service.load_or_import(self.base_config)
        changed_legacy = {**self.base_config, "name": "外部 JSON 修改"}
        restarted = ActivityConfigEventService(self.database)
        second = restarted.load_or_import(changed_legacy)

        self.assertEqual((first.revision, first.config["name"]), (1, "节日活动"))
        self.assertEqual((second.revision, second.config["name"]), (1, "节日活动"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM activity_config_state"), 1)

    def test_replace_versions_config_and_replays_first_snapshot(self) -> None:
        state = self.service.load_or_import(self.base_config)
        first_config = {**state.config, "enabled": False}
        first = self.service.replace(
            "config:message-1:admin",
            {"action": "toggle", "enabled": False, "operator": "admin"},
            state.revision,
            first_config,
            result_text="已关闭签到活动",
        )
        later_config = {**first.config, "name": "后来更新"}
        later = self.service.replace(
            "config:web-2:admin",
            {"action": "replace", "config": later_config, "operator": "admin"},
            first.revision,
            later_config,
            result_text="活动配置已保存",
        )
        replay = self.service.replace(
            "config:message-1:admin",
            {"action": "toggle", "enabled": False, "operator": "admin"},
            later.revision,
            {**later.config, "enabled": True},
            result_text="不应覆盖首次结果",
        )

        self.assertEqual((first.status, first.revision), ("applied", 2))
        self.assertEqual((later.status, later.revision), ("applied", 3))
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(replay.revision, 2)
        self.assertEqual(replay.config, first.config)
        self.assertEqual(replay.result_text, "已关闭签到活动")
        current = self.service.load_or_import(self.base_config)
        self.assertEqual((current.revision, current.config["name"]), (3, "后来更新"))

    def test_rejects_operation_conflict_and_stale_revision(self) -> None:
        state = self.service.load_or_import(self.base_config)
        applied = self.service.replace(
            "config:same",
            {"enabled": False},
            state.revision,
            {**state.config, "enabled": False},
        )

        conflict = self.service.replay("config:same", {"enabled": True})
        stale = self.service.replace(
            "config:stale",
            {"enabled": True},
            state.revision,
            {**state.config, "enabled": True},
        )

        self.assertEqual(applied.status, "applied")
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual((stale.status, stale.revision), ("state_changed", 2))
        self.assertFalse(stale.config["enabled"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM activity_config_operations"), 1)

    def test_unchanged_result_is_recorded_and_replayed(self) -> None:
        state = self.service.load_or_import(self.base_config)
        first = self.service.replace(
            "config:unchanged",
            {"action": "toggle", "target": "missing"},
            state.revision,
            state.config,
            result_text="未找到活动：missing",
        )
        replay = self.service.replay(
            "config:unchanged", {"action": "toggle", "target": "missing"}
        )

        self.assertEqual((first.status, first.revision), ("unchanged", 1))
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(replay.result_text, "未找到活动：missing")

    def test_operation_failure_rolls_back_config_version(self) -> None:
        state = self.service.load_or_import(self.base_config)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_activity_config_operation BEFORE INSERT "
                "ON activity_config_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.replace(
                "config:failed",
                {"enabled": False},
                state.revision,
                {**state.config, "enabled": False},
            )

        current = self.service.load_or_import(self.base_config)
        self.assertEqual((current.revision, current.config["enabled"]), (1, True))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM activity_config_operations"), 0)

    def test_production_toggle_uses_versioned_event_service(self) -> None:
        def load_state():
            return self.service.load_or_import(self.base_config)

        def replay(operation_id, request_identity):
            return self.service.replay(operation_id, request_identity)

        def save(config, **kwargs):
            return self.service.replace(
                kwargs["operation_id"],
                kwargs["request_identity"],
                kwargs["expected_revision"],
                config,
                result_text=kwargs["result_text"],
            )

        with (
            patch.object(activity_service, "load_config_state", load_state),
            patch.object(activity_service, "replay_config_event", replay),
            patch.object(activity_service, "save_config", save),
        ):
            first_text = activity_service.set_enabled(
                False,
                "集字",
                operation_id="activity:config-close:admin:message-1",
                operator_id="admin",
            )
            duplicate_text = activity_service.set_enabled(
                False,
                "集字",
                operation_id="activity:config-close:admin:message-1",
                operator_id="admin",
            )
            conflict_text = activity_service.set_enabled(
                False,
                "积分",
                operation_id="activity:config-close:admin:message-1",
                operator_id="admin",
            )

        current = self.service.load_or_import(self.base_config)
        enabled = {
            activity["key"]: activity["enabled"]
            for activity in current.config["gameplay_activities"]
        }
        self.assertEqual(first_text, "已关闭1个集字")
        self.assertEqual(duplicate_text, first_text)
        self.assertEqual(conflict_text, "同一消息事件不能用于不同的活动配置操作")
        self.assertEqual(enabled, {"collect": False, "points": True})

    def test_production_sources_have_no_direct_config_write_bypass(self) -> None:
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        config_source = (root / "xiuxian_activity/activity_config.py").read_text(
            encoding="utf-8"
        )
        service_source = (root / "xiuxian_activity/service.py").read_text(
            encoding="utf-8"
        )
        command_source = (root / "xiuxian_activity/__init__.py").read_text(
            encoding="utf-8"
        )
        web_source = (root / "xiuxian_web/activity.py").read_text(encoding="utf-8")
        template_source = (root / "xiuxian_web/templates/activity.html").read_text(
            encoding="utf-8"
        )

        self.assertIn("activity_config_event_service.load_or_import(", config_source)
        self.assertIn("activity_config_event_service.replace(", config_source)
        self.assertIn("expected_revision=state.revision", service_source)
        self.assertIn('operation_id=_activity_operation_id(event, "config-open"', command_source)
        self.assertIn("expected_revision=expected_revision", web_source)
        self.assertIn("expected_revision: activityConfigRevision", template_source)

    def test_web_post_passes_operation_revision_and_operator(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="activity-config-test")
        client = app.test_client()
        with client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"

        captured = {}

        def save(config, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                status="applied",
                succeeded=True,
                revision=4,
                config=config,
                result_text="活动配置已保存",
            )

        normalized = {
            **self.base_config,
            "daily_rewards": [{"day": 1, "reward": "灵石x1"}],
        }
        with (
            patch.object(web_core, "ADMIN_IDS", {"admin-1"}),
            patch.object(web_activity, "_normalize_activity_config", return_value=normalized),
            patch.object(web_activity, "save_activity_config", save),
            patch.object(web_activity, "activity_state", return_value=(True, "")),
            patch.object(web_activity, "activity_runtime_state", return_value={}),
        ):
            response = client.post(
                "/api/activity/config",
                json={
                    "config": normalized,
                    "operation_id": "activity-config-web:request-1",
                    "expected_revision": 3,
                },
                headers={"X-CSRF-Token": "csrf-token"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["config_revision"], 4)
        self.assertEqual(captured["operation_id"], "activity-config-web:request-1")
        self.assertEqual(captured["expected_revision"], 3)
        self.assertEqual(captured["request_identity"]["operator_id"], "admin-1")


if __name__ == "__main__":
    unittest.main()
