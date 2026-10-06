from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import (
    activity_config,
    service as activity_service,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.config_event_service import (
    ActivityConfigEventService,
)
from nonebot_plugin_xiuxian_2.features.activity.config_application import (
    ActivityConfigApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.config_repository import (
    ActivityConfigSqlRepository,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import activity as web_activity
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core as web_core
from tests.test_db_backend import db_backend


class ActivityConfigEventServiceTests(unittest.TestCase):
    def test_activity_config_defers_event_service_construction(self) -> None:
        self.assertIsNone(activity_config._activity_config_event_service_instance)

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

    def test_read_state_does_not_create_database_or_state_schema(self) -> None:
        missing_database = Path(self.temp.name) / "not-created.db"
        reader = ActivityConfigEventService(missing_database)

        self.assertIsNone(reader.read_state())
        self.assertFalse(missing_database.exists())

        self.service.load_or_import(self.base_config)
        state = self.service.read_state()
        self.assertEqual((state.revision, state.config["name"]), (1, "节日活动"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM activity_config_operations"), 0)

    def test_replay_lookup_does_not_create_missing_database_or_schema(self) -> None:
        missing_database = Path(self.temp.name) / "not-created-for-replay.db"
        reader = ActivityConfigEventService(missing_database)

        self.assertIsNone(reader.replay("missing-operation", {"action": "toggle"}))
        self.assertFalse(missing_database.exists())

        with db_backend.connection(self.database) as conn:
            conn.execute("CREATE TABLE unrelated(value TEXT)")
        self.assertIsNone(
            self.service.replay("missing-operation", {"action": "toggle"})
        )
        self.assertEqual(
            self.scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                "AND name LIKE 'activity_config_%'"
            ),
            0,
        )

    def test_regular_config_read_prefers_event_state_without_legacy_import(self) -> None:
        config_path = Path(self.temp.name) / "activity_config.json"
        config_path.write_text('{"name":"陈旧 JSON","enabled":false}', encoding="utf-8")
        state_database = Path(self.temp.name) / "activity.db"
        service = ActivityConfigEventService(state_database)
        service.load_or_import(self.base_config)

        with (
            patch.object(activity_config, "CONFIG_PATH", config_path),
            patch.object(activity_config, "_activity_config_event_service", return_value=service),
        ):
            config = activity_config.load_config()

        self.assertEqual(config["name"], "节日活动")
        with db_backend.connection(state_database) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM activity_config_operations").fetchone()[0],
                0,
            )

    def test_regular_config_read_with_missing_state_is_read_only(self) -> None:
        config_path = Path(self.temp.name) / "missing" / "activity_config.json"
        database = Path(self.temp.name) / "missing" / "activity.db"
        service = ActivityConfigEventService(database)
        with (
            patch.object(activity_config, "CONFIG_PATH", config_path),
            patch.object(activity_config, "_activity_config_event_service", return_value=service),
        ):
            config = activity_config.load_config()

        self.assertEqual(config["template_type"], "festival_sign")
        self.assertFalse(database.exists())
        self.assertFalse(config_path.parent.exists())

    def test_read_config_state_returns_legacy_projection_without_import(self) -> None:
        config_path = Path(self.temp.name) / "missing" / "activity_config.json"
        database = Path(self.temp.name) / "missing" / "activity.db"
        service = ActivityConfigEventService(database)
        with (
            patch.object(activity_config, "CONFIG_PATH", config_path),
            patch.object(activity_config, "_activity_config_event_service", return_value=service),
        ):
            state = activity_config.read_config_state()

        self.assertEqual(state.revision, 0)
        self.assertEqual(state.config["template_type"], "festival_sign")
        self.assertFalse(database.exists())
        self.assertFalse(config_path.parent.exists())

    def test_replace_initializes_state_from_read_only_revision(self) -> None:
        result = self.service.replace(
            "config:first-save",
            {"action": "replace", "operator_id": "admin-1"},
            0,
            self.base_config,
        )

        self.assertEqual((result.status, result.revision), ("applied", 1))
        self.assertEqual(result.config, self.base_config)
        replay = self.service.replace(
            "config:first-save",
            {"action": "replace", "operator_id": "admin-1"},
            0,
            self.base_config,
        )
        self.assertEqual((replay.status, replay.revision), ("duplicate", 1))

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
        activity_config_application_source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/features/activity/config_application.py"
        ).read_text(encoding="utf-8")
        command_source = (root / "xiuxian_activity/__init__.py").read_text(
            encoding="utf-8"
        )
        web_source = (root / "xiuxian_web/activity.py").read_text(encoding="utf-8")
        template_source = (root / "xiuxian_web/templates/activity.html").read_text(
            encoding="utf-8"
        )

        self.assertIn("_activity_config_event_service().load_or_import(", config_source)
        self.assertIn("_activity_config_event_service().replace(", config_source)
        self.assertIn("_activity_config_event_service_instance = None", config_source)
        self.assertIn("def _activity_config_event_service(", config_source)
        self.assertNotIn("activity_config_event_service.replace(", config_source)
        self.assertIn("expected_revision=state.revision", service_source)
        self.assertIn('operation_id=_activity_operation_id(event, "config-open"', command_source)
        self.assertIn("def read(self) -> ActivityConfigState:", activity_config_application_source)
        self.assertIn("def replace(", activity_config_application_source)
        self.assertIn("_activity_config_application().read()", web_source)
        self.assertIn("_activity_config_application().replace(", web_source)
        self.assertNotIn("save_activity_config", web_source)
        self.assertIn("expected_revision=expected_revision", web_source)
        self.assertIn("expected_revision: activityConfigRevision", template_source)

    def test_web_post_passes_operation_revision_and_operator(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="activity-config-test")
        client = app.test_client()
        with client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"

        captured = {}

        def replace(**kwargs):
            captured.update(kwargs)
            config = kwargs["config"]
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
            patch.object(
                web_activity,
                "_activity_config_application",
                return_value=SimpleNamespace(replace=replace),
            ),
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
        self.assertEqual(captured["operator_id"], "admin-1")

    def test_web_config_and_management_page_read_through_application(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="activity-config-test")
        client = app.test_client()
        with client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"

        config_path = Path(self.temp.name) / "missing" / "activity_config.json"
        database = Path(self.temp.name) / "missing" / "activity.db"
        service = ActivityConfigEventService(database)
        repository = ActivityConfigSqlRepository(
            database,
            event_service=service,
            config_loader=activity_config._load_default_config,
            projection_writer=lambda _config: None,
        )
        application = ActivityConfigApplication(repository)
        with (
            patch.object(web_core, "ADMIN_IDS", {"admin-1"}),
            patch.object(
                web_activity,
                "_activity_config_application",
                return_value=application,
            ) as read_state,
            patch.object(web_activity, "activity_state", return_value=(True, "")),
            patch.object(web_activity, "activity_runtime_state", return_value={}),
        ):
            response = client.get("/api/activity/config")
            page = client.get("/activity")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(page.status_code, 200)
            self.assertEqual(response.get_json()["config_revision"], 0)
            self.assertFalse(database.exists())
            self.assertFalse(config_path.parent.exists())
            self.assertEqual(read_state.call_count, 2)

            payload = {
                "config": response.get_json()["config"],
                "operation_id": "activity-config-web:first-save",
                "expected_revision": 0,
            }
            headers = {"X-CSRF-Token": "csrf-token"}
            saved = client.post("/api/activity/config", json=payload, headers=headers)
            replay = client.post("/api/activity/config", json=payload, headers=headers)
            changed_config = {**payload["config"], "name": "different config"}
            conflict = client.post(
                "/api/activity/config",
                json={**payload, "config": changed_config},
                headers=headers,
            )
            stale = client.post(
                "/api/activity/config",
                json={**payload, "operation_id": "activity-config-web:stale", "expected_revision": 0},
                headers=headers,
            )

        self.assertEqual(saved.status_code, 200)
        self.assertEqual(saved.get_json()["config_revision"], 1)
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(replay.get_json()["config_revision"], 1)
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(stale.get_json()["config_revision"], 1)
        with db_backend.connection(database) as conn:
            receipt_count = conn.execute(
                "SELECT COUNT(*) FROM activity_config_operations"
            ).fetchone()[0]
        self.assertEqual(receipt_count, 1)
        self.assertFalse((Path(self.temp.name) / "xiuxian.db").exists())

    def test_web_config_post_requires_csrf_before_application(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="activity-config-test")
        client = app.test_client()
        with client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"

        application = SimpleNamespace(replace=lambda **_kwargs: self.fail("application called"))
        with (
            patch.object(web_core, "ADMIN_IDS", {"admin-1"}),
            patch.object(web_activity, "_activity_config_application", return_value=application),
        ):
            missing = client.post("/api/activity/config", json={"operation_id": "missing-csrf"})
            wrong = client.post(
                "/api/activity/config",
                json={"operation_id": "wrong-csrf"},
                headers={"X-CSRF-Token": "wrong"},
            )

        self.assertEqual(missing.status_code, 403)
        self.assertEqual(wrong.status_code, 403)

    def test_static_template_routes_remain_read_only(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="activity-config-test")
        client = app.test_client()
        with client.session_transaction() as session:
            session["admin_id"] = "admin-1"

        with patch.object(web_core, "ADMIN_IDS", {"admin-1"}):
            activity = client.get("/api/activity/template/festival_sign")
            gameplay = client.get("/api/activity/gameplay-template/duanwu_collect_words")
            missing = client.get("/api/activity/template/not-a-template")

        self.assertEqual(activity.status_code, 200)
        self.assertTrue(activity.get_json()["success"])
        self.assertEqual(gameplay.status_code, 200)
        self.assertTrue(gameplay.get_json()["success"])
        self.assertFalse(missing.get_json()["success"])


if __name__ == "__main__":
    unittest.main()
