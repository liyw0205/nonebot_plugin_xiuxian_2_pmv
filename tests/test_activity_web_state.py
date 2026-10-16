"""Activity web state route: read-only activity state for the panel.

The panel page for 活动 is backed by ``/api/v1/activity/state``; claiming keeps
its own write route. Written as ``unittest.TestCase`` because the CI quality job
collects tests with ``python -m unittest discover``.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.activity import (
    create_blueprint,
)
from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

NOW = datetime(2026, 10, 15, 12)
CONFIG = {
    "template_key": "spring",
    "enabled": True,
    "name": "春日庆典",
    "start_time": "0",
    "end_time": "无限",
    "extensions": {},
}


class ActivityWebStateTests(unittest.TestCase):
    def _database(self) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_activity_state_schema(uow)
        return database

    def _client(
        self,
        database,
        *,
        permission=None,
        config=CONFIG,
        with_application: bool = True,
    ):
        web = Flask(__name__)
        web.secret_key = "test"
        application = None
        if with_application:
            application = SimpleNamespace(
                read_model=ActivityReadModelApplication(
                    database,
                    config_loader=lambda: config,
                    clock=SimpleNamespace(now=lambda: NOW),
                )
            )
        web.register_blueprint(
            create_blueprint(
                context=None,
                application=application,
                permission=permission or (lambda _required: True),
            )
        )
        return web.test_client()

    def test_center_view_returns_the_card_text(self) -> None:
        client = self._client(self._database())
        response = client.get("/api/v1/activity/state?user_id=u1")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()["data"]
        self.assertEqual(data["view"], "center")
        self.assertEqual(data["user_id"], "u1")
        self.assertIn("活动中心", data["text"])

    def test_stage_rank_and_boss_views(self) -> None:
        client = self._client(self._database())
        for view in ("stage", "rank", "boss"):
            response = client.get(f"/api/v1/activity/state?user_id=u1&view={view}")
            self.assertEqual(response.status_code, 200, view)
            data = response.get_json()["data"]
            self.assertEqual(data["view"], view)
            self.assertTrue(data["text"].strip())

    def test_detail_view_uses_the_configured_activity_info_read_model(self) -> None:
        client = self._client(self._database())
        response = client.get("/api/v1/activity/state?view=detail")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()["data"]
        self.assertEqual(data["view"], "detail")
        self.assertIn("春日庆典", data["text"])
        self.assertIn("状态：进行中", data["text"])
        self.assertIn("活动时间：", data["text"])

    def test_personal_progress_requires_admin_but_public_state_does_not(self) -> None:
        client = self._client(
            self._database(),
            permission=lambda required: required == "user",
        )
        public = client.get("/api/v1/activity/state?view=detail")
        private = client.get("/api/v1/activity/state?view=detail&user_id=u1")
        self.assertEqual(public.status_code, 200)
        self.assertEqual(private.status_code, 403)
        self.assertEqual(private.get_json()["error"]["code"], "forbidden")

    def test_detail_with_minimal_legacy_activity_state_stays_read_only(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "legacy-game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE activity_user(user_id TEXT PRIMARY KEY,sign_days INTEGER,"
                "total_sign_days INTEGER,last_sign_date TEXT)"
            )
            uow.execute(
                "INSERT INTO activity_user VALUES('u1',3,5,'2026-10-15')"
            )
        config = {
            **CONFIG,
            "daily_tasks": [],
            "weekly_tasks": [],
            "gameplay_activities": [],
            "extensions": {"activity_pass": {"enabled": False}},
        }

        response = self._client(database, config=config).get(
            "/api/v1/activity/state?view=detail&user_id=u1"
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("我的累计签到：3 天", response.get_json()["data"]["text"])
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            names = {
                row["name"]
                for row in uow.query_all(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertEqual(names, {"activity_user"})

    def test_detail_without_database_does_not_create_one(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "missing-game.db"
        response = self._client(database).get("/api/v1/activity/state?view=detail")
        self.assertEqual(response.status_code, 200)
        self.assertIn("春日庆典", response.get_json()["data"]["text"])
        self.assertFalse(database.exists())

    def test_missing_user_id_returns_the_card_without_personal_progress(self) -> None:
        """The panel fetches this endpoint without parameters."""
        client = self._client(self._database())
        response = client.get("/api/v1/activity/state")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()["data"]
        self.assertEqual(data["view"], "center")
        self.assertEqual(data["user_id"], "")
        self.assertIn("活动中心", data["text"])
        self.assertNotIn("进度：", data["text"])

    def test_panel_page_endpoints_point_at_the_state_route(self) -> None:
        from nonebot_plugin_xiuxian_2.adapters.web.blueprints.pages import (
            PAGE_ENDPOINTS,
        )

        self.assertEqual(PAGE_ENDPOINTS["activity"], "/api/v1/activity/state")
        self.assertEqual(PAGE_ENDPOINTS["reward_center"], "/api/v1/activity/state")

    def test_activity_panel_exposes_views_and_optional_player_lookup(self) -> None:
        from nonebot_plugin_xiuxian_2.adapters.web.blueprints.pages import (
            create_blueprint,
        )

        web = Flask(
            __name__,
            template_folder=str(
                Path(__file__).resolve().parents[1]
                / "nonebot_plugin_xiuxian_2/adapters/web/templates"
            ),
        )
        web.secret_key = "test"
        web.jinja_env.globals["csrf_token"] = lambda: "test-token"
        web.register_blueprint(
            create_blueprint(permission=lambda required: required == "admin")
        )
        response = web.test_client().get("/pages/activity")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"data-activity-view", response.data)
        self.assertIn(b"value=\"detail\"", response.data)
        self.assertIn(b"data-activity-user-id", response.data)
        self.assertIn(b"data-activity-controls", response.data)

    def test_unknown_view_is_rejected(self) -> None:
        client = self._client(self._database())
        response = client.get("/api/v1/activity/state?user_id=u1&view=nope")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"]["code"], "validation_error")

    def test_permission_denied(self) -> None:
        client = self._client(self._database(), permission=lambda _required: False)
        response = client.get("/api/v1/activity/state?user_id=u1")
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error"]["code"], "forbidden")

    def test_missing_application_keeps_the_compatibility_response(self) -> None:
        client = self._client(self._database(), with_application=False)
        response = client.get("/api/v1/activity/state?user_id=u1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json()["data"], {"items": [], "source": "compatibility"}
        )

    def test_audit_route_still_answers(self) -> None:
        client = self._client(self._database())
        response = client.get("/api/v1/activity")
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            response.get_json()["data"]["source"], {"compatibility", "web_audit"}
        )


if __name__ == "__main__":
    unittest.main()
