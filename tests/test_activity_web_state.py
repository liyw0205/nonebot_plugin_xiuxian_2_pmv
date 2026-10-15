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

    def _client(self, database, *, permission=None, with_application: bool = True):
        web = Flask(__name__)
        web.secret_key = "test"
        application = None
        if with_application:
            application = SimpleNamespace(
                read_model=ActivityReadModelApplication(
                    database,
                    config_loader=lambda: CONFIG,
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

    def test_missing_user_id_is_rejected(self) -> None:
        client = self._client(self._database())
        response = client.get("/api/v1/activity/state")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"]["code"], "validation_error")

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
