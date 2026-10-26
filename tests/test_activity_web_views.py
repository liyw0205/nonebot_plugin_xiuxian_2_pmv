"""Activity web read-only views: the four bot-side player views reachable from the panel.

The bot exposes six delivery-contracted player views (活动阶段/活动首领/活动商店/
活动背包/活动奖励/活动玩法). ``/api/v1/activity/state`` used to serve only five
(center/detail/stage/rank/boss), so the panel could not show the shop guidance,
collect-bag readiness, reward catalog or personal gameplay state. These tests pin
the four added views, their permission semantics and the read-only fallbacks.

Written as ``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
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
    "extensions": {
        "point_shop": {
            "enabled": True,
            "goods": [{"key": "g1", "name": "聚气丹", "cost": 10, "limit": 2}],
        },
        "collect_words": {
            "enabled": True,
            "groups": [{"name": "新春", "chars": ["新", "春"], "reward": "灵石x10"}],
        },
    },
    "gameplay_activities": [
        {"key": "collect", "name": "集字", "enabled": True, "commands": ["活动兑换"]},
        {"key": "point", "name": "积分", "enabled": True, "commands": ["活动积分"]},
    ],
}

# view -> a fragment the read model always renders for this configuration
VIEW_FRAGMENTS = {
    "shop": "活动商店",
    "bag": "活动背包",
    "reward": "奖励",
    "gameplay": "玩法",
}

# A narrowed configuration: the pass gameplay and the collect/point gameplay
# tables are switched off, so every added view renders on a legacy database.
LEGACY_CONFIG = {
    **CONFIG,
    "daily_tasks": [],
    "weekly_tasks": [],
    "gameplay_activities": [],
    "extensions": {"activity_pass": {"enabled": False}},
}


class ActivityWebViewsTests(unittest.TestCase):
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

    def test_each_added_view_renders_public_text(self) -> None:
        client = self._client(self._database())
        for view, fragment in VIEW_FRAGMENTS.items():
            with self.subTest(view=view):
                response = client.get(f"/api/v1/activity/state?view={view}")
                self.assertEqual(response.status_code, 200)
                data = response.get_json()["data"]
                self.assertEqual(data["view"], view)
                self.assertEqual(data["user_id"], "")
                self.assertIn(fragment, data["text"])
                self.assertTrue(data["text"].strip())

    def test_each_added_view_renders_personal_text_with_admin(self) -> None:
        client = self._client(self._database())
        for view, fragment in VIEW_FRAGMENTS.items():
            with self.subTest(view=view):
                response = client.get(
                    f"/api/v1/activity/state?user_id=u1&view={view}"
                )
                self.assertEqual(response.status_code, 200)
                data = response.get_json()["data"]
                self.assertEqual(data["view"], view)
                self.assertEqual(data["user_id"], "u1")
                self.assertIn(fragment, data["text"])

    def test_added_views_keep_the_admin_requirement_for_personal_state(self) -> None:
        client = self._client(
            self._database(), permission=lambda required: required == "user"
        )
        for view in VIEW_FRAGMENTS:
            with self.subTest(view=view):
                public = client.get(f"/api/v1/activity/state?view={view}")
                private = client.get(
                    f"/api/v1/activity/state?user_id=u1&view={view}"
                )
                self.assertEqual(public.status_code, 200)
                self.assertEqual(private.status_code, 403)
                self.assertEqual(
                    private.get_json()["error"]["code"], "forbidden"
                )

    def test_added_views_match_the_bot_read_models_verbatim(self) -> None:
        """The route must not re-render: it returns the read model text as-is."""
        database = self._database()
        client = self._client(database)
        read_model = ActivityReadModelApplication(
            database, config_loader=lambda: CONFIG, clock=SimpleNamespace(now=lambda: NOW)
        )
        expected = {
            "shop": read_model.point_shop_guidance_text("u1"),
            "bag": read_model.collect_bag_guidance_text("u1"),
            "reward": read_model.reward_catalog_text("u1"),
            "gameplay": read_model.gameplay_personal_text("u1"),
        }
        for view, text in expected.items():
            with self.subTest(view=view):
                response = client.get(
                    f"/api/v1/activity/state?user_id=u1&view={view}"
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.get_json()["data"]["text"], text)

    def test_all_nine_views_are_served(self) -> None:
        client = self._client(self._database())
        for view in (
            "center",
            "detail",
            "stage",
            "rank",
            "boss",
            "shop",
            "bag",
            "reward",
            "gameplay",
        ):
            with self.subTest(view=view):
                response = client.get(
                    f"/api/v1/activity/state?user_id=u1&view={view}"
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.get_json()["data"]["view"], view)

    def test_unknown_view_is_still_rejected(self) -> None:
        client = self._client(self._database())
        response = client.get("/api/v1/activity/state?user_id=u1&view=nope")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"]["code"], "validation_error")

    def test_added_views_without_application_keep_the_compatibility_response(self) -> None:
        client = self._client(self._database(), with_application=False)
        for view in VIEW_FRAGMENTS:
            with self.subTest(view=view):
                response = client.get(
                    f"/api/v1/activity/state?user_id=u1&view={view}"
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.get_json()["data"],
                    {"items": [], "source": "compatibility"},
                )

    def test_added_views_are_read_only_on_a_legacy_database(self) -> None:
        """A narrowed config keeps every added view readable on a legacy database."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "legacy-game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE activity_user(user_id TEXT PRIMARY KEY,sign_days INTEGER,"
                "total_sign_days INTEGER,last_sign_date TEXT)"
            )
            uow.execute("INSERT INTO activity_user VALUES('u1',3,5,'2026-10-15')")
        before = database.read_bytes()
        client = self._client(database, config=LEGACY_CONFIG)
        for view in VIEW_FRAGMENTS:
            with self.subTest(view=view):
                response = client.get(
                    f"/api/v1/activity/state?user_id=u1&view={view}"
                )
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.get_json()["data"]["text"].strip())
        self.assertEqual(database.read_bytes(), before)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            names = {
                row["name"]
                for row in uow.query_all(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertEqual(names, {"activity_user"})

    def test_added_views_fail_closed_without_the_optional_tables(self) -> None:
        """Views needing optional tables fail closed, exactly like center/detail."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "legacy-full-config.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE activity_user(user_id TEXT PRIMARY KEY,sign_days INTEGER,"
                "total_sign_days INTEGER,last_sign_date TEXT)"
            )
        before = database.read_bytes()
        client = self._client(database)
        for view in VIEW_FRAGMENTS:
            with self.subTest(view=view):
                response = client.get(
                    f"/api/v1/activity/state?user_id=u1&view={view}"
                )
                # shop only needs activity_user; bag/reward/gameplay need optional
                # tables and must refuse rather than invent data.
                if response.status_code == 200:
                    self.assertTrue(response.get_json()["data"]["text"].strip())
                else:
                    self.assertEqual(response.status_code, 503)
                    self.assertEqual(
                        response.get_json()["error"]["code"], "schema_missing"
                    )
        self.assertEqual(database.read_bytes(), before)

    def test_added_views_without_database_do_not_create_one(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "missing-game.db"
        client = self._client(database)
        for view in VIEW_FRAGMENTS:
            with self.subTest(view=view):
                response = client.get(f"/api/v1/activity/state?view={view}")
                self.assertEqual(response.status_code, 200)
        self.assertFalse(database.exists())

    def test_panel_offers_every_view_the_route_serves(self) -> None:
        """The panel selector must not lag behind the route's view set."""
        from nonebot_plugin_xiuxian_2.adapters.web.blueprints.pages import (
            create_blueprint as create_pages_blueprint,
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
            create_pages_blueprint(permission=lambda required: required == "admin")
        )
        response = web.test_client().get("/pages/activity")
        self.assertEqual(response.status_code, 200)
        for view in (
            "center",
            "detail",
            "stage",
            "rank",
            "boss",
            "shop",
            "bag",
            "reward",
            "gameplay",
        ):
            with self.subTest(view=view):
                self.assertIn(
                    f'value="{view}"'.encode(), response.data
                )

    def test_panel_view_options_cover_the_route_mapping(self) -> None:
        """Every option value the panel offers must resolve on the state route."""
        template = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2/adapters/web/templates/pages/admin.html"
        ).read_text(encoding="utf-8")
        import re

        block = template.split("data-activity-view", 1)[1].split("</select>", 1)[0]
        options = re.findall(r'value="([a-z]+)"', block)
        self.assertEqual(
            options,
            [
                "center",
                "detail",
                "stage",
                "rank",
                "boss",
                "shop",
                "bag",
                "reward",
                "gameplay",
            ],
        )
        client = self._client(self._database())
        for view in options:
            with self.subTest(view=view):
                response = client.get(
                    f"/api/v1/activity/state?user_id=u1&view={view}"
                )
                self.assertNotEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
