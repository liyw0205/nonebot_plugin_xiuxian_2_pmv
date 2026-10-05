from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from flask import Flask

from nonebot_plugin_xiuxian_2.features.info.profile_application import PlayerProfileApplication
from nonebot_plugin_xiuxian_2.features.info.web import blueprint


class InfoUserSearchWebTests(unittest.TestCase):
    def _app(self, database: Path, permission) -> Flask:
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(blueprint(PlayerProfileApplication(database), permission=permission))
        return app

    def test_requires_admin_permission(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = self._app(Path(directory) / "game.db", lambda _: False)
            response = app.test_client().get("/api/v1/info/users/search")
            self.assertEqual(response.status_code, 403)
            self.assertEqual(response.json["error"]["code"], "forbidden")

    def test_returns_uniform_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', '道友')")
            app = self._app(database, lambda _: True)
            response = app.test_client().get("/api/v1/info/users/search?query=道友")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json["data"]["users"], [{"id": "u1", "name": "道友"}])
            self.assertTrue(response.json["ok"])
            self.assertIn("request_id", response.json)

    def test_overlong_query_is_validation_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = self._app(Path(directory) / "game.db", lambda _: True)
            response = app.test_client().get("/api/v1/info/users/search?query=" + "x" * 65)
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response.json["error"]["code"], "validation_error")


if __name__ == "__main__":
    unittest.main()
