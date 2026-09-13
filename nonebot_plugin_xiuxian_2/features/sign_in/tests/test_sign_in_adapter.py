from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from flask import Flask

from ....adapters.web.app import create_app
from ....bootstrap import build_runtime_context
from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SignInApplication
from ..web import blueprint


class SignInAdapterTests(unittest.TestCase):
    def test_post_requires_csrf_and_returns_uniform_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(blueprint(SignInApplication(Path(directory) / "game.db"), permission=lambda _: True))
            with app.test_client() as client:
                response = client.post("/api/v1/sign-in", json={"user_id": "u"})
                self.assertEqual(response.status_code, 403)
                self.assertFalse(response.json["ok"])
                token = client.get("/api/v1/csrf")
                self.assertEqual(token.status_code, 404)

    def test_validation_error_is_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(blueprint(SignInApplication(Path(directory) / "game.db"), permission=lambda _: True))
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["_csrf_token"] = "token"
                response = client.post(
                    "/api/v1/sign-in",
                    headers={"X-CSRF-Token": "token"},
                    json={"user_id": "", "operation_id": "op"},
                )
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.json["error"]["code"], "validation_error")

    def test_runtime_app_accepts_csrf_protected_claim(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            context = build_runtime_context(data_dir=directory)
            with DatabaseUnitOfWork(context.database.path("game_db")) as uow:
                uow.execute("CREATE TABLE user_xiuxian (user_id TEXT, is_sign INTEGER DEFAULT 0, stone INTEGER DEFAULT 0)")
                uow.execute("INSERT INTO user_xiuxian(user_id, is_sign, stone) VALUES (?, 0, 0)", ("u1",))
            app = create_app(context=context)
            with app.test_client() as client:
                token = client.get("/api/v1/csrf").json["data"]["token"]
                response = client.post(
                    "/api/v1/sign-in",
                    headers={"X-CSRF-Token": token, "Idempotency-Key": "web-op"},
                    json={"user_id": "u1", "lower_limit": 25, "upper_limit": 25},
                )
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.json["ok"])
                self.assertEqual(response.json["data"]["granted"]["stone"], 25)


if __name__ == "__main__":
    unittest.main()
