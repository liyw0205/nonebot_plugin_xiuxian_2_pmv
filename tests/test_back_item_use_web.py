from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
from unittest.mock import Mock

from flask import Flask

from nonebot_plugin_xiuxian_2.features.back.web import blueprint
from nonebot_plugin_xiuxian_2.features.back.application import BackApplication
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_item_use
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class BackItemUseWebTests(unittest.TestCase):
    def test_generic_route_calls_item_use_application_contract(self) -> None:
        application = Mock()
        outcome = Mock(ok=True, to_dict=lambda: {"status": "applied"})
        application.use_item.return_value = outcome
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(blueprint(application, permission=lambda _: True))
        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        response = client.post(
            "/api/v1/back/use_item",
            headers={"Idempotency-Key": "item-use-1", "X-CSRF-Token": "csrf"},
            json={"user_id": "u", "item_id": 20012, "quantity": 2},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["data"], {"status": "applied"})
        self.assertEqual(application.use_item.call_args.kwargs["item_id"], 20012)

    def test_real_application_returns_json_instead_of_internal_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                    "bind_num INTEGER,state INTEGER,UNIQUE(user_id,goods_id))"
                )
                conn.execute("INSERT INTO back VALUES('u',20012,3,2,0)")
            with DatabaseUnitOfWork(database) as uow:
                apply_item_use(uow)
            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(blueprint(BackApplication(database, database), permission=lambda _: True))
            client = app.test_client()
            with client.session_transaction() as session:
                session["_csrf_token"] = "csrf"
            response = client.post(
                "/api/v1/back/use_item",
                headers={"Idempotency-Key": "real-item-use", "X-CSRF-Token": "csrf"},
                json={"user_id": "u", "item_id": 20012, "quantity": 2, "expected_item_count": 3},
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["data"]["status"], "applied")


if __name__ == "__main__":
    unittest.main()
