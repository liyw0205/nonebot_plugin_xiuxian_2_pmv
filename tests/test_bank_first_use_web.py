from __future__ import annotations

import unittest
from unittest.mock import Mock
import sqlite3
import tempfile
from pathlib import Path

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.bank_first_use import create_first_use_blueprint
from nonebot_plugin_xiuxian_2.features.bank.account_application import BankDepositApplication
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankFirstUseWebTests(unittest.TestCase):
    def test_deposit_route_calls_new_application(self) -> None:
        application = Mock()
        application.deposit.return_value = {"status": "applied", "saved_stone": 100}
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_first_use_blueprint(application=application, permission=lambda _: True))
        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        response = client.post("/api/v1/bank/v2/deposit", headers={"Idempotency-Key": "bank-web-1", "X-CSRF-Token": "csrf"}, json={"user_id": "u1", "amount": 100, "interest": 0, "limit": 1000, "bank_level": "1", "settled_at": "2026-09-13T00:00:00Z"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(application.deposit.call_args.kwargs["operation_id"], "bank-web-1")
        self.assertEqual(response.get_json()["data"]["status"], "applied")

    def test_deposit_route_executes_real_application_and_replays(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 1000)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)

            app = Flask(__name__)
            app.secret_key = "test"
            application = BankDepositApplication(database)
            app.register_blueprint(create_first_use_blueprint(application=application, permission=lambda _: True))
            client = app.test_client()
            with client.session_transaction() as session:
                session["_csrf_token"] = "csrf"
            payload = {
                "user_id": "u1", "amount": 100, "interest": 0,
                "limit": 1000, "bank_level": "1", "settled_at": "2026-09-13T00:00:00Z",
            }
            headers = {"Idempotency-Key": "bank-web-real-1", "X-CSRF-Token": "csrf"}
            first = client.post("/api/v1/bank/v2/deposit", headers=headers, json=payload)
            duplicate = client.post("/api/v1/bank/v2/deposit", headers=headers, json=payload)
            self.assertEqual((first.status_code, duplicate.status_code), (200, 200))
            self.assertEqual((first.get_json()["data"]["status"], duplicate.get_json()["data"]["status"]), ("applied", "duplicate"))

    def test_deposit_route_reports_missing_startup_schema_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "unmigrated.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 1000)")

            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(create_first_use_blueprint(application=BankDepositApplication(database), permission=lambda _: True))
            client = app.test_client()
            with client.session_transaction() as session:
                session["_csrf_token"] = "csrf"
            response = client.post(
                "/api/v1/bank/v2/deposit",
                headers={"Idempotency-Key": "bank-web-schema-missing", "X-CSRF-Token": "csrf"},
                json={"user_id": "u1", "amount": 100, "limit": 1000},
            )
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.get_json()["error"]["code"], "schema_missing")
            with sqlite3.connect(database) as connection:
                tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertEqual(tables, {"user_xiuxian"})


if __name__ == "__main__":
    unittest.main()
