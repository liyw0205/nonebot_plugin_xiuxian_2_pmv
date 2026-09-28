from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.bank_first_use import create_interest_blueprint
from nonebot_plugin_xiuxian_2.features.bank.account_interest_application import BankInterestApplication
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankInterestWebTests(unittest.TestCase):
    def test_interest_route_calls_new_application(self) -> None:
        application = Mock()
        application.settle_interest.return_value = {"status": "applied", "interest": 7}
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_interest_blueprint(application=application, permission=lambda _: True))
        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        response = client.post("/api/v1/bank/v2/interest", headers={"Idempotency-Key": "interest-web-1", "X-CSRF-Token": "csrf"}, json={"user_id": "u1", "interest": 7, "bank_level": "1", "settled_at": "2026-09-13T00:00:00Z"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(application.settle_interest.call_args.kwargs["operation_id"], "interest-web-1")

    def test_interest_route_executes_real_application(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 100)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
                uow.execute("INSERT INTO bank_accounts VALUES ('u1', 50, '1', 't')")

            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(create_interest_blueprint(application=BankInterestApplication(database), permission=lambda _: True))
            client = app.test_client()
            with client.session_transaction() as session:
                session["_csrf_token"] = "csrf"
            headers = {"Idempotency-Key": "interest-web-real-1", "X-CSRF-Token": "csrf"}
            payload = {"user_id": "u1", "interest": 7, "bank_level": "1", "settled_at": "t2"}
            response = client.post("/api/v1/bank/v2/interest", headers=headers, json=payload)
            duplicate = client.post("/api/v1/bank/v2/interest", headers=headers, json=payload)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["data"]["status"], "applied")
            self.assertEqual(duplicate.status_code, 200)
            self.assertEqual(duplicate.get_json()["data"]["status"], "duplicate")
            self.assertEqual(duplicate.get_json()["data"]["interest"], 7)
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0], 107)
                self.assertEqual(connection.execute("SELECT saved_stone FROM bank_accounts WHERE user_id='u1'").fetchone()[0], 50)
                self.assertEqual(connection.execute("SELECT count(*) FROM bank_account_operations").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
