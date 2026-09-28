from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.bank_first_use import create_upgrade_blueprint
from nonebot_plugin_xiuxian_2.features.bank.account_upgrade_application import BankUpgradeApplication
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankUpgradeWebTests(unittest.TestCase):
    def test_upgrade_route_passes_idempotency_and_payload(self) -> None:
        application = Mock()
        application.upgrade.return_value = {"status": "applied", "bank_level": "2"}
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_upgrade_blueprint(application=application, permission=lambda _: True))
        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        response = client.post(
            "/api/v1/bank/v2/upgrade",
            headers={"Idempotency-Key": "upgrade-web-1", "X-CSRF-Token": "csrf"},
            json={"user_id": "u1", "expected_level": "1", "next_level": "2", "cost": 200000, "settled_at": "2026-09-13T00:00:00Z"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(application.upgrade.call_args.kwargs["operation_id"], "upgrade-web-1")
        self.assertEqual(application.upgrade.call_args.kwargs["next_level"], "2")

    def test_upgrade_route_executes_real_application(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 500000)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
                uow.execute("INSERT INTO bank_accounts VALUES ('u1', 100, '1', 't')")

            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(create_upgrade_blueprint(application=BankUpgradeApplication(database), permission=lambda _: True))
            client = app.test_client()
            with client.session_transaction() as session:
                session["_csrf_token"] = "csrf"
            headers = {"Idempotency-Key": "upgrade-web-real-1", "X-CSRF-Token": "csrf"}
            payload = {"user_id": "u1", "expected_level": "1", "next_level": "2", "cost": 200000, "settled_at": "t2"}
            response = client.post("/api/v1/bank/v2/upgrade", headers=headers, json=payload)
            duplicate = client.post("/api/v1/bank/v2/upgrade", headers=headers, json=payload)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["data"]["status"], "applied")
            self.assertEqual(duplicate.status_code, 200)
            self.assertEqual(duplicate.get_json()["data"]["status"], "duplicate")
            self.assertEqual(duplicate.get_json()["data"]["cost"], 200000)
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0], 300000)
                self.assertEqual(connection.execute("SELECT bank_level FROM bank_accounts WHERE user_id='u1'").fetchone()[0], "2")
                self.assertEqual(connection.execute("SELECT count(*) FROM bank_account_operations").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
