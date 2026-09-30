from __future__ import annotations

import unittest
from unittest.mock import Mock
import sqlite3
import tempfile
from pathlib import Path

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.bank_first_use import create_info_blueprint
from nonebot_plugin_xiuxian_2.features.bank.account_info_application import BankAccountInfoApplication
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankInfoWebTests(unittest.TestCase):
    def test_info_route_reads_application(self) -> None:
        application = Mock()
        application.get_info.return_value = {"status": "ok", "saved_stone": 20}
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_info_blueprint(application=application, permission=lambda _: True))
        response = app.test_client().get("/api/v1/bank/v2/info?user_id=u1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(application.get_info.call_args.kwargs["user_id"], "u1")

    def test_info_route_does_not_import_legacy_account(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            player = root / "player.db"
            with sqlite3.connect(game) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 80)")
            with DatabaseUnitOfWork(game) as uow:
                apply_bank_accounts(uow)
            with sqlite3.connect(player) as connection:
                connection.execute("CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY, savestone INTEGER, savetime TEXT, banklevel TEXT)")
                connection.execute("INSERT INTO bankinfo VALUES ('u1', 20, 'then', '2')")

            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(
                create_info_blueprint(
                    application=BankAccountInfoApplication(game, player_database=player),
                    permission=lambda _: True,
                )
            )
            response = app.test_client().get("/api/v1/bank/v2/info?user_id=u1")
            self.assertEqual(response.status_code, 404)
            self.assertEqual(response.get_json()["data"]["status"], "account_missing")
            with sqlite3.connect(game) as connection:
                self.assertIsNone(connection.execute("SELECT bank_level FROM bank_accounts WHERE user_id='u1'").fetchone())

    def test_info_route_returns_not_found_for_invalid_legacy_account(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            player = root / "player.db"
            with sqlite3.connect(game) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 80)")
            with DatabaseUnitOfWork(game) as uow:
                apply_bank_accounts(uow)
            with sqlite3.connect(player) as connection:
                connection.execute("CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY, savestone INTEGER)")
                connection.execute("INSERT INTO bankinfo VALUES ('u1', 20)")

            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(create_info_blueprint(application=BankAccountInfoApplication(game, player_database=player), permission=lambda _: True))
            response = app.test_client().get("/api/v1/bank/v2/info?user_id=u1")
            self.assertEqual(response.status_code, 404)
            self.assertEqual(response.get_json()["data"]["status"], "account_missing")


if __name__ == "__main__":
    unittest.main()
