from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from flask import Flask

from nonebot_plugin_xiuxian_2.features.trade.web import blueprint as trade_blueprint
from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_deposit_repository import (
    GuishiDepositSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_guishi_deposit,
    apply_trade_guishi_schema,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import (
    apply_platform_schema,
    build_migrations,
    migrations_for_database,
)


class GuishiDepositSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.db"
        self.trade_database = root / "trade.db"
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian(user_id,stone) VALUES(?,?)", ("u", 1000))
            apply_platform_schema(uow)
            apply_trade_guishi_deposit(uow)
        with DatabaseUnitOfWork(self.trade_database) as uow:
            apply_trade_guishi_schema(uow)
        self.repository = GuishiDepositSqlRepository(
            self.game_database, self.trade_database
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self) -> tuple[int, int, int]:
        with DatabaseUnitOfWork(self.game_database) as uow:
            stone = int(
                uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", ("u",))["stone"]
            )
            operations = int(
                uow.query_one("SELECT COUNT(*) AS count FROM trade_guishi_deposit_operations")["count"]
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            row = uow.query_one(
                "SELECT stored_stone FROM guishi_info WHERE user_id=?", ("u",)
            )
        return stone, int(row["stored_stone"]) if row is not None else 0, operations

    def deposit(self, operation_id: str = "deposit", amount: int = 600):
        return self.repository.deposit(
            operation_id=operation_id, user_id="u", amount=amount
        )

    def test_deposit_replays_and_rejects_conflicting_operation(self) -> None:
        first = self.deposit("same")
        replay = self.deposit("same")
        conflict = self.deposit("same", 500)

        self.assertEqual(
            (first.status, replay.status, conflict.status),
            ("completed", "duplicate", "operation_conflict"),
        )
        self.assertEqual((replay.amount, replay.stored_balance), (600, 600))
        self.assertEqual(self.state(), (400, 600, 1))

    def test_rejections_preserve_both_databases_and_dirty_balance_is_not_committed(self) -> None:
        self.assertEqual(self.deposit("poor", 1001).status, "stone_insufficient")
        self.assertEqual(self.deposit("capped", GuishiDepositSqlRepository.OP_AMOUNT_CAP + 1).status, "amount_capped")
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "INSERT INTO guishi_info(user_id,stored_stone,items) VALUES(?,?,?)",
                ("u", GuishiDepositSqlRepository.STORED_CAP * 3, "{}"),
            )
        result = self.deposit("full", 1)

        self.assertEqual((result.status, result.stored_balance), ("stored_cap_exceeded", GuishiDepositSqlRepository.STORED_CAP))
        self.assertEqual(self.state(), (1000, GuishiDepositSqlRepository.STORED_CAP * 3, 0))

    def test_legacy_operation_is_replayed_without_a_second_transfer(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TABLE guishi_stone_operations("
                "operation_id TEXT PRIMARY KEY,operation_type TEXT,user_id TEXT,amount INTEGER,"
                "fee INTEGER,actual_amount INTEGER,stored_balance INTEGER)"
            )
            uow.execute(
                "INSERT INTO guishi_stone_operations VALUES(?,?,?,?,?,?,?)",
                ("legacy", "deposit", "u", 600, 0, 600, 600),
            )

        replay = self.deposit("legacy")
        conflict = self.deposit("legacy", 500)

        self.assertEqual((replay.status, replay.stored_balance, conflict.status), ("duplicate", 600, "operation_conflict"))
        self.assertEqual(self.state(), (1000, 0, 0))

    def test_operation_write_failure_rolls_back_player_and_trade_projection(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_guishi_deposit BEFORE INSERT ON "
                "trade_guishi_deposit_operations BEGIN SELECT RAISE(ABORT,'reject'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.deposit("rollback")
        self.assertEqual(self.state(), (1000, 0, 0))

    def test_application_default_uses_feature_owned_repository(self) -> None:
        result = TradeApplication(
            self.game_database, self.trade_database
        ).guishi_deposit(operation_id="application", user_id="u", amount=200)

        self.assertEqual(result.status, "completed")
        self.assertEqual(self.state(), (800, 200, 1))

    def test_web_deposit_uses_feature_repository_and_standard_outcome(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(
            trade_blueprint(
                TradeApplication(self.game_database, self.trade_database),
                permission=lambda _: True,
            )
        )
        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"

        response = client.post(
            "/api/v1/trade/deposit",
            headers={"Idempotency-Key": "web-deposit", "X-CSRF-Token": "csrf"},
            json={"user_id": "u", "amount": 300},
        )
        replay = client.post(
            "/api/v1/trade/deposit",
            headers={"Idempotency-Key": "web-deposit", "X-CSRF-Token": "csrf"},
            json={"user_id": "u", "amount": 300},
        )

        self.assertEqual((response.status_code, replay.status_code), (200, 200))
        self.assertEqual(response.get_json()["data"]["status"], "applied")
        self.assertEqual(response.get_json()["data"]["data"]["status"], "completed")
        self.assertEqual(replay.get_json()["data"]["status"], "replayed")
        self.assertEqual(self.state(), (700, 300, 1))


class TradeMigrationRoutingTests(unittest.TestCase):
    def test_guishi_deposit_schema_is_owned_by_the_correct_database(self) -> None:
        migrations = build_migrations()
        game_versions = {item.version for item in migrations_for_database(migrations, "game_db")}
        trade_versions = {item.version for item in migrations_for_database(migrations, "trade_db")}

        self.assertIn("trade.002", game_versions)
        self.assertNotIn("trade.003", game_versions)
        self.assertIn("trade.003", trade_versions)
        self.assertNotIn("trade.002", trade_versions)


if __name__ == "__main__":
    unittest.main()
