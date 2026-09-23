from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_stone_rules import withdrawal_fee
from nonebot_plugin_xiuxian_2.features.trade.guishi_withdraw_repository import (
    GuishiWithdrawSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_guishi_schema,
    apply_trade_guishi_withdraw,
)
from nonebot_plugin_xiuxian_2.features.trade.web import blueprint as trade_blueprint
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import (
    apply_platform_schema,
    build_migrations,
    migrations_for_database,
)


class FixedClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


SATURDAY = datetime(2026, 9, 26, 12, tzinfo=timezone.utc)
MONDAY = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)


class GuishiWithdrawSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.db"
        self.trade_database = root / "trade.db"
        self.clock = FixedClock(SATURDAY)
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian(user_id,stone) VALUES(?,?)", ("u", 1000))
            apply_platform_schema(uow)
            apply_trade_guishi_withdraw(uow)
        with DatabaseUnitOfWork(self.trade_database) as uow:
            apply_trade_guishi_schema(uow)
            uow.execute(
                "INSERT INTO guishi_info(user_id,stored_stone,items) VALUES(?,?,?)",
                ("u", 800, "{}"),
            )
        self.application = TradeApplication(
            self.game_database, self.trade_database, clock=self.clock
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self) -> tuple[int, int, int]:
        with DatabaseUnitOfWork(self.game_database) as uow:
            stone = int(
                uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", ("u",))["stone"]
            )
            operations = int(
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM trade_guishi_withdraw_operations"
                )["count"]
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            stored = int(
                uow.query_one(
                    "SELECT stored_stone FROM guishi_info WHERE user_id=?", ("u",)
                )["stored_stone"]
            )
        return stone, stored, operations

    def withdraw(self, operation_id: str = "withdraw", amount: int = 500):
        return self.application.guishi_withdraw(
            operation_id=operation_id, user_id="u", amount=amount
        )

    def test_withdraw_applies_dynamic_fee_and_replays_without_double_credit(self) -> None:
        first = self.withdraw("same")
        replay = self.withdraw("same")
        conflict = self.withdraw("same", 400)

        self.assertEqual(
            (first.status, first.fee, first.actual_amount, replay.status, conflict.status),
            ("completed", 100, 400, "duplicate", "operation_conflict"),
        )
        self.assertEqual((replay.fee, replay.actual_amount, replay.stored_balance), (100, 400, 300))
        self.assertEqual(self.state(), (1400, 300, 1))

    def test_weekend_policy_is_clock_owned_and_does_not_write_rejections(self) -> None:
        self.clock.value = MONDAY
        closed = self.withdraw("weekday")
        self.assertEqual(closed.status, "weekend_closed")
        self.assertEqual(self.state(), (1000, 800, 0))

        self.clock.value = SATURDAY
        applied = self.withdraw("weekday")
        self.assertEqual((applied.status, applied.actual_amount), ("completed", 400))
        self.assertEqual(self.state(), (1400, 300, 1))

    def test_legacy_operation_replays_before_weekend_policy(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TABLE guishi_stone_operations("
                "operation_id TEXT PRIMARY KEY,operation_type TEXT,user_id TEXT,amount INTEGER,"
                "fee INTEGER,actual_amount INTEGER,stored_balance INTEGER)"
            )
            uow.execute(
                "INSERT INTO guishi_stone_operations VALUES(?,?,?,?,?,?,?)",
                ("legacy", "withdraw", "u", 500, 100, 400, 300),
            )
        self.clock.value = MONDAY

        replay = self.withdraw("legacy")
        conflict = self.withdraw("legacy", 400)

        self.assertEqual(
            (replay.status, replay.fee, replay.actual_amount, conflict.status),
            ("duplicate", 100, 400, "operation_conflict"),
        )
        self.assertEqual(self.state(), (1000, 800, 0))

    def test_rejections_and_state_change_leave_both_databases_unchanged(self) -> None:
        insufficient = self.withdraw("poor", 801)
        capped = self.withdraw(
            "capped", GuishiWithdrawSqlRepository.OP_AMOUNT_CAP + 1
        )
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TRIGGER skip_guishi_credit BEFORE UPDATE ON user_xiuxian "
                "BEGIN SELECT RAISE(IGNORE); END"
            )
        changed = self.withdraw("changed")

        self.assertEqual(
            (insufficient.status, capped.status, changed.status),
            ("stored_insufficient", "amount_capped", "state_changed"),
        )
        self.assertEqual(self.state(), (1000, 800, 0))

    def test_dirty_balance_is_clamped_only_by_a_completed_withdrawal(self) -> None:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "UPDATE guishi_info SET stored_stone=? WHERE user_id=?",
                (GuishiWithdrawSqlRepository.STORED_CAP * 3, "u"),
            )

        result = self.withdraw("dirty", 100)

        self.assertEqual((result.status, result.fee, result.actual_amount), ("completed", 20, 80))
        self.assertEqual(
            self.state(),
            (1080, GuishiWithdrawSqlRepository.STORED_CAP - 100, 1),
        )

    def test_operation_write_failure_rolls_back_wallet_and_trade_projection(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_guishi_withdraw BEFORE INSERT ON "
                "trade_guishi_withdraw_operations BEGIN SELECT RAISE(ABORT,'reject'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.withdraw("rollback")
        self.assertEqual(self.state(), (1000, 800, 0))

    def test_web_withdraw_uses_same_application_and_replays_across_clock_change(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(
            trade_blueprint(self.application, permission=lambda _: True)
        )
        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"

        response = client.post(
            "/api/v1/trade/withdraw",
            headers={"Idempotency-Key": "web-withdraw", "X-CSRF-Token": "csrf"},
            json={"user_id": "u", "amount": 500},
        )
        self.clock.value = MONDAY
        replay = client.post(
            "/api/v1/trade/withdraw",
            headers={"Idempotency-Key": "web-withdraw", "X-CSRF-Token": "csrf"},
            json={"user_id": "u", "amount": 500},
        )

        self.assertEqual((response.status_code, replay.status_code), (200, 200))
        self.assertEqual(response.get_json()["data"]["data"]["status"], "completed")
        self.assertEqual(replay.get_json()["data"]["status"], "replayed")
        self.assertEqual(self.state(), (1400, 300, 1))


class GuishiWithdrawRuleTests(unittest.TestCase):
    def test_fee_schedule_keeps_historical_dynamic_bands(self) -> None:
        self.assertEqual(withdrawal_fee(1_000_000_000, 100), 20)
        self.assertEqual(withdrawal_fee(20_000_000_000, 100), 25)
        self.assertEqual(withdrawal_fee(200_000_000_000, 100), 80)


class TradeWithdrawMigrationRoutingTests(unittest.TestCase):
    def test_guishi_withdraw_operations_are_owned_by_game_database(self) -> None:
        migrations = build_migrations()
        game_versions = {
            item.version for item in migrations_for_database(migrations, "game_db")
        }
        trade_versions = {
            item.version for item in migrations_for_database(migrations, "trade_db")
        }

        self.assertIn("trade.004", game_versions)
        self.assertNotIn("trade.004", trade_versions)


if __name__ == "__main__":
    unittest.main()
