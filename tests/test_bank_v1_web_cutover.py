from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.bank import create_blueprint
from nonebot_plugin_xiuxian_2.features.bank.account_application import BankDepositApplication
from nonebot_plugin_xiuxian_2.features.bank.application import BankApplication
from nonebot_plugin_xiuxian_2.features.bank.domain import BankDepositRequest
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts, apply_bank_legacy_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class BankV1WebCutoverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with sqlite3.connect(self.game) as connection:
            connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
            connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 1000)")
        with DatabaseUnitOfWork(self.game) as uow:
            apply_bank_accounts(uow)
            apply_platform_schema(uow)
        with sqlite3.connect(self.player) as connection:
            connection.execute(
                "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY, savestone INTEGER, savetime TEXT, banklevel TEXT)"
            )
            connection.execute("INSERT INTO bankinfo VALUES ('u1', 100, 'legacy-time', '1')")

        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_blueprint(application=BankApplication(self.game, self.player), permission=lambda _: True))
        self.client = app.test_client()
        with self.client.session_transaction() as session:
            session["_csrf_token"] = "csrf"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def post(self, route: str, operation_id: str, **payload):
        return self.client.post(
            f"/api/v1/bank/{route}",
            headers={"Idempotency-Key": operation_id, "X-CSRF-Token": "csrf"},
            json={"user_id": "u1", **payload},
        )

    def test_all_v1_writes_use_imported_game_account_and_replay(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            apply_bank_legacy_accounts(uow)
        deposit_payload = {
            "amount": 50, "expected_saved_stone": 100, "expected_saved_at": "legacy-time",
            "bank_level": "1", "interest": 0, "settled_at": "deposit-time", "save_limit": 1000,
        }
        first = self.post("deposit", "v1-deposit", **deposit_payload)
        replay = self.post("deposit", "v1-deposit", **deposit_payload)
        self.assertEqual((first.status_code, replay.status_code), (200, 200))
        self.assertEqual((first.get_json()["data"]["status"], replay.get_json()["data"]["status"]), ("applied", "replayed"))

        withdrawal = self.post(
            "withdraw", "v1-withdraw", amount=20, expected_saved_stone=150,
            expected_saved_at="deposit-time", bank_level="1", interest=0, settled_at="withdraw-time",
        )
        self.assertEqual(withdrawal.status_code, 200)
        self.assertEqual(withdrawal.get_json()["data"]["data"]["saved_stone"], 130)

        upgrade = self.post("upgrade", "v1-upgrade", expected_level="1", next_level="2", cost=100)
        self.assertEqual(upgrade.status_code, 200)

        interest = self.post(
            "interest", "v1-interest", expected_saved_stone=130,
            expected_saved_at="withdraw-time", bank_level="2", interest=5, settled_at="interest-time",
        )
        self.assertEqual(interest.status_code, 200)

        with sqlite3.connect(self.game) as connection:
            wallet = connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0]
            account = connection.execute(
                "SELECT saved_stone, bank_level, updated_at FROM bank_accounts WHERE user_id='u1'"
            ).fetchone()
            old_ledgers = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
                "('bank_deposit_operations','bank_withdrawal_operations','bank_upgrade_operations','bank_interest_operations')"
            ).fetchall()
        with sqlite3.connect(self.player) as connection:
            legacy = connection.execute("SELECT savestone,savetime,banklevel FROM bankinfo WHERE user_id='u1'").fetchone()
        self.assertEqual(wallet, 875)
        self.assertEqual(account, (130, "2", "interest-time"))
        self.assertEqual(legacy, (100, "legacy-time", "1"))
        self.assertEqual(old_ledgers, [])

    def test_stale_snapshot_is_rejected_without_asset_changes(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) VALUES(?,?,?,?)",
                ("u1", 200, "2", "current-time"),
            )
        stale_amount = self.post(
            "deposit", "stale-deposit", amount=20, expected_saved_stone=199,
            expected_saved_at="current-time", bank_level="2", interest=0,
            settled_at="next-time", save_limit=1000,
        )
        stale_time = self.post(
            "withdraw", "stale-withdraw", amount=20, expected_saved_stone=200,
            expected_saved_at="old-time", bank_level="2", interest=0, settled_at="next-time",
        )
        stale_level = self.post(
            "interest", "stale-interest", expected_saved_stone=200,
            expected_saved_at="current-time", bank_level="1", interest=5, settled_at="next-time",
        )
        for response in (stale_amount, stale_time, stale_level):
            self.assertEqual(response.status_code, 409)
            self.assertEqual(response.get_json()["data"]["data"]["status"], "state_changed")
        with sqlite3.connect(self.game) as connection:
            wallet = connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0]
            saved = connection.execute("SELECT saved_stone FROM bank_accounts WHERE user_id='u1'").fetchone()[0]
        self.assertEqual((wallet, saved), (1000, 200))

    def test_fresh_account_deposit_does_not_create_or_overwrite_legacy_schema(self) -> None:
        with sqlite3.connect(self.player) as connection:
            connection.execute("DROP TABLE bankinfo")
        response = self.post(
            "deposit", "fresh-deposit", amount=25, expected_saved_stone=0,
            expected_saved_at="fresh-time", bank_level="1", interest=0,
            settled_at="deposit-time", save_limit=1000,
        )
        self.assertEqual(response.status_code, 200)
        with sqlite3.connect(self.game) as connection:
            account = connection.execute("SELECT saved_stone,bank_level FROM bank_accounts WHERE user_id='u1'").fetchone()
        with sqlite3.connect(self.player) as connection:
            legacy_table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bankinfo'"
            ).fetchone()
        self.assertEqual(account, (25, "1"))
        self.assertIsNone(legacy_table)

    def test_missing_player_database_is_not_created_by_first_deposit(self) -> None:
        self.player.unlink()
        response = self.post(
            "deposit", "fresh-without-player-db", amount=25, expected_saved_stone=0,
            expected_saved_at="fresh-time", bank_level="1", interest=0,
            settled_at="deposit-time", save_limit=1000,
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(self.player.exists())

    def test_invalid_legacy_schema_blocks_startup_backfill_without_asset_changes(self) -> None:
        with sqlite3.connect(self.player) as connection:
            connection.execute("DROP TABLE bankinfo")
            connection.execute("CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY, savestone INTEGER)")
            connection.execute("INSERT INTO bankinfo VALUES ('u1', 900)")
        with self.assertRaisesRegex(RuntimeError, "legacy bankinfo schema incomplete"):
            with DatabaseUnitOfWork(self.game) as uow:
                apply_bank_legacy_accounts(uow)
        with sqlite3.connect(self.game) as connection:
            wallet = connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0]
            account = connection.execute("SELECT 1 FROM bank_accounts WHERE user_id='u1'").fetchone()
        self.assertEqual(wallet, 1000)
        self.assertIsNone(account)

    def test_started_ledger_resumes_from_committed_account_receipt(self) -> None:
        request = {
            "operation_id": "recover-deposit", "user_id": "u1", "amount": 25,
            "expected_saved_stone": 100, "expected_saved_at": "legacy-time",
            "bank_level": "1", "interest": 0, "settled_at": "recovered-time", "save_limit": 1000,
        }
        with DatabaseUnitOfWork(self.game) as uow:
            apply_bank_legacy_accounts(uow)
        BankDepositApplication(self.game).deposit(
            operation_id=request["operation_id"], user_id=request["user_id"],
            amount=request["amount"], interest=request["interest"], limit=request["save_limit"],
            bank_level=request["bank_level"], settled_at=request["settled_at"],
        )
        ledger_payload = BankDepositRequest(
            request["operation_id"], request["user_id"], request["amount"],
            request["expected_saved_stone"], request["expected_saved_at"], request["bank_level"],
            request["interest"], request["settled_at"], request["save_limit"],
        ).payload()
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            OperationLedger().begin(uow, request["operation_id"], "bank.deposit", ledger_payload)

        outcome = BankApplication(self.game, self.player).deposit(**request)
        self.assertEqual(outcome.status, "applied")
        self.assertEqual(outcome.data["status"], "duplicate")
        with sqlite3.connect(self.game) as connection:
            wallet = connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0]
            saved = connection.execute("SELECT saved_stone FROM bank_accounts WHERE user_id='u1'").fetchone()[0]
        self.assertEqual((wallet, saved), (975, 125))


if __name__ == "__main__":
    unittest.main()
