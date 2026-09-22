import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.application import BankApplication
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


class BankApplicationDepositTests(unittest.TestCase):
    def test_default_deposit_uses_account_application(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u1', 1000)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
                apply_platform_schema(uow)
            app = BankApplication(database, database)
            outcome = app.deposit(
                operation_id="bank-default-deposit", user_id="u1", amount=300,
                expected_saved_stone=0, expected_saved_at="", bank_level="1",
                interest=10, settled_at="t", save_limit=1000,
            )
            replay = app.deposit(
                operation_id="bank-default-deposit", user_id="u1", amount=300,
                expected_saved_stone=0, expected_saved_at="", bank_level="1",
                interest=10, settled_at="t", save_limit=1000,
            )
            self.assertEqual((outcome.status, replay.status), ("applied", "replayed"))
