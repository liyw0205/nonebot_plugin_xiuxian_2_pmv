import tempfile
import unittest
from pathlib import Path

from ..account_bootstrap_application import BankAccountBootstrapApplication
from ..migrations import apply_bank_accounts
from ....infrastructure.database import DatabaseUnitOfWork


class BankAccountBootstrapApplicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            apply_bank_accounts(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY,savestone INTEGER,savetime TEXT,banklevel TEXT)"
            )
            uow.execute("INSERT INTO bankinfo VALUES('u',80,'2026-09-22 10:00:00','2')")

    def tearDown(self):
        self.tmp.cleanup()

    def test_imports_legacy_account_into_game_database(self):
        application = BankAccountBootstrapApplication(self.game, self.player)
        self.assertEqual(
            application.ensure_account(user_id="u", default_level="1"),
            {"status": "imported", "user_id": "u", "saved_stone": 80, "bank_level": "2"},
        )
        with DatabaseUnitOfWork(self.game) as uow:
            row = uow.query_one("SELECT saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id='u'")
        self.assertEqual((row["saved_stone"], row["bank_level"], row["updated_at"]), (80, "2", "2026-09-22 10:00:00"))

    def test_import_is_idempotent(self):
        application = BankAccountBootstrapApplication(self.game, self.player)
        first = application.ensure_account(user_id="u", default_level="1")
        second = application.ensure_account(user_id="u", default_level="1")
        self.assertEqual(first["status"], "imported")
        self.assertEqual(second["status"], "existing")

    def test_missing_legacy_row_is_explicit(self):
        application = BankAccountBootstrapApplication(self.game, self.player)
        self.assertEqual(
            application.ensure_account(user_id="missing", default_level="1")["status"],
            "legacy_missing",
        )

    def test_missing_migration_rejects_import_without_creating_tables(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TABLE bank_account_operations")
            uow.execute("DROP TABLE bank_accounts")

        application = BankAccountBootstrapApplication(self.game, self.player)
        with self.assertRaisesRegex(RuntimeError, r"bank\.002 schema_missing: bank_accounts"):
            application.ensure_account(user_id="u", default_level="1")

        with DatabaseUnitOfWork(self.game) as uow:
            tables = {row["name"] for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(tables, {"user_xiuxian"})

    def test_partial_migration_rejects_import_without_writing_account(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TABLE bank_account_operations")

        application = BankAccountBootstrapApplication(self.game, self.player)
        with self.assertRaisesRegex(RuntimeError, r"bank\.002 schema_missing: bank_account_operations"):
            application.ensure_account(user_id="u", default_level="1")

        with DatabaseUnitOfWork(self.game) as uow:
            self.assertIsNone(uow.query_one("SELECT user_id FROM bank_accounts WHERE user_id='u'"))


if __name__ == "__main__":
    unittest.main()
