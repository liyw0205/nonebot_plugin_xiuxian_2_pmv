from __future__ import annotations

import tempfile
import unittest
import sqlite3
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..legacy_state import crystal_balance
from ..migrations import apply_impart_exact_crystal_balances


class ImpartCrystalExactMigrationTests(unittest.TestCase):
    def test_exact_storage_migrations_follow_database_ownership(self):
        from ....plugin import build_migrations, migrations_for_database

        migrations = build_migrations()
        game = {m.version for m in migrations_for_database(migrations, "game_db")}
        impart = {m.version for m in migrations_for_database(migrations, "impart_db")}
        self.assertIn("impart.010", game)
        self.assertNotIn("impart.011", game)
        self.assertIn("impart.011", impart)
        self.assertNotIn("impart.010", impart)

    def _migrate(self, declaration: str, value):
        temp = tempfile.TemporaryDirectory()
        database = Path(temp.name) / "impart.db"
        with DatabaseUnitOfWork(database) as uow:
            uow.execute(
                f"CREATE TABLE xiuxian_impart(user_id TEXT,stone_num {declaration})"
            )
            uow.execute(
                "INSERT INTO xiuxian_impart(user_id,stone_num) VALUES(?,?)",
                ("actor", value),
            )
            apply_impart_exact_crystal_balances(uow)
        return temp, database

    def test_integer_scientific_text_and_integral_real_migrate_exactly(self):
        cases = (
            ("INTEGER", 2**63 - 1, str(2**63 - 1)),
            ("TEXT", "1.234e5", "123400"),
            ("REAL", 12345.0, "12345"),
            ("TEXT", str(10**45 + 123), str(10**45 + 123)),
        )
        for declaration, value, expected in cases:
            with self.subTest(declaration=declaration, value_type=type(value).__name__):
                temp, database = self._migrate(declaration, value)
                try:
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        row = uow.query_one("SELECT * FROM xiuxian_impart WHERE user_id='actor'")
                        self.assertEqual(row["stone_num_exact"], expected)
                        self.assertEqual(row["stone_num_precision_state"], "exact")
                        self.assertEqual(crystal_balance(row), int(expected))
                finally:
                    temp.cleanup()

    def test_ambiguous_large_or_fractional_real_is_marked_unrecoverable(self):
        cases = (
            (float(2**53 + 2), "precision_lost_real"),
            (float(2**53), "precision_lost_real"),
            (float(-(2**53)), "precision_lost_real"),
            (12.5, "invalid_numeric_value"),
        )
        for value, expected_state in cases:
            with self.subTest(value=value):
                temp, database = self._migrate("REAL", value)
                try:
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        row = uow.query_one("SELECT * FROM xiuxian_impart WHERE user_id='actor'")
                        self.assertIsNone(row["stone_num_exact"])
                        self.assertEqual(row["stone_num_precision_state"], expected_state)
                        self.assertEqual(row["stone_num"], value)
                finally:
                    temp.cleanup()

    def test_migration_replay_preserves_exact_shadow_and_legacy_value(self):
        temp, database = self._migrate("TEXT", "1e30")
        try:
            with DatabaseUnitOfWork(database) as uow:
                before = uow.query_one(
                    "SELECT stone_num,stone_num_exact,stone_num_precision_state "
                    "FROM xiuxian_impart WHERE user_id='actor'"
                )
                apply_impart_exact_crystal_balances(uow)
                after = uow.query_one(
                    "SELECT stone_num,stone_num_exact,stone_num_precision_state "
                    "FROM xiuxian_impart WHERE user_id='actor'"
                )
            self.assertEqual(tuple(after.values()), tuple(before.values()))
            self.assertEqual(after["stone_num_exact"], "1000000000000000000000000000000")
        finally:
            temp.cleanup()

    def test_legacy_balance_updates_without_exact_shadow_abort_without_writes(self):
        temp, database = self._migrate("INTEGER", 100)
        try:
            with DatabaseUnitOfWork(database) as uow:
                before = uow.query_one(
                    "SELECT stone_num,stone_num_exact,stone_num_precision_state "
                    "FROM xiuxian_impart WHERE user_id='actor'"
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    uow.execute(
                        "UPDATE xiuxian_impart SET stone_num=stone_num-1 "
                        "WHERE user_id='actor'"
                    )
                after = uow.query_one(
                    "SELECT stone_num,stone_num_exact,stone_num_precision_state "
                    "FROM xiuxian_impart WHERE user_id='actor'"
                )
            self.assertEqual(tuple(after.values()), tuple(before.values()))
        finally:
            temp.cleanup()

    def test_numeric_sql_arithmetic_keeps_exact_decimal_operands(self):
        with tempfile.TemporaryDirectory() as temp:
            with DatabaseUnitOfWork(Path(temp) / "arithmetic.db") as uow:
                huge = 10**45 + 123
                row = uow.query_one(
                    "SELECT xiuxian_int_add(?,?) AS added,xiuxian_int_sub(?,?) AS subtracted",
                    (str(huge), 5, str(huge), 20),
                )
                self.assertEqual((row["added"], row["subtracted"]), (str(huge + 5), str(huge - 20)))
                with self.assertRaises(sqlite3.OperationalError):
                    uow.execute("SELECT xiuxian_int_add(?,?)", ("not-an-integer", 1))


if __name__ == "__main__":
    unittest.main()
