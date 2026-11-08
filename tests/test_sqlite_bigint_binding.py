"""Overflow regressions for the shipped SQLite numeric binding.

Production failures (xiu2.log 11:00 / 12:12 / 17:43):

    OverflowError: Python int too large to convert to SQLite INTEGER

from 虚神界出关 (``UPDATE user_xiuxian … power=?``) and the hourly
``materialsupdate_`` job.  Any Python int outside int64 aborts the whole
transaction, so these cases pin the shipped guard:

* :func:`bind_sqlite_param` rewrites oversized ints to plain decimal TEXT and
  leaves int64-range ints, floats, strings and ``None`` untouched;
* :class:`DatabaseUnitOfWork` applies it to every statement;
* an int64-range value round-trips as an exact INTEGER, while a value beyond
  int64 lands as REAL (double) — SQLite's documented numeric storage limit,
  disclosed in ``docs/gameplay_notes.md``; nothing is clipped or clamped;
* a failing statement still rolls the oversized write back.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.numeric import (
    SQLITE_MAX_INT,
    SQLITE_MIN_INT,
    adapt_sqlite_params,
    bind_sqlite_param,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

HUGE = 10**25 + 12345


class BindSqliteParamTests(unittest.TestCase):
    def test_int64_range_values_are_left_exact(self) -> None:
        for value in (0, 1, -1, 42, SQLITE_MAX_INT, SQLITE_MIN_INT):
            self.assertIs(type(bind_sqlite_param(value)), int)
            self.assertEqual(value, bind_sqlite_param(value))

    def test_oversized_integers_become_decimal_text(self) -> None:
        for value in (SQLITE_MAX_INT + 1, SQLITE_MIN_INT - 1, HUGE):
            self.assertEqual(str(value), bind_sqlite_param(value))

    def test_floats_rates_strings_and_none_are_untouched(self) -> None:
        for value in (0.5, 1.25, "1e9", "", None, "abc"):
            self.assertEqual(value, bind_sqlite_param(value))
            self.assertIs(type(bind_sqlite_param(value)), type(value))

    def test_booleans_and_param_shapes(self) -> None:
        self.assertEqual(1, bind_sqlite_param(True))
        self.assertEqual(0, bind_sqlite_param(False))
        self.assertEqual((), adapt_sqlite_params(None))
        self.assertEqual((1, str(2**64)), adapt_sqlite_params([1, 2**64]))
        self.assertEqual({"a": str(2**64)}, adapt_sqlite_params({"a": 2**64}))
        self.assertEqual(("x",), adapt_sqlite_params("x"))


class PersistedValueTests(unittest.TestCase):
    """Actual on-disk storage through the UoW boundary, not just the adapter."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "values.db"
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp NUMERIC,power NUMERIC)"
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def row(self) -> tuple:
        with sqlite3.connect(self.database) as connection:
            return connection.execute(
                "SELECT typeof(exp),exp,typeof(power),power FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()

    def test_uow_writes_int64_value_as_exact_integer(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO user_xiuxian(user_id,exp,power) VALUES(?,?,?)",
                ("u", SQLITE_MAX_INT, 3),
            )

        kind, value, power_kind, power = self.row()
        self.assertEqual("integer", kind)
        self.assertEqual(SQLITE_MAX_INT, value)
        self.assertEqual(("integer", 3), (power_kind, power))

    def test_oversized_value_persists_as_real_without_raising(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO user_xiuxian(user_id,exp,power) VALUES(?,?,?)",
                ("u", HUGE, HUGE),
            )

        kind, value, power_kind, power = self.row()
        # Numeric affinity cannot store >int64 as INTEGER; the value is kept as
        # REAL (double) instead of aborting the transaction or being clamped.
        self.assertEqual("real", kind)
        self.assertEqual(float(HUGE), value)
        self.assertEqual(("real", float(HUGE)), (power_kind, power))

    def test_accumulation_survives_overflow(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO user_xiuxian(user_id,exp,power) VALUES(?,?,?)",
                ("u", HUGE, 0),
            )
            uow.execute(
                "UPDATE user_xiuxian SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL)"
                " WHERE user_id=?",
                (10**22, "u"),
            )

        kind, value, _power_kind, _power = self.row()
        self.assertEqual("real", kind)
        self.assertEqual(float(HUGE) + 1e22, value)

    def test_failed_transaction_rolls_the_oversized_write_back(self) -> None:
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TABLE guarded(user_id TEXT PRIMARY KEY, exp NUMERIC)"
            )
            connection.execute(
                "CREATE TRIGGER reject BEFORE INSERT ON guarded"
                " BEGIN SELECT RAISE(ABORT,'reject'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                uow.execute(
                    "INSERT INTO guarded(user_id,exp) VALUES(?,?)", ("u", HUGE)
                )

        with sqlite3.connect(self.database) as connection:
            self.assertEqual(
                0, connection.execute("SELECT COUNT(*) FROM guarded").fetchone()[0]
            )


if __name__ == "__main__":
    unittest.main()
