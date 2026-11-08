"""High-realm 虚神界出关 settlement must finish (SQLite int64 boundary).

Production traceback (xiu2.log 12:12-17:43):

    UPDATE user_xiuxian SET exp=COALESCE(exp,0)+?,hp=?,mp=?,atk=?,power=?
    OverflowError: Python int too large to convert to SQLite INTEGER

Trigger: a player whose 修为/战力 already exceeds int64 (stored REAL) produces a
derived ``power`` int outside int64.  The whole transaction aborted, so the
player could never leave 打坐 (type=4) and the settlement receipt was lost.

These cases use the real production repository with the deployed column
affinities (``user_xiuxian`` NUMERIC) and pin:

* normal and high-realm settlement both finish as ``applied``;
* the receipt returns the exact Python int reward (nothing is clamped);
* storage beyond int64 is SQLite REAL (double) — the disclosed historic
  precision limit, not a silent clamp to int64;
* duplicate/retried requests replay the stored reward without granting again or
  consuming 祝福 twice;
* a conflicting operation id and a stale state change write nothing;
* a failing statement rolls the whole settlement back.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.numeric import adapt_sqlite_params
from nonebot_plugin_xiuxian_2.features.impart_pk.closing_settlement_repository import (
    ImpartClosingSettlementSqlRepository,
)

SETTLEMENT_FIELDS = ("虚神界闭关时长", "虚神界闭关修为", "虚神界闭关祝福时长")
HUGE_EXP = 10**25 + 7
HUGE_GAIN = 10**22 + 9
HUGE_POWER = 10**25 + 12345


def _statistics_ddl() -> str:
    columns = ",".join(f'"{field}" INTEGER DEFAULT 0' for field in SETTLEMENT_FIELDS)
    return f"CREATE TABLE statistics(user_id TEXT PRIMARY KEY,{columns})"


class ClosingSettlementBigIntegerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.impart, self.player = (
            root / "game.db",
            root / "impart.db",
            root / "player.db",
        )
        with sqlite3.connect(self.game) as connection:
            # Production affinity: exp/power are NUMERIC, not INTEGER.
            connection.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp NUMERIC,"
                "hp NUMERIC,mp NUMERIC,atk NUMERIC,power NUMERIC)"
            )
            connection.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,"
                "create_time TEXT,scheduled_time TEXT)"
            )
            connection.execute(
                "CREATE TABLE impart_closing_operations(operation_id TEXT PRIMARY KEY,"
                "payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
        with sqlite3.connect(self.impart) as connection:
            connection.execute(
                "CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER)"
            )
        with sqlite3.connect(self.player) as connection:
            connection.execute(_statistics_ddl())
        self.repository = ImpartClosingSettlementSqlRepository(
            self.game, self.impart, self.player
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    # --- fixtures ---------------------------------------------------------
    def seed(self, *, exp, power, exp_day=30) -> None:
        with sqlite3.connect(self.game) as connection:
            # Mirror the app: oversized ints reach the driver through the shared
            # adapter, which is what leaves them stored as REAL in NUMERIC columns.
            connection.execute(
                "INSERT INTO user_xiuxian(user_id,exp,hp,mp,atk,power)"
                " VALUES(?,?,?,?,?,?)",
                adapt_sqlite_params(("u", exp, 1, 2, 3, power)),
            )
            connection.execute("INSERT INTO user_cd VALUES('u',4,'start',NULL)")
        with sqlite3.connect(self.impart) as connection:
            connection.execute("INSERT INTO xiuxian_impart VALUES('u',?)", (exp_day,))

    def settle(self, operation_id="op", *, user="u", create_time="start",
               exp, exp_day=30, gain=20, cost=10, minutes=60, hp=40, mp=50,
               atk=6, power=999):
        return self.repository.settle(
            operation_id, user, create_time, exp, exp_day, gain, cost, minutes,
            hp, mp, atk, power,
        )

    def user_row(self) -> tuple:
        with sqlite3.connect(self.game) as connection:
            return connection.execute(
                "SELECT typeof(exp),exp,typeof(power),power,hp,mp,atk"
                " FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()

    def cd_type(self) -> int:
        with sqlite3.connect(self.game) as connection:
            return connection.execute(
                "SELECT type FROM user_cd WHERE user_id='u'"
            ).fetchone()[0]

    def blessing(self) -> int:
        with sqlite3.connect(self.impart) as connection:
            return connection.execute(
                "SELECT exp_day FROM xiuxian_impart WHERE user_id='u'"
            ).fetchone()[0]

    def statistics(self):
        with sqlite3.connect(self.player) as connection:
            row = connection.execute(
                "SELECT " + ",".join(f'"{f}"' for f in SETTLEMENT_FIELDS)
                + " FROM statistics WHERE user_id='u'"
            ).fetchone()
        return None if row is None else tuple(row)

    # --- cases ------------------------------------------------------------
    def test_normal_realm_settlement_stays_exact_integers(self) -> None:
        self.seed(exp=100, power=4)

        result = self.settle(exp=100)

        self.assertEqual("applied", result.status)
        self.assertEqual(20, result.exp_gain)
        self.assertEqual(10, result.blessing_cost)
        self.assertEqual(20, result.exp_day_remaining)
        self.assertEqual(("integer", 120, "integer", 999, 40, 50, 6), self.user_row())
        self.assertEqual(0, self.cd_type())
        self.assertEqual(20, self.blessing())
        self.assertEqual((60, 20, 10), self.statistics())

    def test_high_realm_settlement_finishes_without_overflow(self) -> None:
        self.seed(exp=HUGE_EXP, power=10**24)

        result = self.settle(exp=HUGE_EXP, gain=HUGE_GAIN, power=HUGE_POWER)

        self.assertEqual("applied", result.status)
        # Receipt keeps the exact Python int; nothing is clamped to int64.
        self.assertEqual(HUGE_GAIN, result.exp_gain)
        self.assertEqual(20, result.exp_day_remaining)
        kind, stored_exp, power_kind, stored_power, hp, mp, atk = self.user_row()
        # NUMERIC affinity cannot hold >int64 as INTEGER: the value is kept as
        # REAL (double), which is the disclosed historic precision limit.
        self.assertEqual("real", kind)
        self.assertEqual(float(HUGE_EXP) + float(HUGE_GAIN), stored_exp)
        self.assertEqual("real", power_kind)
        self.assertEqual(float(HUGE_POWER), stored_power)
        self.assertEqual((40, 50, 6), (hp, mp, atk))
        self.assertEqual(0, self.cd_type())
        self.assertEqual(20, self.blessing())
        # The statistics column is INTEGER affinity: beyond int64 it keeps REAL.
        self.assertEqual((60, float(HUGE_GAIN), 10), self.statistics())

    def test_high_realm_retry_replays_without_double_reward_or_blessing(self) -> None:
        self.seed(exp=HUGE_EXP, power=10**24)
        first = self.settle(exp=HUGE_EXP, gain=HUGE_GAIN, power=HUGE_POWER)
        self.assertEqual("applied", first.status)
        before = self.user_row()
        blessed_before = self.blessing()

        # Same operation id, different numbers: identity wins, stored reward replays.
        replay = self.settle(exp=HUGE_EXP, gain=HUGE_GAIN + 5, power=HUGE_POWER)

        self.assertEqual("duplicate", replay.status)
        self.assertEqual(first.exp_gain, replay.exp_gain)
        self.assertEqual(first.blessing_cost, replay.blessing_cost)
        self.assertEqual(first.exp_day_remaining, replay.exp_day_remaining)
        self.assertEqual(before, self.user_row())
        self.assertEqual(blessed_before, self.blessing())

    def test_conflicting_operation_payload_is_rejected_without_writes(self) -> None:
        self.seed(exp=HUGE_EXP, power=10**24)
        self.assertEqual(
            "applied",
            self.settle(exp=HUGE_EXP, gain=HUGE_GAIN, power=HUGE_POWER).status,
        )
        before, blessed_before = self.user_row(), self.blessing()

        conflict = self.repository.settle(
            "op", "u", "other-session", HUGE_EXP, 30, HUGE_GAIN, 10, 60,
            40, 50, 6, HUGE_POWER,
        )

        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual(before, self.user_row())
        self.assertEqual(blessed_before, self.blessing())

    def test_stale_state_change_writes_nothing(self) -> None:
        self.seed(exp=HUGE_EXP, power=10**24)
        before, blessed_before = self.user_row(), self.blessing()

        stale = self.settle(exp=HUGE_EXP, exp_day=99, gain=HUGE_GAIN,
                            power=HUGE_POWER)

        self.assertEqual("state_changed", stale.status)
        self.assertEqual(before, self.user_row())
        self.assertEqual(4, self.cd_type())
        self.assertEqual(blessed_before, self.blessing())
        with sqlite3.connect(self.game) as connection:
            self.assertEqual(
                0,
                connection.execute(
                    "SELECT COUNT(*) FROM impart_closing_operations"
                ).fetchone()[0],
            )

    def test_failing_statement_rolls_back_the_whole_settlement(self) -> None:
        self.seed(exp=HUGE_EXP, power=10**24)
        before, blessed_before = self.user_row(), self.blessing()
        with sqlite3.connect(self.game) as connection:
            connection.execute(
                "CREATE TRIGGER reject_close BEFORE INSERT ON impart_closing_operations"
                " BEGIN SELECT RAISE(ABORT,'reject'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.settle(exp=HUGE_EXP, gain=HUGE_GAIN, power=HUGE_POWER)

        self.assertEqual(before, self.user_row())
        self.assertEqual(4, self.cd_type())
        self.assertEqual(blessed_before, self.blessing())
        # No statistics row at all: the increment never survived the rollback.
        self.assertIsNone(self.statistics())

    def test_receipt_payload_is_json_ints_not_text(self) -> None:
        self.seed(exp=HUGE_EXP, power=10**24)
        self.settle(exp=HUGE_EXP, gain=HUGE_GAIN, power=HUGE_POWER)

        with sqlite3.connect(self.game) as connection:
            stored = connection.execute(
                "SELECT result_json FROM impart_closing_operations"
                " WHERE operation_id='op'"
            ).fetchone()[0]

        self.assertEqual([HUGE_GAIN, 10, 20], json.loads(stored))


if __name__ == "__main__":
    unittest.main()
