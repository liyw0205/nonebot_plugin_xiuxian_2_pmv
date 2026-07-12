from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.stone_contest_service import StoneContestService
from tests.test_db_backend import db_backend


class StoneContestServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("payer", 100))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("receiver", 20))
        self.service = StoneContestService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def balances(self):
        with db_backend.connection(self.database) as conn:
            return tuple(int(row[0]) for row in conn.execute("SELECT stone FROM user_xiuxian ORDER BY user_id").fetchall())

    def test_transfers_stones_atomically(self) -> None:
        result = self.service.transfer("contest-1", "payer", "receiver", 30)
        self.assertEqual((result.status, result.transferred_amount, result.payer_balance), ("transferred", 30, 70))
        self.assertEqual(self.balances(), (70, 50))

    def test_caps_transfer_at_live_payer_balance(self) -> None:
        result = self.service.transfer("contest-cap", "payer", "receiver", 150)
        self.assertEqual((result.transferred_amount, result.payer_balance), (100, 0))
        self.assertEqual(self.balances(), (0, 120))

    def test_duplicate_does_not_transfer_twice(self) -> None:
        first = self.service.transfer("contest-repeat", "payer", "receiver", 30)
        second = self.service.transfer("contest-repeat", "payer", "receiver", 30)
        self.assertEqual((first.status, second.status), ("transferred", "duplicate"))
        self.assertEqual(self.balances(), (70, 50))

    def test_changed_duplicate_is_rejected(self) -> None:
        self.service.transfer("contest-conflict", "payer", "receiver", 30)
        result = self.service.transfer("contest-conflict", "payer", "receiver", 40)
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.balances(), (70, 50))

    def test_missing_user_or_empty_payer_changes_nothing(self) -> None:
        missing = self.service.transfer("contest-missing", "payer", "missing", 30)
        self.service.transfer("contest-drain", "payer", "receiver", 100)
        empty = self.service.transfer("contest-empty", "payer", "receiver", 1)
        self.assertEqual((missing.status, empty.status), ("user_missing", "payer_empty"))
        self.assertEqual(self.balances(), (0, 120))

    def test_operation_failure_rolls_back_both_balances(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE stone_contest_operations (operation_id TEXT PRIMARY KEY, payer_id TEXT NOT NULL, receiver_id TEXT NOT NULL, requested_amount INTEGER NOT NULL, transferred_amount INTEGER NOT NULL, payer_balance INTEGER NOT NULL)")
            conn.execute("CREATE TRIGGER fail_contest_operation BEFORE INSERT ON stone_contest_operations BEGIN SELECT RAISE(ABORT, 'operation failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.transfer("contest-write-fail", "payer", "receiver", 30)
        self.assertEqual(self.balances(), (100, 20))


if __name__ == "__main__":
    unittest.main()
