from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.stone_gift_service import (
    StoneGiftService,
)
from tests.test_db_backend import db_backend


class StoneGiftServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "stone-gift.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("sender", 1000))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("recipient", 100))
        self.service = StoneGiftService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def balances(self) -> tuple[int, int]:
        with db_backend.connection(self.database) as conn:
            rows = conn.execute(
                "SELECT user_id, stone FROM user_xiuxian ORDER BY user_id"
            ).fetchall()
        values = {str(user_id): int(stone) for user_id, stone in rows}
        return values["sender"], values["recipient"]

    def operation_count(self) -> int:
        with db_backend.connection(self.database) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("stone_gift_operations",),
            ).fetchone()
            if exists is None:
                return 0
            return int(
                conn.execute("SELECT COUNT(*) FROM stone_gift_operations").fetchone()[0]
            )

    def test_transfer_charges_gross_and_credits_net_atomically(self) -> None:
        result = self.service.transfer("gift-success", "sender", "recipient", 500)

        self.assertEqual(result.status, "transferred")
        self.assertEqual(
            (result.gross_amount, result.net_amount, result.fee_amount),
            (500, 450, 50),
        )
        self.assertEqual(self.balances(), (500, 550))
        self.assertEqual(self.operation_count(), 1)

    def test_duplicate_returns_original_result_without_moving_stones(self) -> None:
        first = self.service.transfer("gift-repeat", "sender", "recipient", 500)
        second = self.service.transfer("gift-repeat", "sender", "recipient", 900)
        stored = self.service.get_operation("gift-repeat", "sender", "recipient")

        self.assertEqual(
            (first.status, second.status, stored.status),
            ("transferred", "duplicate", "duplicate"),
        )
        self.assertEqual(
            (second.gross_amount, second.net_amount, second.fee_amount),
            (500, 450, 50),
        )
        self.assertEqual(self.balances(), (500, 550))
        self.assertEqual(self.operation_count(), 1)

    def test_insufficient_balance_does_not_change_either_player(self) -> None:
        result = self.service.transfer("gift-poor", "sender", "recipient", 1001)

        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.balances(), (1000, 100))
        self.assertEqual(self.operation_count(), 0)

    def test_missing_recipient_does_not_charge_sender(self) -> None:
        result = self.service.transfer("gift-missing", "sender", "missing", 500)

        self.assertEqual(result.status, "recipient_missing")
        self.assertEqual(self.balances(), (1000, 100))
        self.assertEqual(self.operation_count(), 0)

    def test_operation_failure_rolls_back_both_balances(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_stone_gift BEFORE INSERT ON stone_gift_operations "
                "BEGIN SELECT RAISE(ABORT, 'gift failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.transfer("gift-fail", "sender", "recipient", 500)
        self.assertEqual(self.balances(), (1000, 100))
        self.assertEqual(self.operation_count(), 0)

    def test_invalid_transfer_parameters_are_rejected(self) -> None:
        for amount in (0, -1):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                self.service.transfer("gift-invalid", "sender", "recipient", amount)
        with self.assertRaises(ValueError):
            self.service.transfer("gift-self", "sender", "sender", 100)
        self.assertEqual(self.balances(), (1000, 100))


if __name__ == "__main__":
    unittest.main()
