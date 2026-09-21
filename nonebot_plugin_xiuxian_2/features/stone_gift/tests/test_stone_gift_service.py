from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import StoneGiftApplication
from ..migrations import apply_stone_gift


class StoneGiftApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            uow.execute("INSERT INTO user_xiuxian VALUES (?, ?)", ("sender", 1000))
            uow.execute("INSERT INTO user_xiuxian VALUES (?, ?)", ("recipient", 100))
            apply_platform_schema(uow)
            apply_stone_gift(uow)
        self.application = StoneGiftApplication(self.database)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def balances(self) -> tuple[int, int]:
        with DatabaseUnitOfWork(self.database) as uow:
            rows = uow.query_all("SELECT user_id, stone FROM user_xiuxian ORDER BY user_id")
        values = {str(row["user_id"]): int(row["stone"]) for row in rows}
        return values["sender"], values["recipient"]

    def test_transfer_records_assets_and_audit(self) -> None:
        result = self.application.transfer(
            operation_id="gift-1", sender_id="sender", recipient_id="recipient", gross_amount=500
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.consumed["sender_stone"], 500)
        self.assertEqual(result.granted["recipient_stone"], 450)
        self.assertEqual(result.before["sender"]["stone"], 1000)
        self.assertEqual(result.after["recipient"]["stone"], 550)
        self.assertEqual(self.balances(), (500, 550))

    def test_same_operation_replays_without_double_charge(self) -> None:
        first = self.application.transfer(
            operation_id="gift-repeat", sender_id="sender", recipient_id="recipient", gross_amount=500
        )
        second = self.application.transfer(
            operation_id="gift-repeat", sender_id="sender", recipient_id="recipient", gross_amount=500
        )
        self.assertEqual((first.status, second.status), ("applied", "replayed"))
        self.assertEqual(second.data, first.data)
        self.assertEqual(self.balances(), (500, 550))

    def test_business_rejections_do_not_mutate_assets(self) -> None:
        poor = self.application.transfer(
            operation_id="gift-poor", sender_id="sender", recipient_id="recipient", gross_amount=1001
        )
        missing = self.application.transfer(
            operation_id="gift-missing", sender_id="sender", recipient_id="missing", gross_amount=500
        )
        self.assertEqual((poor.code, missing.code), ("stone_insufficient", "recipient_missing"))
        self.assertEqual(self.balances(), (1000, 100))

    def test_daily_limits_are_atomic_and_replayed(self) -> None:
        first = self.application.transfer(
            operation_id="gift-limit-1",
            sender_id="sender",
            recipient_id="recipient",
            gross_amount=500,
            transfer_date="2026-09-12",
            send_limit=500,
            receive_limit=450,
        )
        rejected = self.application.transfer(
            operation_id="gift-limit-2",
            sender_id="sender",
            recipient_id="recipient",
            gross_amount=1,
            transfer_date="2026-09-12",
            send_limit=500,
            receive_limit=450,
        )
        replay = self.application.transfer(
            operation_id="gift-limit-2",
            sender_id="sender",
            recipient_id="recipient",
            gross_amount=1,
            transfer_date="2026-09-12",
            send_limit=500,
            receive_limit=450,
        )
        self.assertEqual(first.status, "applied")
        self.assertEqual((rejected.code, replay.status), ("send_limit_reached", "rejected"))
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                "SELECT sent_amount, received_amount FROM stone_gift_limits WHERE limit_date = ? AND user_id = ?",
                ("2026-09-12", "sender"),
            )
            self.assertEqual((row["sent_amount"], row["received_amount"]), (500, 0))

    def test_legacy_counter_is_seeded_once_before_new_projection(self) -> None:
        result = self.application.transfer(
            operation_id="gift-legacy-counter",
            sender_id="sender",
            recipient_id="recipient",
            gross_amount=100,
            transfer_date="2026-09-12",
            send_limit=600,
            receive_limit=550,
            send_used=500,
            receive_used=450,
        )
        self.assertTrue(result.ok)
        with DatabaseUnitOfWork(self.database) as uow:
            sender = uow.query_one(
                "SELECT sent_amount FROM stone_gift_limits WHERE limit_date = ? AND user_id = ?",
                ("2026-09-12", "sender"),
            )
            recipient = uow.query_one(
                "SELECT received_amount FROM stone_gift_limits WHERE limit_date = ? AND user_id = ?",
                ("2026-09-12", "recipient"),
            )
            self.assertEqual(sender["sent_amount"], 600)
            self.assertEqual(recipient["received_amount"], 540)

    def test_insert_failure_rolls_back_both_balances(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            apply_stone_gift(uow)
            uow.execute(
                "CREATE TRIGGER fail_stone_gift BEFORE INSERT ON stone_gift_operations "
                "BEGIN SELECT RAISE(ABORT, 'gift failed'); END"
            )
        with self.assertRaises(Exception):
            self.application.transfer(
                operation_id="gift-fail", sender_id="sender", recipient_id="recipient", gross_amount=500
            )
        self.assertEqual(self.balances(), (1000, 100))


if __name__ == "__main__":
    unittest.main()
