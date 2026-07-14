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
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL, user_stamina INTEGER NOT NULL)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("payer", 100, 20))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("receiver", 20, 20))
        self.service = StoneContestService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def balances(self):
        with db_backend.connection(self.database) as conn:
            return tuple(
                int(row[0])
                for row in conn.execute(
                    "SELECT stone FROM user_xiuxian "
                    "WHERE user_id IN ('payer','receiver') ORDER BY user_id"
                ).fetchall()
            )

    def stamina(self, user_id):
        with db_backend.connection(self.database) as conn:
            return int(
                conn.execute(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()[0]
            )

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

    def test_successful_theft_transfers_stones_and_charges_stamina_once(self) -> None:
        result = self.service.settle_theft(
            "theft-win",
            "receiver",
            "payer",
            outcome="success",
            requested_amount=30,
            penalty_amount=10,
            stamina_cost=10,
        )
        self.assertEqual(
            (result.status, result.outcome, result.transferred_amount, result.payer_balance),
            ("settled", "success", 30, 70),
        )
        self.assertEqual(self.balances(), (70, 50))
        self.assertEqual(self.stamina("receiver"), 10)

    def test_failed_theft_pays_penalty_and_charges_stamina_once(self) -> None:
        result = self.service.settle_theft(
            "theft-failure",
            "payer",
            "receiver",
            outcome="failure",
            requested_amount=30,
            penalty_amount=30,
            stamina_cost=10,
        )
        self.assertEqual(
            (result.status, result.outcome, result.transferred_amount, result.payer_balance),
            ("settled", "failure", 30, 70),
        )
        self.assertEqual(self.balances(), (70, 50))
        self.assertEqual(self.stamina("payer"), 10)

    def test_theft_with_insufficient_stamina_does_not_transfer(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s",
                (5, "receiver"),
            )
        result = self.service.settle_theft(
            "theft-tired",
            "receiver",
            "payer",
            outcome="success",
            requested_amount=30,
            penalty_amount=10,
            stamina_cost=10,
        )
        self.assertEqual(result.status, "stamina_insufficient")
        self.assertEqual(self.balances(), (100, 20))
        self.assertEqual(self.stamina("receiver"), 5)

    def test_theft_rejects_missing_preparation_stones_without_stamina_cost(self) -> None:
        result = self.service.settle_theft(
            "theft-poor",
            "receiver",
            "payer",
            outcome="failure",
            requested_amount=30,
            penalty_amount=30,
            stamina_cost=10,
        )
        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.balances(), (100, 20))
        self.assertEqual(self.stamina("receiver"), 20)

    def test_theft_rejects_empty_victim_without_stamina_cost(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=0 WHERE user_id=%s", ("payer",))
        result = self.service.settle_theft(
            "theft-empty",
            "receiver",
            "payer",
            outcome="success",
            requested_amount=10,
            penalty_amount=10,
            stamina_cost=10,
        )
        self.assertEqual(result.status, "payer_empty")
        self.assertEqual(self.balances(), (0, 20))
        self.assertEqual(self.stamina("receiver"), 20)

    def test_theft_replay_returns_first_random_result(self) -> None:
        first = self.service.settle_theft(
            "theft-repeat",
            "receiver",
            "payer",
            outcome="success",
            requested_amount=30,
            penalty_amount=10,
            stamina_cost=10,
        )
        second = self.service.settle_theft(
            "theft-repeat",
            "receiver",
            "payer",
            outcome="failure",
            requested_amount=10,
            penalty_amount=10,
            stamina_cost=10,
        )
        replay = self.service.replay_theft("theft-repeat", "receiver", "payer")
        self.assertEqual(first.status, "settled")
        self.assertEqual(
            (second.status, second.outcome, second.requested_amount, second.transferred_amount),
            ("duplicate", "success", 30, 30),
        )
        self.assertEqual(replay, second)
        self.assertEqual(self.balances(), (70, 50))
        self.assertEqual(self.stamina("receiver"), 10)

    def test_theft_replay_rejects_changed_participants(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("third", 40, 20))
        self.service.settle_theft(
            "theft-conflict",
            "receiver",
            "payer",
            outcome="success",
            requested_amount=30,
            penalty_amount=10,
            stamina_cost=10,
        )
        changed_target = self.service.settle_theft(
            "theft-conflict",
            "receiver",
            "third",
            outcome="failure",
            requested_amount=10,
            penalty_amount=10,
            stamina_cost=10,
        )
        changed_user = self.service.replay_theft("theft-conflict", "third", "payer")
        self.assertEqual(changed_target.status, "operation_conflict")
        self.assertEqual(changed_user.status, "operation_conflict")
        self.assertEqual(self.balances(), (70, 50))

    def test_theft_operation_failure_rolls_back_stones_and_stamina(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE stone_contest_operations ("
                "operation_id TEXT PRIMARY KEY,payer_id TEXT NOT NULL,receiver_id TEXT NOT NULL,"
                "requested_amount INTEGER NOT NULL,transferred_amount INTEGER NOT NULL,"
                "payer_balance INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_theft_operation BEFORE INSERT ON stone_contest_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.settle_theft(
                "theft-write-fail",
                "receiver",
                "payer",
                outcome="success",
                requested_amount=30,
                penalty_amount=10,
                stamina_cost=10,
            )
        self.assertEqual(self.balances(), (100, 20))
        self.assertEqual(self.stamina("receiver"), 20)

    def test_old_operation_table_is_migrated_without_losing_transfers(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE stone_contest_operations ("
                "operation_id TEXT PRIMARY KEY,payer_id TEXT NOT NULL,receiver_id TEXT NOT NULL,"
                "requested_amount INTEGER NOT NULL,transferred_amount INTEGER NOT NULL,"
                "payer_balance INTEGER NOT NULL)"
            )
            conn.execute(
                "INSERT INTO stone_contest_operations VALUES (%s,%s,%s,%s,%s,%s)",
                ("legacy-transfer", "payer", "receiver", 5, 5, 95),
            )
        legacy = self.service.transfer("legacy-transfer", "payer", "receiver", 5)
        theft = self.service.settle_theft(
            "migrated-theft",
            "receiver",
            "payer",
            outcome="success",
            requested_amount=10,
            penalty_amount=10,
            stamina_cost=10,
        )
        with db_backend.connection(self.database) as conn:
            columns = set(conn.column_names("stone_contest_operations"))
        self.assertEqual(legacy.status, "duplicate")
        self.assertEqual(theft.status, "settled")
        self.assertTrue(
            {"operation_type", "thief_id", "victim_id", "outcome", "stamina_cost"}
            <= columns
        )


if __name__ == "__main__":
    unittest.main()
