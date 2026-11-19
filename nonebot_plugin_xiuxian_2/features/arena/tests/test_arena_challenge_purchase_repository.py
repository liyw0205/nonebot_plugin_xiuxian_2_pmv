import tempfile
import unittest
from datetime import date
from pathlib import Path

from ..repository import ArenaChallengePurchaseSqlRepository
from tests.test_db_backend import db_backend


class ArenaChallengePurchaseRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        root = Path(self.t.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            conn.execute("CREATE TABLE arena_challenge_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,amount INTEGER,cost INTEGER,stone INTEGER,bought INTEGER,extra INTEGER)")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE arena(user_id TEXT PRIMARY KEY,daily_challenge_buys INTEGER,daily_extra_challenges INTEGER,last_buy_date TEXT)")
            conn.execute("INSERT INTO arena VALUES('u',1,1,'2026-09-15')")
        self.repo = ArenaChallengePurchaseSqlRepository(self.game, self.player)

    def tearDown(self):
        self.t.cleanup()

    def buy(self, operation="op", **kwargs):
        values = dict(amount=1, unit_cost=10, daily_limit=3, expected_stone=100, expected_bought=1, expected_extra=1, expected_last_buy_date="2026-09-15", today=date(2026, 9, 15))
        values.update(kwargs)
        return self.repo.purchase_challenges(operation, "u", **values)

    def state(self):
        with db_backend.connection(self.game) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0]
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT daily_challenge_buys,daily_extra_challenges,last_buy_date FROM arena WHERE user_id='u'").fetchone()
        return int(stone), tuple(row)

    def test_success_duplicate_conflict(self):
        first = self.buy()
        duplicate = self.buy()
        conflict = self.buy(amount=2)
        self.assertEqual((first["status"], duplicate["status"], conflict["status"]), ("applied", "duplicate", "state_changed"))
        self.assertEqual(self.state(), (90, (2, 2, "2026-09-15")))

    def test_new_day_and_rejections(self):
        fresh = self.buy("new", expected_stone=100, expected_bought=0, expected_extra=0, expected_last_buy_date="2026-09-14", today=date(2026, 9, 16))
        self.assertEqual(fresh["status"], "applied")
        poor = self.buy("poor", expected_stone=90, expected_bought=1, expected_extra=1, expected_last_buy_date="2026-09-16", unit_cost=101, today=date(2026, 9, 16))
        self.assertEqual(poor["status"], "stone_insufficient")

    def test_operation_trigger_rolls_back_wallet_and_arena(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_arena_operation BEFORE INSERT ON arena_challenge_purchase_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):
            self.buy("rollback")
        self.assertEqual(self.state(), (100, (1, 1, "2026-09-15")))

    def test_receipt_ignore_rolls_back_wallet_and_counters_with_specific_reason(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER refuse_receipt BEFORE INSERT ON arena_challenge_purchase_operations BEGIN SELECT RAISE(IGNORE); END")

        result = self.buy("receipt-ignore")

        self.assertEqual(("state_changed", "arena_challenge_purchase_receipt_write_rejected", "receipt"), tuple(result[key] for key in ("status", "reason", "subject")))
        self.assertEqual(self.state(), (100, (1, 1, "2026-09-15")))

    def test_asset_after_triggers_are_rejected_and_reverted(self):
        for target, reason, database, trigger in (
            ("user", "arena_challenge_purchase_stone_write_rejected", self.game, "CREATE TRIGGER rewrite_stone AFTER UPDATE OF stone ON user_xiuxian BEGIN UPDATE user_xiuxian SET stone=stone+7 WHERE user_id='u'; END"),
            ("arena", "arena_challenge_purchase_state_write_rejected", self.player, "CREATE TRIGGER rewrite_counter AFTER UPDATE OF daily_challenge_buys ON arena BEGIN UPDATE arena SET daily_extra_challenges=daily_extra_challenges+1 WHERE user_id='u'; END"),
        ):
            with self.subTest(target=target):
                with db_backend.transaction(database) as conn:
                    conn.execute(trigger)
                result = self.buy(f"{target}-rewrite")
                self.assertEqual(("state_changed", reason, target), tuple(result[key] for key in ("status", "reason", "subject")))
                self.assertEqual(self.state(), (100, (1, 1, "2026-09-15")))
                with db_backend.transaction(database) as conn:
                    conn.execute("DROP TRIGGER rewrite_stone" if target == "user" else "DROP TRIGGER rewrite_counter")


if __name__ == "__main__":
    unittest.main()
