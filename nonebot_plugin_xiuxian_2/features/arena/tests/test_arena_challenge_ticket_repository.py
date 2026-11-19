import tempfile
import unittest
from pathlib import Path

from ..repository import ArenaChallengePurchaseSqlRepository
from tests.test_db_backend import db_backend


class ArenaChallengeTicketRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO back VALUES('u',9,3,3)")
            conn.execute("CREATE TABLE arena_challenge_ticket_operations(operation_id TEXT PRIMARY KEY,payload TEXT,used_tickets INTEGER,item_remaining INTEGER,challenges_used INTEGER,challenges_remaining INTEGER,challenge_cap INTEGER)")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE arena(user_id TEXT PRIMARY KEY,daily_challenges_used INTEGER,daily_extra_challenges INTEGER)")
            conn.execute("INSERT INTO arena VALUES('u',2,1)")

    def tearDown(self):
        self.temp.cleanup()

    def use(self, operation="op", **values):
        params = dict(item_id=9, requested_count=1, expected_item_count=3, expected_challenges_used=2, expected_extra_challenges=1, challenge_cap=11)
        params.update(values)
        return self.repo.use_challenge_ticket(operation, "u", **params)

    def test_success_duplicate_and_conflict(self):
        self.repo = ArenaChallengePurchaseSqlRepository(self.game, self.player)
        first = self.use()
        duplicate = self.use()
        conflict = self.use(requested_count=2)
        self.assertEqual((first["status"], duplicate["status"], conflict["status"]), ("applied", "duplicate", "operation_conflict"))

    def test_item_missing_and_trigger_rollback(self):
        self.repo = ArenaChallengePurchaseSqlRepository(self.game, self.player)
        self.assertEqual("item_missing", self.use("missing", item_id=10, expected_item_count=0)["status"])
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_ticket BEFORE INSERT ON arena_challenge_ticket_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):
            self.use("rollback")
        with db_backend.connection(self.game) as conn:
            self.assertEqual((3, 3), tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone()))
        with db_backend.connection(self.player) as conn:
            self.assertEqual((2, 1), tuple(conn.execute("SELECT daily_challenges_used,daily_extra_challenges FROM arena").fetchone()))

    def test_receipt_ignore_rolls_back_both_databases_with_specific_reason(self):
        self.repo = ArenaChallengePurchaseSqlRepository(self.game, self.player)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER refuse_receipt BEFORE INSERT ON arena_challenge_ticket_operations BEGIN SELECT RAISE(IGNORE); END")

        result = self.use("receipt-ignore")

        self.assertEqual(("state_changed", "arena_ticket_receipt_write_rejected", "receipt"), tuple(result[key] for key in ("status", "reason", "subject")))
        with db_backend.connection(self.game) as conn:
            self.assertEqual((3, 3, 0), tuple(conn.execute("SELECT goods_num,bind_num,(SELECT COUNT(*) FROM arena_challenge_ticket_operations) FROM back").fetchone()))
        with db_backend.connection(self.player) as conn:
            self.assertEqual((2, 1), tuple(conn.execute("SELECT daily_challenges_used,daily_extra_challenges FROM arena").fetchone()))

    def test_receipt_after_trigger_rewrite_rolls_back_and_recovers(self):
        self.repo = ArenaChallengePurchaseSqlRepository(self.game, self.player)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER rewrite_receipt AFTER INSERT ON arena_challenge_ticket_operations BEGIN UPDATE arena_challenge_ticket_operations SET item_remaining=99 WHERE operation_id=NEW.operation_id; END")

        result = self.use("receipt-rewrite")

        self.assertEqual(("state_changed", "arena_ticket_receipt_write_rejected"), tuple(result[key] for key in ("status", "reason")))
        with db_backend.connection(self.game) as conn:
            self.assertEqual((3, 0), tuple(conn.execute("SELECT goods_num,(SELECT COUNT(*) FROM arena_challenge_ticket_operations) FROM back").fetchone()))
        with db_backend.transaction(self.game) as conn:
            conn.execute("DROP TRIGGER rewrite_receipt")
        recovered = self.use("receipt-rewrite")
        self.assertEqual(("applied", 1), (recovered["status"], recovered["used_tickets"]))

    def test_item_and_count_after_triggers_are_rejected_before_receipt(self):
        for target, reason, database, trigger in (
            ("item", "arena_ticket_item_write_rejected", self.game, "CREATE TRIGGER rewrite_item AFTER UPDATE ON back BEGIN UPDATE back SET goods_num=goods_num+1 WHERE user_id='u' AND goods_id=9; END"),
            ("arena", "arena_ticket_count_write_rejected", self.player, "CREATE TRIGGER rewrite_count AFTER UPDATE ON arena BEGIN UPDATE arena SET daily_extra_challenges=daily_extra_challenges+1 WHERE user_id='u'; END"),
        ):
            with self.subTest(target=target):
                self.repo = ArenaChallengePurchaseSqlRepository(self.game, self.player)
                with db_backend.transaction(database) as conn:
                    conn.execute(trigger)
                result = self.use(f"{target}-rewrite")
                self.assertEqual(("state_changed", reason, target), tuple(result[key] for key in ("status", "reason", "subject")))
                with db_backend.connection(self.game) as conn:
                    self.assertEqual((3, 0), tuple(conn.execute("SELECT goods_num,(SELECT COUNT(*) FROM arena_challenge_ticket_operations) FROM back").fetchone()))
                with db_backend.connection(self.player) as conn:
                    self.assertEqual((2, 1), tuple(conn.execute("SELECT daily_challenges_used,daily_extra_challenges FROM arena").fetchone()))
                with db_backend.transaction(database) as conn:
                    conn.execute("DROP TRIGGER rewrite_item" if target == "item" else "DROP TRIGGER rewrite_count")


if __name__ == "__main__":
    unittest.main()
