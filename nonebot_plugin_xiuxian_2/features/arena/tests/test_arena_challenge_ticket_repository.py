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


if __name__ == "__main__":
    unittest.main()
