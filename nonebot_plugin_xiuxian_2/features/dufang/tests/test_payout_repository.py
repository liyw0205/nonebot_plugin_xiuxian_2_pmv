import tempfile
import unittest
from pathlib import Path

from ..payout_repository import DufangPayoutSqlRepository
from tests.test_db_backend import db_backend


class DufangPayoutRepositoryTests(unittest.TestCase):
    def test_payout_replay_and_pending_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
                conn.execute("CREATE TABLE dufang_bets(bet_id TEXT PRIMARY KEY,user_id TEXT,cost INTEGER,status TEXT,placed_at TEXT,settled_at TEXT)")
                conn.execute("INSERT INTO dufang_bets VALUES('bet','u',20,'pending','now',NULL)")
            with db_backend.transaction(player) as conn:
                conn.execute("CREATE TABLE unseal_data(user_id TEXT PRIMARY KEY,count INTEGER,total_cost INTEGER,profit INTEGER,loss INTEGER,last_update TEXT)")
                conn.execute("INSERT INTO unseal_data VALUES('u',1,20,0,0,'')")
            repo = DufangPayoutSqlRepository(game, player)
            first = repo.settle("p1", "bet", "u", "win", 50, 0, "end")
            duplicate = repo.settle("p1", "bet", "u", "win", 50, 0, "end")
            stale = repo.settle("p2", "bet", "u", "win", 50, 0, "later")
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
