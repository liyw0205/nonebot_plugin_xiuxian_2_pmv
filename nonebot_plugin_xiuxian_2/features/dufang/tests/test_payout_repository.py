import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_dufang_bet_payout, apply_dufang_share_player
from ..payout_repository import DufangPayoutSqlRepository


class DufangPayoutRepositoryTests(unittest.TestCase):
    def test_payout_replay_and_pending_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
                apply_dufang_bet_payout(uow)
                uow.execute("INSERT INTO dufang_bets VALUES('bet','u',20,'pending','now',NULL)")
            with DatabaseUnitOfWork(player) as uow:
                apply_dufang_share_player(uow)
                uow.execute("INSERT INTO unseal_data(user_id,count,total_cost,profit,loss,last_update) VALUES('u',1,20,0,0,'')")
            repo = DufangPayoutSqlRepository(game, player)
            first = repo.settle("p1", "bet", "u", "win", 50, 0, "end")
            duplicate = repo.settle("p1", "bet", "u", "win", 50, 0, "end")
            stale = repo.settle("p2", "bet", "u", "win", 50, 0, "later")
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))

    def test_missing_schema_fails_without_creating_player_database_or_tables(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "missing-player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("CREATE TABLE dufang_bets(bet_id TEXT PRIMARY KEY,user_id TEXT,status TEXT,settled_at TEXT)")
            repo = DufangPayoutSqlRepository(game, player)
            self.assertEqual(repo.settle("p1", "bet", "u", "win", 10, 0, "now").status, "schema_missing")
            self.assertEqual(repo.get_result("p1").status, "schema_missing")
            self.assertFalse(player.exists())
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                names = {str(row["name"]) for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("dufang_payout_operations", names)

    def test_missing_player_statistics_rolls_back_game_payout(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
                apply_dufang_bet_payout(uow)
                uow.execute("INSERT INTO dufang_bets VALUES('bet','u',20,'pending','now',NULL)")
            with DatabaseUnitOfWork(player) as uow:
                apply_dufang_share_player(uow)
            repo = DufangPayoutSqlRepository(game, player)
            with self.assertRaises(RuntimeError):
                repo.settle("p1", "bet", "u", "win", 50, 0, "end")
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"]
                status = uow.query_one("SELECT status FROM dufang_bets WHERE bet_id='bet'")["status"]
                payout_count = uow.query_one("SELECT COUNT(*) AS count FROM dufang_payout_operations")["count"]
            self.assertEqual((wallet, status, payout_count), (100, "pending", 0))

    def test_competing_payout_operations_settle_pending_bet_once(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
                apply_dufang_bet_payout(uow)
                uow.execute("INSERT INTO dufang_bets VALUES('bet','u',20,'pending','now',NULL)")
            with DatabaseUnitOfWork(player) as uow:
                apply_dufang_share_player(uow)
                uow.execute("INSERT INTO unseal_data(user_id,count,total_cost,profit,loss,last_update) VALUES('u',1,20,0,0,'')")
            repo = DufangPayoutSqlRepository(game, player)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(
                    lambda operation: repo.settle(operation, "bet", "u", "win", 50, 0, "end"),
                    ("p1", "p2"),
                ))
            self.assertEqual(sorted(result.status for result in results), ["applied", "state_changed"])
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"]
                count = uow.query_one("SELECT COUNT(*) AS count FROM dufang_payout_operations")["count"]
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                profit = uow.query_one("SELECT profit FROM unseal_data WHERE user_id='u'")["profit"]
            self.assertEqual((wallet, count, profit), (150, 1, 50))
