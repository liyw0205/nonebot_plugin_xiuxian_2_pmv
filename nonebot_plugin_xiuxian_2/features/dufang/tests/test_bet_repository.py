import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_dufang_bet_payout, apply_dufang_share_player
from ..bet_repository import DufangBetSqlRepository


class DufangBetRepositoryTests(unittest.TestCase):
    def test_place_replay_and_insufficient(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
                apply_dufang_bet_payout(uow)
            with DatabaseUnitOfWork(player) as uow:
                apply_dufang_share_player(uow)
                uow.execute("INSERT INTO unseal_data(user_id,count,total_cost,last_update) VALUES('u',0,0,'')")
            repo = DufangBetSqlRepository(game, player)
            first = repo.place("b1", "u", 30, "now")
            duplicate = repo.place("b1", "u", 30, "now")
            insufficient = repo.place("b2", "u", 100, "later")
            self.assertEqual((first.status, duplicate.status, insufficient.status), ("applied", "duplicate", "stone_insufficient"))

    def test_missing_schema_fails_without_creating_player_database_or_tables(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "missing-player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            result = DufangBetSqlRepository(game, player).place("b1", "u", 30, "now")
            self.assertEqual(result.status, "schema_missing")
            self.assertFalse(player.exists())
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                names = {str(row["name"]) for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")}
                wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"]
            self.assertEqual(wallet, 100)
            self.assertNotIn("dufang_bets", names)

    def test_concurrent_replay_charges_once(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
                apply_dufang_bet_payout(uow)
            with DatabaseUnitOfWork(player) as uow:
                apply_dufang_share_player(uow)
                uow.execute("INSERT INTO unseal_data(user_id,count,total_cost,last_update) VALUES('u',0,0,'')")
            repo = DufangBetSqlRepository(game, player)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: repo.place("same", "u", 30, "now"), range(2)))
            self.assertEqual(sorted(result.status for result in results), ["applied", "duplicate"])
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"]
                count = uow.query_one("SELECT COUNT(*) AS count FROM dufang_bets")["count"]
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                stats = uow.query_one("SELECT count,total_cost FROM unseal_data WHERE user_id='u'")
            self.assertEqual((wallet, count, stats["count"], stats["total_cost"]), (70, 1, 1, 30))
