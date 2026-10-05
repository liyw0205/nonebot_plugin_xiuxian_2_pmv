import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import (
    apply_dufang_bet_payout,
    apply_dufang_player_receipts,
    apply_dufang_resolution,
    apply_dufang_share_player,
)
from ..bet_repository import DufangBetSqlRepository
from ..player_stats_repository import DufangPlayerStatsSqlRepository


WIN_PLAN = {"payout_outcome": "win", "gain": 50, "requested_loss": 0}


class DufangBetRepositoryTests(unittest.TestCase):
    def test_stats_snapshot_reads_without_creating_missing_user(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(player) as uow:
                apply_dufang_share_player(uow)
                apply_dufang_player_receipts(uow)
                uow.execute(
                    "INSERT INTO unseal_data(user_id,count,total_cost,profit,loss,"
                    "shared_profit,shared_loss,received_profit,received_loss,last_update) "
                    "VALUES('u',2,300,40,5,6,7,8,9,'2026-07-12 10:00:00')"
                )

            repository = DufangPlayerStatsSqlRepository(game, player)
            existing = repository.snapshot("u")
            missing = repository.snapshot("missing")

            self.assertEqual(existing.status, "ok")
            self.assertEqual(existing.legacy_data()["unseal_info"], {
                "count": 2,
                "total_cost": 300,
                "profit": 40,
                "loss": 5,
            })
            self.assertEqual(existing.legacy_data()["sharing_info"], {
                "shared_profit": 6,
                "shared_loss": 7,
                "received_profit": 8,
                "received_loss": 9,
            })
            self.assertEqual(missing.status, "ok")
            self.assertEqual(missing.unseal_info, {"count": 0, "total_cost": 0, "profit": 0, "loss": 0})
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS count FROM unseal_data")["count"],
                    1,
                )

    def test_stats_snapshot_fails_closed_without_database_or_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "missing-player.db"
            repository = DufangPlayerStatsSqlRepository(game, player)
            self.assertEqual(repository.snapshot("u").status, "schema_missing")
            self.assertFalse(player.exists())

            player.touch()
            self.assertEqual(repository.snapshot("u").status, "schema_missing")
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one(
                        "SELECT COUNT(*) AS count FROM sqlite_master "
                        "WHERE type='table' AND name='unseal_data'"
                    )["count"],
                    0,
                )

            with DatabaseUnitOfWork(player) as uow:
                apply_dufang_share_player(uow)
            self.assertEqual(repository.snapshot("u").status, "schema_missing")

    def test_place_replay_and_insufficient(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
                apply_dufang_bet_payout(uow)
                apply_dufang_resolution(uow)
            with DatabaseUnitOfWork(player) as uow:
                apply_dufang_share_player(uow)
                apply_dufang_player_receipts(uow)
                uow.execute("INSERT INTO unseal_data(user_id,count,total_cost,last_update) VALUES('u',0,0,'')")
            repo = DufangBetSqlRepository(game)
            first = repo.place("b1", "u", 30, "now", WIN_PLAN)
            duplicate = repo.place("b1", "u", 30, "now", WIN_PLAN)
            insufficient = repo.place("b2", "u", 100, "later", WIN_PLAN)
            self.assertEqual((first.status, duplicate.status, insufficient.status), ("applied", "duplicate", "stone_insufficient"))
            self.assertEqual(first.resolution, duplicate.resolution)
            self.assertEqual(repo.total_cost("u"), 30)

    def test_missing_schema_fails_without_creating_player_database_or_tables(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "missing-player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            result = DufangBetSqlRepository(game).place("b1", "u", 30, "now", WIN_PLAN)
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
                apply_dufang_resolution(uow)
            with DatabaseUnitOfWork(player) as uow:
                apply_dufang_share_player(uow)
                apply_dufang_player_receipts(uow)
                uow.execute("INSERT INTO unseal_data(user_id,count,total_cost,last_update) VALUES('u',0,0,'')")
            repo = DufangBetSqlRepository(game)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: repo.place("same", "u", 30, "now", WIN_PLAN), range(2)))
            self.assertEqual(sorted(result.status for result in results), ["applied", "duplicate"])
            DufangPlayerStatsSqlRepository(game, player).reconcile(limit=5)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"]
                count = uow.query_one("SELECT COUNT(*) AS count FROM dufang_bets")["count"]
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                stats = uow.query_one("SELECT count,total_cost FROM unseal_data WHERE user_id='u'")
            self.assertEqual((wallet, count, stats["count"], stats["total_cost"]), (70, 1, 1, 30))
