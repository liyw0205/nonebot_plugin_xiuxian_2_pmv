import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..bet_repository import DufangBetSqlRepository
from ..migrations import (
    apply_dufang_bet_payout,
    apply_dufang_player_receipts,
    apply_dufang_resolution,
    apply_dufang_share_player,
)
from ..payout_repository import DufangPayoutSqlRepository
from ..player_stats_repository import DufangPlayerStatsSqlRepository


WIN_PLAN = {"payout_outcome": "win", "gain": 50, "requested_loss": 0}


class DufangPayoutRepositoryTests(unittest.TestCase):
    def test_player_projection_reports_pending_outbox_after_bounded_batch(self):
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
            bets = DufangBetSqlRepository(game)
            bets.place("bet-1", "u", 10, "one", WIN_PLAN)
            bets.place("bet-2", "u", 10, "two", WIN_PLAN)

            result = DufangPlayerStatsSqlRepository(game, player).reconcile(limit=1)
            self.assertEqual((result.applied, result.pending), (1, 1))

    def test_player_receipt_prevents_double_projection_after_ack_failure(self):
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
            DufangBetSqlRepository(game).place("bet", "u", 20, "now", WIN_PLAN)
            with DatabaseUnitOfWork(game) as uow:
                uow.execute(
                    "CREATE TRIGGER fail_outbox_ack BEFORE UPDATE OF status ON dufang_player_outbox "
                    "BEGIN SELECT RAISE(ABORT,'simulated process interruption'); END"
                )

            projector = DufangPlayerStatsSqlRepository(game, player)
            with self.assertRaises(sqlite3.IntegrityError):
                projector.reconcile(limit=5)
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                stats = uow.query_one("SELECT count,total_cost FROM unseal_data WHERE user_id='u'")
                receipts = uow.query_one("SELECT COUNT(*) AS count FROM dufang_player_operation_receipts")
            self.assertEqual((stats["count"], stats["total_cost"], receipts["count"]), (1, 20, 1))

            with DatabaseUnitOfWork(game) as uow:
                uow.execute("DROP TRIGGER fail_outbox_ack")
            retry = projector.reconcile(limit=5)
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                stats = uow.query_one("SELECT count,total_cost FROM unseal_data WHERE user_id='u'")
            self.assertEqual(retry.applied, 1)
            self.assertEqual((stats["count"], stats["total_cost"]), (1, 20))

    def test_payout_replay_and_pending_guard(self):
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
                uow.execute("INSERT INTO unseal_data(user_id,count,total_cost,profit,loss,last_update) VALUES('u',1,20,0,0,'')")
            DufangBetSqlRepository(game).place("bet", "u", 20, "now", WIN_PLAN)
            repo = DufangPayoutSqlRepository(game)
            first = repo.settle("p1", "bet", "u", "end")
            duplicate = repo.settle("p1", "bet", "u", "end")
            stale = repo.settle("p2", "bet", "u", "later")
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
            self.assertEqual((first.wallet_stone, first.gain, first.loss), (130, 50, 0))

    def test_missing_schema_fails_without_creating_player_database_or_tables(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "missing-player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("CREATE TABLE dufang_bets(bet_id TEXT PRIMARY KEY,user_id TEXT,status TEXT,settled_at TEXT)")
            repo = DufangPayoutSqlRepository(game)
            self.assertEqual(repo.settle("p1", "bet", "u", "now").status, "schema_missing")
            self.assertEqual(repo.get_result("p1").status, "schema_missing")
            self.assertFalse(player.exists())
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                names = {str(row["name"]) for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("dufang_payout_operations", names)

    def test_missing_player_statistics_are_repaired_from_game_outbox(self):
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
            DufangBetSqlRepository(game).place("bet", "u", 20, "now", WIN_PLAN)
            payout = DufangPayoutSqlRepository(game).settle("p1", "bet", "u", "end")
            self.assertEqual(payout.status, "applied")
            projected = DufangPlayerStatsSqlRepository(game, player).reconcile(limit=5)
            self.assertEqual(projected.applied, 2)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"]
                status = uow.query_one("SELECT status FROM dufang_bets WHERE bet_id='bet'")["status"]
                payout_count = uow.query_one("SELECT COUNT(*) AS count FROM dufang_payout_operations")["count"]
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                stats = uow.query_one("SELECT count,total_cost,profit,loss FROM unseal_data WHERE user_id='u'")
            self.assertEqual((wallet, status, payout_count), (130, "win", 1))
            self.assertEqual(
                (stats["count"], stats["total_cost"], stats["profit"], stats["loss"]),
                (1, 20, 50, 0),
            )

    def test_competing_payout_operations_settle_pending_bet_once(self):
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
                uow.execute("INSERT INTO unseal_data(user_id,count,total_cost,profit,loss,last_update) VALUES('u',1,20,0,0,'')")
            DufangBetSqlRepository(game).place("bet", "u", 20, "now", WIN_PLAN)
            repo = DufangPayoutSqlRepository(game)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(
                    lambda operation: repo.settle(operation, "bet", "u", "end"),
                    ("p1", "p2"),
                ))
            self.assertEqual(sorted(result.status for result in results), ["applied", "state_changed"])
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"]
                count = uow.query_one("SELECT COUNT(*) AS count FROM dufang_payout_operations")["count"]
            self.assertEqual((wallet, count), (130, 1))
