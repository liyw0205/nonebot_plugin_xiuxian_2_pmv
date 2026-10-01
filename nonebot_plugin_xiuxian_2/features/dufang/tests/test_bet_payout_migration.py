from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, MigrationRunner
from ....plugin import build_migrations, migrations_for_database


class DufangBetPayoutMigrationTests(unittest.TestCase):
    def test_bet_payout_migration_is_game_only(self):
        migrations = build_migrations()
        game_versions = {item.version for item in migrations_for_database(migrations, "game_db")}
        player_versions = {item.version for item in migrations_for_database(migrations, "player_db")}
        self.assertIn("legacy.dufang.004", game_versions)
        self.assertNotIn("legacy.dufang.004", player_versions)

    def test_migration_is_idempotent_and_preserves_legacy_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute(
                    "CREATE TABLE dufang_bets(bet_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,cost INTEGER NOT NULL,"
                    "status TEXT NOT NULL,placed_at TEXT NOT NULL,settled_at TEXT)"
                )
                uow.execute(
                    "CREATE TABLE dufang_bet_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "cost INTEGER NOT NULL,wallet_stone INTEGER NOT NULL,bet_id TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                uow.execute(
                    "CREATE TABLE dufang_payout_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "wallet_stone INTEGER NOT NULL,gain INTEGER NOT NULL,loss INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                uow.execute("INSERT INTO dufang_bets VALUES('bet','u',20,'win','then','later')")
                uow.execute(
                    "INSERT INTO dufang_bet_operations VALUES(?,?,?,?,?,?)",
                    ("bet", '["u",20]', 20, 80, "bet", "then"),
                )
                uow.execute(
                    "INSERT INTO dufang_payout_operations VALUES(?,?,?,?,?,?)",
                    ("pay", '["bet","u"]', 130, 50, 0, "later"),
                )

            migration = next(
                item for item in migrations_for_database(build_migrations(), "game_db")
                if item.version == "legacy.dufang.004"
            )
            runner = MigrationRunner((migration,))
            with DatabaseUnitOfWork(game) as uow:
                self.assertEqual(runner.apply(uow), ["legacy.dufang.004"])
                self.assertEqual(runner.apply(uow), [])
                bet = uow.query_one("SELECT bet_id,user_id,status FROM dufang_bets WHERE bet_id='bet'")
                bet_operation = uow.query_one("SELECT operation_id,cost,wallet_stone FROM dufang_bet_operations WHERE operation_id='bet'")
                payout = uow.query_one("SELECT operation_id,wallet_stone,gain,loss FROM dufang_payout_operations WHERE operation_id='pay'")
            self.assertEqual(tuple(bet.values()), ("bet", "u", "win"))
            self.assertEqual(tuple(bet_operation.values()), ("bet", 20, 80))
            self.assertEqual(tuple(payout.values()), ("pay", 130, 50, 0))


if __name__ == "__main__":
    unittest.main()
