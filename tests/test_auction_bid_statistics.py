from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.features.auction.bid_statistics import AuctionBidStatisticsRepository
from nonebot_plugin_xiuxian_2.features.auction.migrations import apply_auction_bid_statistics
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


class AuctionBidStatisticsTests(unittest.TestCase):
    def test_statistics_migration_is_player_database_only(self):
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        player = {item.version for item in migrations_for_database(migrations, "player_db")}
        self.assertNotIn("auction.006", game)
        self.assertIn("auction.006", player)

    def test_operation_id_makes_statistics_projection_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_auction_bid_statistics(uow)
            repository = AuctionBidStatisticsRepository(database)
            args = {
                "operation_id": "bid-1",
                "user_id": "u1",
                "bid_price": 300,
                "occurred_at": "2026-09-24T00:00:00+00:00",
            }

            self.assertTrue(repository.record_bid(**args))
            self.assertFalse(repository.record_bid(**args))

            self.assertEqual(
                repository.values(user_id="u1"),
                {"拍卖出价次数": 1, "拍卖出价灵石": 300},
            )
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS n FROM auction_bid_statistics_events")["n"],
                    2,
                )

    def test_operation_id_payload_conflict_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_auction_bid_statistics(uow)
            repository = AuctionBidStatisticsRepository(database)
            repository.record_bid(
                operation_id="bid-1", user_id="u1", bid_price=300,
                occurred_at="2026-09-24T00:00:00+00:00",
            )
            with self.assertRaises(OperationConflictError):
                repository.record_bid(
                    operation_id="bid-1", user_id="u1", bid_price=301,
                    occurred_at="2026-09-24T00:00:00+00:00",
                )
            self.assertEqual(repository.values(user_id="u1")["拍卖出价灵石"], 300)


if __name__ == "__main__":
    unittest.main()
