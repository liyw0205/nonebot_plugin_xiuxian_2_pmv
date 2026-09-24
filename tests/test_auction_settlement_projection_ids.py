from __future__ import annotations

import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.auction.migrations import apply_auction_settlement_game_effects
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend, economy_log, season_rank_service


class _SqlMessage:
    def __init__(self, database: Path):
        self.conn = db_backend.connect(database, check_same_thread=False)
        self.lock = threading.RLock()

    def _commit_write(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


class AuctionSettlementProjectionIdTests(unittest.TestCase):
    def test_settlement_projection_migrations_route_to_their_owner_databases(self):
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        player = {item.version for item in migrations_for_database(migrations, "player_db")}
        self.assertIn("auction.007", game)
        self.assertNotIn("auction.007", player)
        self.assertNotIn("auction.008", game)
        self.assertIn("auction.008", player)

    def test_season_rank_receipt_prevents_duplicate_score_and_rejects_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            game_db = Path(directory) / "game.db"
            with DatabaseUnitOfWork(game_db) as uow:
                apply_auction_settlement_game_effects(uow)
            manager = _SqlMessage(game_db)
            now = datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc)
            paths = SimpleNamespace(game_db=game_db)
            try:
                with (
                    patch.object(season_rank_service, "_sql_message", return_value=manager),
                    patch("nonebot_plugin_xiuxian_2.paths.get_paths", return_value=paths),
                ):
                    first = season_rank_service.add_season_rank_score(
                        rank_type="交易活跃", score=300, mode="monthly", user_id="u1",
                        event_id="auction-event:monthly", now=now,
                    )
                    replay = season_rank_service.add_season_rank_score(
                        rank_type="交易活跃", score=300, mode="monthly", user_id="u1",
                        event_id="auction-event:monthly", now=now,
                    )
                    with self.assertRaises(ValueError):
                        season_rank_service.add_season_rank_score(
                            rank_type="交易活跃", score=301, mode="monthly", user_id="u1",
                            event_id="auction-event:monthly", now=now,
                        )
                with DatabaseUnitOfWork(game_db) as uow:
                    row = uow.query_one(
                        "SELECT score FROM season_rank WHERE user_id='u1'"
                    )
                self.assertEqual(first["score"], 300)
                self.assertTrue(replay["replayed"])
                self.assertEqual(row["score"], 300)
            finally:
                manager.close()

    def test_economy_log_event_id_is_idempotent_and_payload_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            manager = _SqlMessage(database)
            payload = {
                "user_id": "u1",
                "source": "auction",
                "action": "auction_buy",
                "stone_delta": -300,
                "item_delta": [{"id": 10, "name": "灵剑", "amount": 1}],
                "detail": {"auction_id": "a-1"},
                "trace_id": "trade:auction:a-1",
                "event_id": "auction-event:buyer",
                "created_at": "2026-09-24T10:00:00+00:00",
            }
            try:
                with patch.object(economy_log, "XiuxianDateManage", return_value=manager):
                    first = economy_log.log_economy_change(**payload)
                    replay = economy_log.log_economy_change(**payload)
                    with self.assertRaises(ValueError):
                        economy_log.log_economy_change(**{**payload, "stone_delta": -301})
                with manager.lock:
                    row = manager.conn.cursor().execute(
                        "SELECT COUNT(*) FROM economy_log WHERE event_id=?",
                        (payload["event_id"],),
                    ).fetchone()
                self.assertEqual(first, replay)
                self.assertEqual(int(row[0]), 1)
            finally:
                manager.close()


if __name__ == "__main__":
    unittest.main()
