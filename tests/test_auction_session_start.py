from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.auction_session_service import (
    AuctionSessionService,
)
from tests.test_db_backend import db_backend


class AuctionSessionStartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.trade = root / "trade.sqlite3"
        with db_backend.transaction(self.game):
            pass
        with db_backend.transaction(self.trade) as conn:
            conn.execute(
                "CREATE TABLE auction_player_upload (user_id TEXT,item_id INTEGER,"
                "item_name TEXT,start_price INTEGER,user_name TEXT,"
                "PRIMARY KEY(user_id,item_id))"
            )
            conn.execute(
                "INSERT INTO auction_player_upload VALUES (%s,%s,%s,%s,%s)",
                ("seller", 1001, "玩家法器", 600000, "卖家"),
            )
        self.service = AuctionSessionService(self.game, self.trade, 99)
        self.system = [{"item_id": 2001, "name": "系统丹药", "start_price": 800000}]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_start_moves_queue_and_records_session_atomically(self) -> None:
        result = self.service.start(
            "start-1", "session-1", start_time=100.0, end_time=200.0,
            system_items=self.system,
        )
        self.assertEqual(result.status, "started")
        self.assertEqual(result.items_count, 2)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_current").fetchone()[0], 2)
            row = conn.execute("SELECT status,items_count FROM auction_sessions").fetchone()
            self.assertEqual(tuple(row), ("active", 2))
        with db_backend.connection(self.trade) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_player_upload").fetchone()[0], 0)

    def test_start_is_idempotent_and_rejects_conflicting_payload(self) -> None:
        first = self.service.start("start-1", "session-1", start_time=100, end_time=200, system_items=self.system)
        replay = self.service.start("start-1", "session-1", start_time=100, end_time=200, system_items=self.system)
        conflict = self.service.start("start-1", "session-2", start_time=100, end_time=200, system_items=self.system)
        self.assertEqual(first.status, "started")
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(conflict.status, "state_changed")

    def test_parallel_session_is_rejected(self) -> None:
        self.service.start("start-1", "session-1", start_time=100, end_time=200, system_items=self.system)
        result = self.service.start("start-2", "session-2", start_time=100, end_time=200, system_items=self.system)
        self.assertEqual(result.status, "already_active")

    def test_operation_failure_rolls_back_items_queue_and_session(self) -> None:
        self.service.get_active_session()
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER reject_start_operation BEFORE INSERT ON auction_session_operations "
                "BEGIN SELECT RAISE(ABORT, 'reject'); END"
            )
        with self.assertRaises(Exception):
            self.service.start("start-1", "session-1", start_time=100, end_time=200, system_items=self.system)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_current").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_sessions").fetchone()[0], 0)
        with db_backend.connection(self.trade) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_player_upload").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
