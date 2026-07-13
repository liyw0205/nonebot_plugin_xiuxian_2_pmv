from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.auction_session_service import (
    AuctionSessionService,
)
from tests.test_db_backend import db_backend


class AuctionSessionFinishTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.trade = root / "trade.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,user_name TEXT,stone INTEGER)")
            conn.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER DEFAULT 0,"
                "UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("seller", "卖家", 0))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("winner", "买家", 1000))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("loser", "落败者", 500))
        with db_backend.transaction(self.trade) as conn:
            conn.execute(
                "CREATE TABLE auction_player_upload (user_id TEXT,item_id INTEGER,item_name TEXT,"
                "start_price INTEGER,user_name TEXT,PRIMARY KEY(user_id,item_id))"
            )
            conn.execute("INSERT INTO auction_player_upload VALUES (%s,%s,%s,%s,%s)", ("seller", 1001, "玩家法器", 600, "卖家"))
        self.service = AuctionSessionService(self.game, self.trade, 99)
        self.service.start("start", "session", start_time=100, end_time=200, system_items=[])
        with db_backend.transaction(self.game) as conn:
            row = conn.execute("SELECT id FROM auction_current").fetchone()
            self.auction_id = str(row[0])
            conn.execute(
                "UPDATE auction_current SET current_price=%s,bids=%s WHERE id=%s",
                (900, json.dumps({"winner": 900, "loser": 700}), self.auction_id),
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def finish(self, operation_id="finish"):
        return self.service.finish(
            operation_id, "session", end_time=300, fee_rate=0.1,
            item_types={1001: "装备"},
        )

    def test_finish_settles_entire_session_in_one_transaction(self) -> None:
        result = self.finish()
        self.assertEqual(result.status, "settled")
        self.assertEqual(result.results[0]["status"], "成交")
        with db_backend.connection(self.game) as conn:
            users = dict(conn.execute("SELECT user_id,stone FROM user_xiuxian"))
            self.assertEqual(users, {"seller": 810, "winner": 1000, "loser": 1200})
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id=%s", ("winner",)).fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_current").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT status FROM auction_sessions").fetchone()[0], "settled")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_history").fetchone()[0], 1)

    def test_finish_replay_is_idempotent(self) -> None:
        first = self.finish()
        replay = self.finish()
        self.assertEqual(first.status, "settled")
        self.assertEqual(replay.status, "duplicate")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='seller'").fetchone()[0], 810)
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='winner'").fetchone()[0], 1)

    def test_operation_failure_rolls_back_whole_session(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER reject_finish_operation BEFORE INSERT ON auction_session_operations "
                "WHEN NEW.action='finish' BEGIN SELECT RAISE(ABORT, 'reject'); END"
            )
        with self.assertRaises(Exception):
            self.finish()
        with db_backend.connection(self.game) as conn:
            users = dict(conn.execute("SELECT user_id,stone FROM user_xiuxian"))
            self.assertEqual(users, {"seller": 0, "winner": 1000, "loser": 500})
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM back").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_current").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT status FROM auction_sessions").fetchone()[0], "active")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_history").fetchone()[0], 0)

    def test_inventory_full_blocks_without_partial_settlement(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s,%s,%s,%s,%s,NULL,NULL,%s)",
                ("winner", 1001, "玩家法器", "装备", 99, 99),
            )
        result = self.finish()
        self.assertEqual(result.status, "inventory_full")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_current").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT status FROM auction_sessions").fetchone()[0], "active")


if __name__ == "__main__":
    unittest.main()
