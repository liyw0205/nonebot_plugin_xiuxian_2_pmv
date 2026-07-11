from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.auction_queue_service import (
    AuctionQueueService,
)
from tests.test_db_backend import db_backend


class AuctionQueueServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.trade_database = root / "trade.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT, goods_id INTEGER, goods_name TEXT,
                    goods_type TEXT, goods_num INTEGER, create_time TEXT,
                    update_time TEXT, bind_num INTEGER DEFAULT 0,
                    state INTEGER DEFAULT 0, UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, NULL, NULL, %s, %s)",
                ("user", 1001, "测试法器", "装备", 3, 1, 0),
            )
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                """
                CREATE TABLE auction_player_upload (
                    user_id TEXT NOT NULL, item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL, start_price INTEGER NOT NULL,
                    user_name TEXT NOT NULL, PRIMARY KEY (user_id, item_id)
                )
                """
            )
        self.service = AuctionQueueService(
            self.game_database, self.trade_database, max_goods_num=99
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def inventory(self) -> tuple[int, int]:
        with db_backend.connection(self.game_database) as conn:
            row = conn.execute(
                "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 1001),
            ).fetchone()
        return int(row[0]), int(row[1])

    def queue_count(self) -> int:
        with db_backend.connection(self.trade_database) as conn:
            return int(
                conn.execute("SELECT COUNT(*) FROM auction_player_upload").fetchone()[0]
            )

    def operation_count(self) -> int:
        with db_backend.connection(self.game_database) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("auction_queue_operations",),
            ).fetchone()
            if exists is None:
                return 0
            return int(
                conn.execute("SELECT COUNT(*) FROM auction_queue_operations").fetchone()[0]
            )

    def enqueue(self, operation_id="enqueue-1"):
        return self.service.enqueue(
            operation_id,
            "user",
            1001,
            "测试法器",
            600000,
            "测试道友",
            max_user_items=3,
        )

    def test_enqueue_consumes_inventory_and_adds_queue_row_atomically(self) -> None:
        result = self.enqueue()

        self.assertEqual(result.status, "completed")
        self.assertEqual(self.inventory(), (2, 1))
        self.assertEqual(self.queue_count(), 1)
        self.assertEqual(self.operation_count(), 1)

    def test_dequeue_removes_queue_row_and_returns_bound_item(self) -> None:
        self.enqueue()
        result = self.service.dequeue("dequeue-1", "user", 1001, "装备")

        self.assertEqual(result.status, "completed")
        self.assertEqual(self.inventory(), (3, 2))
        self.assertEqual(self.queue_count(), 0)
        self.assertEqual(self.operation_count(), 2)

    def test_duplicate_does_not_consume_or_return_item_twice(self) -> None:
        first = self.enqueue("enqueue-repeat")
        second = self.enqueue("enqueue-repeat")

        self.assertEqual((first.status, second.status), ("completed", "duplicate"))
        self.assertEqual(self.inventory(), (2, 1))
        self.assertEqual(self.queue_count(), 1)

        first = self.service.dequeue("dequeue-repeat", "user", 1001, "装备")
        second = self.service.dequeue("dequeue-repeat", "user", 1001, "装备")
        self.assertEqual((first.status, second.status), ("completed", "duplicate"))
        self.assertEqual(self.inventory(), (3, 2))
        self.assertEqual(self.queue_count(), 0)

    def test_bound_or_equipped_stock_cannot_be_queued(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "UPDATE back SET goods_num=%s, bind_num=%s, state=%s "
                "WHERE user_id=%s AND goods_id=%s",
                (2, 1, 1, "user", 1001),
            )
        result = self.enqueue()

        self.assertEqual(result.status, "stock_insufficient")
        self.assertEqual(self.inventory(), (2, 1))
        self.assertEqual(self.queue_count(), 0)

    def test_queue_insert_failure_rolls_back_inventory(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_queue_insert BEFORE INSERT "
                "ON auction_player_upload "
                "BEGIN SELECT RAISE(ABORT, 'queue failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.enqueue()

        self.assertEqual(self.inventory(), (3, 1))
        self.assertEqual(self.queue_count(), 0)
        self.assertEqual(self.operation_count(), 0)

    def test_operation_failure_rolls_back_dequeue_and_return(self) -> None:
        self.enqueue()
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_queue_operation BEFORE INSERT "
                "ON auction_queue_operations "
                "WHEN NEW.action='dequeue' "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.dequeue("dequeue-fail", "user", 1001, "装备")

        self.assertEqual(self.inventory(), (2, 1))
        self.assertEqual(self.queue_count(), 1)
        self.assertEqual(self.operation_count(), 1)

    def test_inventory_limit_blocks_dequeue_without_removing_queue_row(self) -> None:
        self.enqueue()
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "UPDATE back SET goods_num=%s WHERE user_id=%s AND goods_id=%s",
                (99, "user", 1001),
            )
        result = self.service.dequeue("dequeue-full", "user", 1001, "装备")

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.queue_count(), 1)
        self.assertEqual(self.operation_count(), 1)


if __name__ == "__main__":
    unittest.main()
