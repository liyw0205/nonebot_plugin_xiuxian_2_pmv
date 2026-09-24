from __future__ import annotations

import tempfile
import unittest
import importlib
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.transaction_service import (
    AuctionQueueService,
)
from nonebot_plugin_xiuxian_2.features.auction.queue_application import AuctionQueueApplication
from nonebot_plugin_xiuxian_2.features.auction.migrations import (
    apply_auction_player_queue,
    apply_auction_queue_operations,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


def test_trade_facade_defers_auction_queue_service_construction():
    trade = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade"
    )
    assert trade._auction_queue_application_instance is None


def test_auction_queue_handlers_use_lazy_game_trade_service():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_trade/__init__.py"
    ).read_text(encoding="utf-8")
    assert "_auction_queue_application_instance = None" in source
    assert "def _auction_queue_application(" in source
    assert "get_paths().game_db" in source
    assert "get_paths().trade_db" in source
    assert "_auction_queue_application().enqueue(" in source
    assert "_auction_queue_application().dequeue(" in source
    assert "_auction_queue_application().get_operation(" in source
    assert "_auction_queue_application().get_player_items(" in source
    assert "_auction_queue_application().count_player_items(" in source
    assert "_trade_manager().get_player_auction_items(" not in source
    assert "auction_queue_service.enqueue(" not in source
    assert "auction_queue_service.dequeue(" not in source


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
        with DatabaseUnitOfWork(self.game_database) as uow:
            apply_auction_queue_operations(uow)
        with DatabaseUnitOfWork(self.trade_database) as uow:
            apply_auction_player_queue(uow)
        self.application = AuctionQueueApplication(
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
        return self.application.enqueue(
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

    def test_player_queue_query_preserves_legacy_shape_and_counts_without_loading(self) -> None:
        self.enqueue()
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "INSERT INTO auction_player_upload VALUES(?,?,?,?,?)",
                ("another-user", 2002, "另一件法器", 700000, "其他道友"),
            )

        own_items = self.application.get_player_items("user")
        all_items = self.application.get_player_items()

        self.assertEqual(
            own_items,
            [{
                "user_id": "user",
                "item_id": 1001,
                "item_name": "测试法器",
                "start_price": 600000,
                "user_name": "测试道友",
            }],
        )
        self.assertEqual(len(all_items), 2)
        self.assertEqual(self.application.count_player_items(), 2)
        self.assertEqual(self.application.count_player_items("user"), 1)

    def test_player_queue_query_handles_missing_database_and_table_without_ddl(self) -> None:
        missing_database = Path(self.temp_dir.name) / "missing-trade.sqlite3"
        missing_table = Path(self.temp_dir.name) / "empty-trade.sqlite3"
        with DatabaseUnitOfWork(missing_table):
            pass

        missing_app = AuctionQueueApplication(
            self.game_database, missing_database, max_goods_num=99
        )
        empty_app = AuctionQueueApplication(
            self.game_database, missing_table, max_goods_num=99
        )

        self.assertEqual(missing_app.get_player_items(), [])
        self.assertEqual(missing_app.count_player_items(), 0)
        self.assertFalse(missing_database.exists())
        self.assertEqual(empty_app.get_player_items(), [])
        self.assertEqual(empty_app.count_player_items(), 0)
        with DatabaseUnitOfWork(missing_table, read_only=True) as uow:
            tables = uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        self.assertEqual(tables, [])

    def test_dequeue_removes_queue_row_and_returns_bound_item(self) -> None:
        self.enqueue()
        result = self.application.dequeue("dequeue-1", "user", 1001, "装备")

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

        first = self.application.dequeue("dequeue-repeat", "user", 1001, "装备")
        second = self.application.dequeue("dequeue-repeat", "user", 1001, "装备")
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
            self.application.dequeue("dequeue-fail", "user", 1001, "装备")

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
        result = self.application.dequeue("dequeue-full", "user", 1001, "装备")

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.queue_count(), 1)
        self.assertEqual(self.operation_count(), 1)

    def test_legacy_service_delegates_to_feature_repository(self) -> None:
        result = AuctionQueueService(
            self.game_database, self.trade_database, max_goods_num=99
        ).enqueue(
            "legacy-enqueue", "user", 1001, "测试法器", 600000, "测试道友",
            max_user_items=3,
        )
        self.assertEqual(result.status, "completed")
        self.assertEqual(self.inventory(), (2, 1))

    def test_queue_schema_migrations_follow_their_database_owners(self) -> None:
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        trade = {item.version for item in migrations_for_database(migrations, "trade_db")}
        self.assertIn("auction.003", game)
        self.assertNotIn("auction.003", trade)
        self.assertIn("auction.004", trade)
        self.assertNotIn("auction.004", game)


if __name__ == "__main__":
    unittest.main()
