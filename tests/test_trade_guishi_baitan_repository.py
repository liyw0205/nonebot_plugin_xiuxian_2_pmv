from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_baitan_repository import (
    GuishiBaitanSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_guishi_immediate_match, apply_trade_guishi_qiugou,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedOrderIds:
    def __init__(self, *values: str) -> None:
        self.values = list(values)

    def new_id(self) -> str:
        return self.values.pop(0)


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 24, 12, 34, 56, tzinfo=timezone.utc)


class GuishiBaitanRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.db"
        self.trade_database = root / "trade.db"
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TABLE back("
                "user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER DEFAULT 0,"
                "state INTEGER DEFAULT 0,update_time TEXT,UNIQUE(user_id,goods_id))"
            )
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_num,bind_num,state) VALUES(?,?,?,?,?)",
                ("seller", 1001, 8, 2, 1),
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TABLE guishi_info(user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
            )
            uow.execute(
                "CREATE TABLE guishi_item("
                "id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,item_name TEXT,item_type TEXT,"
                "price INTEGER,quantity INTEGER,filled_quantity INTEGER DEFAULT 0)"
            )
            apply_trade_guishi_qiugou(uow)
            apply_trade_guishi_immediate_match(uow)
        self.repository = GuishiBaitanSqlRepository(
            self.game_database,
            self.trade_database,
            order_ids=FixedOrderIds("1001", "1002"),
            clock=FixedClock(),
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self) -> tuple[int, int, int]:
        with DatabaseUnitOfWork(self.game_database) as uow:
            inventory = uow.query_one(
                "SELECT goods_num,bind_num FROM back WHERE user_id='seller' AND goods_id=1001"
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            orders = uow.query_one(
                "SELECT COUNT(*) AS count FROM guishi_item WHERE item_type='baitan'"
            )
            operations = uow.query_one(
                "SELECT COUNT(*) AS count FROM guishi_order_create_operations"
            )
        return int(inventory["goods_num"]), int(inventory["bind_num"]), int(orders["count"]) + int(operations["count"])

    def create(
        self,
        operation_id: str = "create",
        quantity: int = 4,
        max_orders: int = 10,
        immediate: bool = False,
    ):
        return self.repository.create(
            operation_id=operation_id,
            user_id="seller",
            item_id=1001,
            item_name="灵草",
            price=20,
            quantity=quantity,
            max_orders=max_orders,
            immediate_match=immediate,
        )

    def test_creation_reserves_tradeable_inventory_and_replays(self) -> None:
        first = self.create("same")
        replay = self.create("same")
        conflict = self.repository.create(
            operation_id="same",
            user_id="seller",
            item_id=1001,
            item_name="灵草",
            price=21,
            quantity=4,
            max_orders=10,
        )

        self.assertEqual((first.status, replay.status, conflict.status), ("created", "duplicate", "operation_conflict"))
        self.assertEqual((first.quantity, replay.order_id), (4, first.order_id))
        self.assertEqual(self.state(), (4, 2, 2))

    def test_limit_and_tradeable_stock_rejections_do_not_mutate(self) -> None:
        first = self.create("first", quantity=4)
        limited = self.create("limit", quantity=1, max_orders=1)
        unavailable = self.create("poor", quantity=4, max_orders=10)

        self.assertEqual(first.status, "created")
        self.assertEqual(limited.status, "limit_reached")
        self.assertEqual(unavailable.status, "stock_insufficient")
        self.assertEqual(self.state(), (4, 2, 2))

    def test_insert_failure_rolls_back_inventory(self) -> None:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_baitan BEFORE INSERT ON guishi_item "
                "WHEN NEW.item_type='baitan' BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.create("rollback")
        self.assertEqual(self.state(), (8, 2, 0))

    def test_item_ignore_does_not_reserve_inventory_without_an_order(self) -> None:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TRIGGER ignore_baitan BEFORE INSERT ON guishi_item "
                "WHEN NEW.item_type='baitan' BEGIN SELECT RAISE(IGNORE); END"
            )
        result = self.create("ignored-item")
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state(), (8, 2, 0))

    def test_receipt_ignore_does_not_reserve_inventory_without_a_receipt(self) -> None:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TRIGGER ignore_baitan_receipt BEFORE INSERT ON guishi_order_create_operations "
                "BEGIN SELECT RAISE(IGNORE); END"
            )
        result = self.create("ignored-receipt")
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state(), (8, 2, 0))

    def test_inventory_cas_rejection_rolls_back_trigger_changes(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TRIGGER race_inventory BEFORE UPDATE ON back "
                "BEGIN UPDATE back SET bind_num=0 WHERE user_id=OLD.user_id AND goods_id=OLD.goods_id; "
                "SELECT RAISE(IGNORE); END"
            )
        self.assertEqual(self.create("cas-rejected").status, "state_changed")
        self.assertEqual(self.state(), (8, 2, 0))
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute("DROP TRIGGER race_inventory")
        result = self.create("cas-rejected")
        replay = self.create("cas-rejected")
        self.assertEqual((result.status, replay.status), ("created", "duplicate"))
        self.assertEqual(self.state(), (4, 2, 2))

    def test_immediate_request_ignore_or_rewrite_rolls_back_and_recovers(self) -> None:
        for trigger_sql, trigger_name in (
            (
                "CREATE TRIGGER ignore_immediate BEFORE INSERT ON guishi_immediate_match_requests "
                "BEGIN SELECT RAISE(IGNORE); END",
                "ignore_immediate",
            ),
            (
                "CREATE TRIGGER rewrite_immediate AFTER INSERT ON guishi_immediate_match_requests "
                "BEGIN UPDATE guishi_immediate_match_requests SET status='complete',order_type='qiugou' "
                "WHERE order_id=NEW.order_id; END",
                "rewrite_immediate",
            ),
        ):
            with self.subTest(trigger=trigger_name):
                if trigger_name != "ignore_immediate":
                    with DatabaseUnitOfWork(self.game_database) as uow:
                        uow.execute("UPDATE back SET goods_num=8,bind_num=2 WHERE user_id='seller' AND goods_id=1001")
                    with DatabaseUnitOfWork(self.trade_database) as uow:
                        uow.execute("DELETE FROM guishi_immediate_match_requests")
                        uow.execute("DELETE FROM guishi_order_create_operations")
                        uow.execute("DELETE FROM guishi_item")
                    self.repository.order_ids.values[:] = ["1001", "1002"]
                with DatabaseUnitOfWork(self.trade_database) as uow:
                    uow.execute(trigger_sql)
                failed = self.create("immediate-" + trigger_name, immediate=True)
                self.assertEqual(
                    (failed.status, failed.reason, failed.subject),
                    ("state_changed", "guishi_immediate_match_write_rejected", "1001"),
                )
                self.assertEqual(self.state(), (8, 2, 0))
                with DatabaseUnitOfWork(self.trade_database) as uow:
                    uow.execute(f"DROP TRIGGER {trigger_name}")
                created = self.create("immediate-" + trigger_name, immediate=True)
                replay = self.create("immediate-" + trigger_name, immediate=True)
                self.assertEqual((created.status, replay.status), ("created", "duplicate"))
                with DatabaseUnitOfWork(self.trade_database) as uow:
                    self.assertEqual(
                        uow.query_all("SELECT order_id,order_type,user_id,status FROM guishi_immediate_match_requests"),
                        [{"order_id": created.order_id, "order_type": "baitan", "user_id": "seller", "status": "pending"}],
                    )

    def test_old_message_receipt_replays_when_closed_and_out_of_inventory(self) -> None:
        first = self.create("guishi-order:baitan:message:seller:1001:20:7", quantity=7)
        replay = self.repository.create(
            operation_id="guishi-order:baitan:message:seller", user_id="seller", item_id=1001,
            item_name="灵草", price=20, quantity=7, max_orders=10, creation_open=False,
            cap_to_stock=True,
        )
        self.assertEqual((replay.status, replay.order_id, replay.quantity), ("duplicate", first.order_id, 7))
        self.assertEqual(self.state(), (1, 1, 2))

    def test_cap_keeps_requested_payload_and_returns_committed_quantity(self) -> None:
        args = dict(operation_id="guishi-order:baitan:message:seller", user_id="seller",
                    item_id=1001, item_name="灵草", price=20, quantity=10, max_orders=1,
                    cap_to_stock=True)
        first = self.repository.create(**args)
        replay = self.repository.create(**args, creation_open=False)
        self.assertEqual((first.status, first.quantity, replay.status, replay.quantity), ("created", 7, "duplicate", 7))
        self.assertEqual(self.state(), (1, 1, 2))
        args["quantity"] = 9
        self.assertEqual(self.repository.create(**args).status, "operation_conflict")

    def test_legacy_capped_receipt_does_not_guess_missing_requested_quantity(self) -> None:
        first = self.create("guishi-order:baitan:message:seller:1001:20:4", quantity=4)
        result = self.repository.create(
            operation_id="guishi-order:baitan:message:seller", user_id="seller", item_id=1001,
            item_name="灵草", price=20, quantity=7, max_orders=10, cap_to_stock=True,
        )
        self.assertEqual((first.status, result.status), ("created", "operation_conflict"))
        self.assertEqual(self.state(), (4, 2, 2))

    def test_order_id_conflict_retries(self) -> None:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "INSERT INTO guishi_item(id,user_id,item_id,item_name,item_type,price,quantity) "
                "VALUES(?,?,?,?,?,?,?)",
                ("1001", "other", 1001, "灵草", "baitan", 20, 1),
            )
        result = self.create("retry", quantity=2)
        self.assertEqual(result.status, "created")
        self.assertEqual(result.order_id, "1002")

    def test_application_default_uses_feature_owned_repository(self) -> None:
        result = TradeApplication(
            self.game_database,
            self.trade_database,
            clock=FixedClock(),
        ).guishi_baitan(
            operation_id="application",
            user_id="seller",
            item_id=1001,
            item_name="灵草",
            price=20,
            quantity=3,
            max_orders=10,
        )
        self.assertEqual(result.status, "created")
        self.assertEqual(self.state(), (5, 2, 2))


if __name__ == "__main__":
    unittest.main()
