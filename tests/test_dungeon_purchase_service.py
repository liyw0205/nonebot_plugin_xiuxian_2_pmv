from __future__ import annotations

import asyncio
import importlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import nonebot
from nonebot.exception import FinishedException

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.transaction_service import (
    DungeonPurchaseService,
)
from tests.test_db_backend import db_backend

dungeon_plugin = importlib.import_module(
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon"
)


class DungeonPurchaseServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("u", 100))
            conn.execute(
                "CREATE TABLE back("
                "user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
        self.service = DungeonPurchaseService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def purchase(self, operation="buy", **overrides):
        values = {
            "user_id": "u",
            "item_id": 1,
            "item_name": "item",
            "item_type": "type",
            "quantity": 2,
            "unit_cost": 10,
            "expected_stone": 100,
            "max_goods": 99,
            "bind_flag": 1,
        }
        values.update(overrides)
        return self.service.purchase(
            operation,
            values["user_id"],
            values["item_id"],
            values["item_name"],
            values["item_type"],
            values["quantity"],
            values["unit_cost"],
            values["expected_stone"],
            values["max_goods"],
            values["bind_flag"],
        )

    def state(self, user_id="u", item_id=1):
        with db_backend.connection(self.database) as conn:
            user = conn.execute(
                "SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                (user_id, item_id),
            ).fetchone()
        return (
            int(user[0]) if user else None,
            tuple(map(int, item)) if item else None,
        )

    def operation(self, operation_id):
        with db_backend.connection(self.database) as conn:
            if not conn.table_exists("dungeon_purchase_operations"):
                return None
            if not conn.column_exists(
                "dungeon_purchase_operations", "result_status"
            ):
                return conn.execute(
                    "SELECT payload,'applied',quantity,cost,stone,inventory,'' "
                    "FROM dungeon_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
            return conn.execute(
                "SELECT payload,result_status,quantity,cost,stone,inventory,response "
                "FROM dungeon_purchase_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()

    def test_purchase_deducts_stone_and_preserves_binding_invariant(self) -> None:
        result = self.purchase()
        self.assertEqual(
            (
                result.status,
                result.cost,
                result.stone,
                result.inventory,
                result.response,
            ),
            ("applied", 20, 80, 2, "成功兑换item×2，消耗20灵石。"),
        )
        self.assertEqual(self.state(), (80, (2, 2)))

        unbound = self.purchase(
            "unbound",
            quantity=1,
            expected_stone=80,
            bind_flag=0,
        )
        self.assertEqual((unbound.status, self.state()), ("applied", (70, (3, 2))))

    def test_success_replay_ignores_mutable_snapshot_and_metadata(self) -> None:
        first = self.purchase("repeat")
        replay = self.purchase(
            "repeat",
            item_name="renamed",
            item_type="changed",
            unit_cost=999,
            expected_stone=80,
            max_goods=0,
        )
        conflict = self.purchase("repeat", quantity=1, expected_stone=80)

        self.assertEqual((first.status, replay.status, conflict.status), (
            "applied",
            "duplicate",
            "state_changed",
        ))
        self.assertEqual(replay.response, first.response)
        self.assertEqual(replay.cost, first.cost)
        self.assertEqual((replay.stone, replay.inventory), (80, 2))
        self.assertEqual(self.state(), (80, (2, 2)))
        row = self.operation("repeat")
        self.assertEqual(json.loads(str(row[0])), ["u", 1, 2, 1])

    def test_handler_replays_success_after_item_is_removed_from_shop(self) -> None:
        operation_id = "dungeon-purchase:same-event:u"
        first = self.purchase(operation_id)
        before_state = self.state()
        before_operation = tuple(self.operation(operation_id))
        bot = SimpleNamespace(self_id="bot")
        event = SimpleNamespace(message_id="same-event")
        args = SimpleNamespace(extract_plain_text=lambda: "1 2")
        service = Mock(wraps=self.service)
        sent = AsyncMock()

        with (
            patch.object(
                dungeon_plugin,
                "assign_bot",
                AsyncMock(return_value=(bot, None)),
            ),
            patch.object(
                dungeon_plugin,
                "check_user",
                Mock(return_value=(True, {"user_id": "u", "stone": 80}, "")),
            ),
            patch.object(dungeon_plugin, "dungeon_purchase_service", service),
            patch.object(dungeon_plugin, "handle_send", sent),
            patch.dict(dungeon_plugin.DUNGEON_SHOP, {}, clear=True),
            patch.object(
                dungeon_plugin.items,
                "get_data_by_item_id",
                wraps=dungeon_plugin.items.get_data_by_item_id,
            ) as item_lookup,
        ):
            with self.assertRaises(FinishedException):
                asyncio.run(
                    dungeon_plugin.handle_dungeon_purchase(bot, event, args)
                )

        sent.assert_awaited_once_with(bot, event, first.response)
        service.operation_result.assert_called_once_with(operation_id, "u", 1, 2, 1)
        service.purchase.assert_not_called()
        item_lookup.assert_not_called()
        self.assertEqual(self.state(), before_state)
        self.assertEqual(tuple(self.operation(operation_id)), before_operation)

    def test_rejections_are_recorded_and_replay_the_first_result(self) -> None:
        poor = self.purchase("poor", unit_cost=60)
        self.assertEqual((poor.status, poor.stone), ("stone_insufficient", 100))
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=1000 WHERE user_id=%s", ("u",))
        poor_replay = self.purchase("poor", unit_cost=60, expected_stone=1000)
        self.assertEqual((poor_replay.status, poor_replay.stone), ("stone_insufficient", 100))
        self.assertEqual(poor_replay.response, poor.response)

        stale = self.purchase("stale", expected_stone=999)
        self.assertEqual(stale.status, "state_changed")
        self.assertEqual(self.purchase("stale", expected_stone=1000).status, "state_changed")

        missing = self.purchase("missing", user_id="missing", expected_stone=0)
        self.assertEqual(missing.status, "user_missing")
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("missing", 100))
        self.assertEqual(
            self.purchase("missing", user_id="missing").status, "user_missing"
        )

        self.assertEqual(self.state(), (1000, None))
        self.assertEqual(
            [self.operation(operation)[1] for operation in ("poor", "stale", "missing")],
            ["stone_insufficient", "state_changed", "user_missing"],
        )

    def test_inventory_full_and_corrupt_binding_are_fixed_rejections(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", 1, "item", "type", 99, "", "", 99),
            )
        full = self.purchase("full")
        self.assertEqual((full.status, full.inventory), ("inventory_full", 99))
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE back SET goods_num=0,bind_num=0 WHERE user_id=%s AND goods_id=%s",
                ("u", 1),
            )
        self.assertEqual(self.purchase("full").status, "inventory_full")
        self.assertEqual(self.state(), (100, (0, 0)))

        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE back SET goods_num=1,bind_num=2 WHERE user_id=%s AND goods_id=%s",
                ("u", 1),
            )
        corrupt = self.purchase("corrupt")
        self.assertEqual(corrupt.status, "state_changed")
        self.assertEqual(self.state(), (100, (1, 2)))

    def test_legacy_success_schema_and_payload_are_migrated(self) -> None:
        legacy_payload = json.dumps(
            ["u", 1, "old item", "old type", 2, 10, 100, 1],
            ensure_ascii=True,
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=80 WHERE user_id=%s", ("u",))
            conn.execute(
                "INSERT INTO back VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", 1, "old item", "old type", 2, "", "", 2),
            )
            conn.execute(
                "CREATE TABLE dungeon_purchase_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,"
                "cost INTEGER NOT NULL,stone INTEGER NOT NULL,inventory INTEGER NOT NULL,"
                "created_at TIMESTAMP)"
            )
            conn.execute(
                "INSERT INTO dungeon_purchase_operations VALUES(%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                ("legacy", legacy_payload, 2, 20, 80, 2),
            )

        replay = self.purchase(
            "legacy",
            item_name="new item",
            item_type="new type",
            expected_stone=80,
            max_goods=0,
        )
        self.assertEqual(
            (replay.status, replay.response),
            ("duplicate", "成功兑换old item×2，消耗20灵石。"),
        )
        row = self.operation("legacy")
        self.assertEqual(json.loads(str(row[0])), ["u", 1, 2, 1])
        self.assertEqual(row[1], "applied")
        with db_backend.connection(self.database) as conn:
            self.assertTrue(conn.column_exists("dungeon_purchase_operations", "result_status"))
            self.assertTrue(conn.column_exists("dungeon_purchase_operations", "response"))
        self.assertEqual(self.state(), (80, (2, 2)))

    def test_partially_migrated_legacy_payload_is_normalized(self) -> None:
        legacy_payload = json.dumps(
            ["u", 1, "old item", "old type", 2, 10, 100, 1],
            ensure_ascii=True,
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE dungeon_purchase_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "result_status TEXT NOT NULL DEFAULT 'applied',"
                "quantity INTEGER NOT NULL,cost INTEGER NOT NULL,stone INTEGER NOT NULL,"
                "inventory INTEGER NOT NULL,response TEXT NOT NULL DEFAULT '',"
                "created_at TIMESTAMP)"
            )
            conn.execute(
                "INSERT INTO dungeon_purchase_operations("
                "operation_id,payload,quantity,cost,stone,inventory,created_at) "
                "VALUES(%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                ("partial", legacy_payload, 2, 20, 80, 2),
            )

        replay = self.purchase("partial", expected_stone=80, max_goods=0)

        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(replay.response, "成功兑换old item×2，消耗20灵石。")
        self.assertEqual(json.loads(str(self.operation("partial")[0])), ["u", 1, 2, 1])

    def test_operation_insert_failure_rolls_back_wallet_and_inventory(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE dungeon_purchase_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,quantity INTEGER,cost INTEGER,"
                "stone INTEGER,inventory INTEGER,created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_purchase_operation BEFORE INSERT "
                "ON dungeon_purchase_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("operation-failure")
        self.assertEqual(self.state(), (100, None))
        self.assertIsNone(self.operation("operation-failure"))

    def test_wallet_failure_rolls_back_operation_and_inventory(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_purchase_wallet BEFORE UPDATE OF stone "
                "ON user_xiuxian BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("wallet-failure")
        self.assertEqual(self.state(), (100, None))
        self.assertIsNone(self.operation("wallet-failure"))

    def test_inventory_failure_rolls_back_wallet_and_operation(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_purchase_inventory BEFORE INSERT ON back "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("inventory-failure")
        self.assertEqual(self.state(), (100, None))
        self.assertIsNone(self.operation("inventory-failure"))


if __name__ == "__main__":
    unittest.main()
