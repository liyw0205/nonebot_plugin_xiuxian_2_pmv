from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class XianshiListingStaminaTransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "xianshi-stamina.sqlite3"
        with db_backend.transaction(self.database) as conn:
            TradeRepository.ensure_schema(conn)
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL, "
                "user_stamina INTEGER NOT NULL)"
            )
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT NOT NULL, goods_id INTEGER NOT NULL,
                    goods_name TEXT, goods_type TEXT, goods_num INTEGER DEFAULT 0,
                    bind_num INTEGER DEFAULT 0, state INTEGER DEFAULT 0,
                    update_time TEXT, action_time TEXT,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
        self.repository = TradeRepository(self.database, max_goods_num=99)
        self.repository.initialize()
        self.plan = [
            {
                "goods_id": 1001,
                "name": "Test weapon",
                "goods_type": "equipment",
                "price": 600000,
                "quantity": 2,
            },
            {
                "goods_id": 1002,
                "name": "Test herb",
                "goods_type": "herb",
                "price": 800000,
                "quantity": 1,
            },
        ]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def seed_assets(
        self, *, stone: int = 500000, stamina: int = 100, first: int = 3,
        second: int = 2,
    ) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s)",
                ("seller", stone, stamina),
            )
            conn.executemany(
                "INSERT INTO back "
                "(user_id, goods_id, goods_name, goods_type, goods_num) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    ("seller", 1001, "Test weapon", "equipment", first),
                    ("seller", 1002, "Test herb", "herb", second),
                ),
            )

    def assert_assets(
        self, *, stone: int, stamina: int, first: int, second: int,
        listings: int,
    ) -> None:
        self.assertEqual(
            self.scalar(
                "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)
            ),
            stone,
        )
        self.assertEqual(
            self.scalar(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s",
                ("seller",),
            ),
            stamina,
        )
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE goods_id=%s", (1001,)),
            first,
        )
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE goods_id=%s", (1002,)),
            second,
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), listings)

    def add_fast(self, operation_id: str, *, stamina_cost: int = 10):
        return self.repository.add_xianshi_items(
            operation_id,
            "seller",
            1001,
            "Test weapon",
            "equipment",
            600000,
            2,
            fee_charged=120000,
            consume_assets=True,
            stamina_cost=stamina_cost,
        )

    def add_plan(self, operation_id: str, *, stamina_cost: int = 30):
        return self.repository.add_xianshi_plan_items(
            operation_id,
            "seller",
            self.plan,
            fee_charged=200000,
            consume_assets=True,
            stamina_cost=stamina_cost,
        )

    def test_fast_listing_consumes_stamina_fee_stock_and_creates_rows(self) -> None:
        self.seed_assets()

        result = self.add_fast("fast-success")

        self.assertTrue(result.applied)
        self.assertEqual(result.stamina_charged, 10)
        self.assert_assets(
            stone=380000, stamina=90, first=1, second=2, listings=2
        )

    def test_plan_listing_consumes_stamina_fee_all_stock_and_creates_rows(self) -> None:
        self.seed_assets()

        result = self.add_plan("plan-success")
        duplicate = self.add_plan("plan-success")
        conflict = self.add_plan("plan-success", stamina_cost=31)

        self.assertTrue(result.applied)
        self.assertEqual((duplicate.status, conflict.status), ("duplicate", "state_changed"))
        self.assertEqual(result.stamina_charged, 30)
        self.assertEqual(duplicate.stamina_charged, 30)
        self.assert_assets(
            stone=300000, stamina=70, first=1, second=1, listings=3
        )

    def test_stamina_rejection_does_not_change_other_state(self) -> None:
        self.seed_assets(stamina=9)

        result = self.add_fast("fast-no-stamina")
        plan_result = self.add_plan("plan-no-stamina")

        self.assertEqual(result.status, "stamina_insufficient")
        self.assertEqual(plan_result.status, "stamina_insufficient")
        self.assert_assets(
            stone=500000, stamina=9, first=3, second=2, listings=0
        )

    def test_later_asset_rejections_roll_back_stamina(self) -> None:
        self.seed_assets(stone=100000)
        stone_result = self.add_fast("fast-no-stone")
        self.assertEqual(stone_result.status, "stone_insufficient")
        self.assert_assets(
            stone=100000, stamina=100, first=3, second=2, listings=0
        )

        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=%s", (500000,))
            conn.execute("UPDATE back SET goods_num=0 WHERE goods_id=%s", (1002,))
        stock_result = self.add_plan("plan-no-stock")
        self.assertEqual(stock_result.status, "stock_insufficient")
        self.assert_assets(
            stone=500000, stamina=100, first=3, second=0, listings=0
        )

    def test_duplicate_does_not_charge_again_and_cost_change_conflicts(self) -> None:
        self.seed_assets()
        first = self.add_fast("fast-repeat")
        duplicate = self.add_fast("fast-repeat")
        conflict = self.add_fast("fast-repeat", stamina_cost=11)

        self.assertEqual(
            (first.status, duplicate.status, conflict.status),
            ("listed", "duplicate", "state_changed"),
        )
        self.assertEqual(duplicate.stamina_charged, 10)
        self.assert_assets(
            stone=380000, stamina=90, first=1, second=2, listings=2
        )

    def test_operation_insert_failures_roll_back_every_change(self) -> None:
        self.seed_assets()
        self.add_fast("create-fast-table", stamina_cost=0)
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM xianshi_item")
            conn.execute("DELETE FROM xianshi_listing_operations")
            conn.execute("UPDATE user_xiuxian SET stone=500000, user_stamina=100")
            conn.execute("UPDATE back SET goods_num=3 WHERE goods_id=1001")
            conn.execute(
                "CREATE TRIGGER fail_fast_operation "
                "BEFORE INSERT ON xianshi_listing_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.add_fast("fast-operation-fail")
        self.assert_assets(
            stone=500000, stamina=100, first=3, second=2, listings=0
        )

        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER fail_fast_operation")
        self.add_plan("create-plan-table", stamina_cost=0)
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM xianshi_item")
            conn.execute("DELETE FROM xianshi_plan_listing_operations")
            conn.execute("UPDATE user_xiuxian SET stone=500000, user_stamina=100")
            conn.execute("UPDATE back SET goods_num=3 WHERE goods_id=1001")
            conn.execute("UPDATE back SET goods_num=2 WHERE goods_id=1002")
            conn.execute(
                "CREATE TRIGGER fail_plan_operation "
                "BEFORE INSERT ON xianshi_plan_listing_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.add_plan("plan-operation-fail")
        self.assert_assets(
            stone=500000, stamina=100, first=3, second=2, listings=0
        )

    def test_legacy_operation_tables_gain_stamina_cost_column(self) -> None:
        legacy_database = Path(self.temp_dir.name) / "legacy-operations.sqlite3"
        repository = TradeRepository(legacy_database, max_goods_num=99)
        plan_text = json.dumps(self.plan, ensure_ascii=False, sort_keys=True)
        with db_backend.transaction(legacy_database) as conn:
            TradeRepository.ensure_schema(conn)
            conn.execute(
                "CREATE TABLE xianshi_listing_operations ("
                "operation_id TEXT PRIMARY KEY, seller_id TEXT NOT NULL, "
                "goods_id INTEGER NOT NULL, name TEXT NOT NULL, "
                "goods_type TEXT NOT NULL, price INTEGER NOT NULL, "
                "requested_quantity INTEGER NOT NULL, listed_quantity INTEGER NOT NULL, "
                "fee_charged INTEGER NOT NULL)"
            )
            conn.execute(
                "INSERT INTO xianshi_listing_operations VALUES "
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                ("legacy-fast", "seller", 1001, "Test weapon", "equipment", 600000, 2, 2, 120000),
            )
            conn.execute(
                "CREATE TABLE xianshi_plan_listing_operations ("
                "operation_id TEXT PRIMARY KEY, seller_id TEXT NOT NULL, "
                "listing_plan TEXT NOT NULL, listed_quantity INTEGER NOT NULL, "
                "fee_charged INTEGER NOT NULL)"
            )
            conn.execute(
                "INSERT INTO xianshi_plan_listing_operations VALUES "
                "(%s, %s, %s, %s, %s)",
                ("legacy-plan", "seller", plan_text, 3, 200000),
            )

        fast_result = repository.add_xianshi_items(
            "legacy-fast", "seller", 1001, "Test weapon", "equipment", 600000,
            2, fee_charged=120000,
        )
        plan_result = repository.add_xianshi_plan_items(
            "legacy-plan", "seller", self.plan, fee_charged=200000
        )

        self.assertEqual((fast_result.status, plan_result.status), ("duplicate", "duplicate"))
        with db_backend.connection(legacy_database) as conn:
            self.assertTrue(
                conn.column_exists("xianshi_listing_operations", "stamina_cost")
            )
            self.assertTrue(
                conn.column_exists("xianshi_plan_listing_operations", "stamina_cost")
            )

    def test_commands_do_not_precharge_or_manually_refund_stamina(self) -> None:
        source_path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_trade"
            / "__init__.py"
        )
        source = source_path.read_text(encoding="utf-8")
        auto_start = source.index("@xianshi_auto_add.handle")
        auto_end = source.index("@xianshi_fast_add.handle", auto_start)
        fast_start = auto_end
        fast_end = source.index("@xiuxian_shop_view.handle", fast_start)

        for command in (source[auto_start:auto_end], source[fast_start:fast_end]):
            self.assertIn("Cooldown(cd_time=0)", command)
            self.assertNotIn("stamina_cost=", command.split("async def", 1)[0])
            self.assertNotIn("update_user_stamina", command)


if __name__ == "__main__":
    unittest.main()
