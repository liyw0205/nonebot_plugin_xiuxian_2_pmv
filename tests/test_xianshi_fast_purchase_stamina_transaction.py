from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class XianshiFastPurchaseStaminaTransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "fast-purchase.sqlite3"
        with db_backend.transaction(self.database) as conn:
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
                    create_time TEXT, update_time TEXT, bind_num INTEGER DEFAULT 0,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s)",
                (
                    ("buyer", 1000, 20),
                    ("buyer-2", 1000, 20),
                    ("seller-1", 100, 0),
                    ("seller-2", 50, 0),
                ),
            )
        self.repository = TradeRepository(self.database, max_goods_num=99)
        self.repository.initialize()
        self.first_listing = self.repository.add_xianshi_item(
            "seller-1", 1001, "Test weapon", "equipment", 200, 2
        )
        self.second_listing = self.repository.add_xianshi_item(
            "seller-2", 1002, "Test herb", "herb", 300, 1
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def purchase(
        self,
        operation_id: str,
        listing_id: str,
        *,
        buyer_id: str = "buyer",
        stamina_operation_id: str = "fast-batch",
        stamina_cost: int = 10,
    ):
        return self.repository.purchase_xianshi_item(
            operation_id,
            buyer_id,
            listing_id,
            1,
            stamina_operation_id=stamina_operation_id,
            stamina_cost=stamina_cost,
        )

    def test_shared_stamina_operation_charges_only_first_success(self) -> None:
        first = self.purchase("purchase-1", self.first_listing)
        second = self.purchase("purchase-2", self.second_listing)

        self.assertEqual((first.status, second.status), ("purchased", "purchased"))
        self.assertEqual((first.stamina_charged, second.stamina_charged), (10, 0))
        self.assertEqual(
            self.scalar(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", ("buyer",)
            ),
            10,
        )
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("buyer",)),
            500,
        )
        self.assertEqual(
            self.scalar(
                "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller-1",)
            ),
            300,
        )
        self.assertEqual(
            self.scalar(
                "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller-2",)
            ),
            350,
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_operations"), 2)
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM xianshi_stamina_operations"), 1
        )

    def test_stamina_insufficient_rejects_purchase_without_mutation(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s",
                (9, "buyer"),
            )

        result = self.purchase("purchase-no-stamina", self.first_listing)

        self.assertEqual(result.status, "stamina_insufficient")
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("buyer",)),
            1000,
        )
        self.assertEqual(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (self.first_listing,)),
            2,
        )
        self.assertIsNone(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("buyer",))
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_operations"), 0)
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM xianshi_stamina_operations"), 0
        )

    def test_failed_purchase_attempts_do_not_charge_stamina(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (100, "buyer")
            )
        poor = self.purchase("purchase-poor", self.first_listing)
        self.assertEqual(poor.status, "stone_insufficient")

        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (1000, "buyer")
            )
            conn.execute(
                "INSERT INTO back (user_id, goods_id, goods_num, bind_num) "
                "VALUES (%s, %s, %s, %s)",
                ("buyer", 1001, 99, 99),
            )
        full = self.purchase("purchase-full", self.first_listing)
        missing = self.purchase("purchase-missing", "missing-listing")

        self.assertEqual((full.status, missing.status), ("inventory_full", "listing_missing"))
        self.assertEqual(
            self.scalar(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", ("buyer",)
            ),
            20,
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_operations"), 0)
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM xianshi_stamina_operations"), 0
        )

    def test_purchase_replay_and_changed_stamina_input_are_idempotent(self) -> None:
        first = self.purchase("purchase-repeat", self.first_listing)
        duplicate = self.purchase("purchase-repeat", self.first_listing)
        conflict = self.purchase(
            "purchase-repeat",
            self.first_listing,
            stamina_operation_id="changed-batch",
        )

        self.assertEqual(
            (first.status, duplicate.status, conflict.status),
            ("purchased", "duplicate", "state_changed"),
        )
        self.assertEqual((first.stamina_charged, duplicate.stamina_charged), (10, 10))
        self.assertEqual(
            self.scalar(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", ("buyer",)
            ),
            10,
        )
        self.assertEqual(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (self.first_listing,)),
            1,
        )

    def test_stamina_operation_rejects_changed_buyer_or_cost(self) -> None:
        self.purchase("purchase-first", self.first_listing)
        changed_buyer = self.purchase(
            "purchase-other-buyer", self.second_listing, buyer_id="buyer-2"
        )
        changed_cost = self.purchase(
            "purchase-other-cost", self.second_listing, stamina_cost=11
        )

        self.assertEqual(
            (changed_buyer.status, changed_cost.status),
            ("state_changed", "state_changed"),
        )
        self.assertEqual(
            self.scalar(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", ("buyer-2",)
            ),
            20,
        )
        self.assertEqual(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (self.second_listing,)),
            1,
        )

    def test_operation_insert_failure_rolls_back_stamina_and_trade(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_purchase_operation "
                "BEFORE INSERT ON xianshi_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("purchase-fail", self.first_listing)

        self.assertEqual(
            self.scalar(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", ("buyer",)
            ),
            20,
        )
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("buyer",)),
            1000,
        )
        self.assertEqual(
            self.scalar(
                "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller-1",)
            ),
            100,
        )
        self.assertEqual(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (self.first_listing,)),
            2,
        )
        self.assertIsNone(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("buyer",))
        )
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM xianshi_stamina_operations"), 0
        )

    def test_legacy_purchase_operations_gain_stamina_columns(self) -> None:
        legacy_database = Path(self.temp_dir.name) / "legacy-purchase.sqlite3"
        with db_backend.transaction(legacy_database) as conn:
            conn.execute(
                "CREATE TABLE xianshi_operations ("
                "operation_id TEXT PRIMARY KEY, listing_id TEXT NOT NULL, "
                "buyer_id TEXT NOT NULL, seller_id TEXT NOT NULL, "
                "goods_id INTEGER NOT NULL, name TEXT NOT NULL, "
                "goods_type TEXT NOT NULL, quantity INTEGER NOT NULL, "
                "total_cost INTEGER NOT NULL)"
            )
            conn.execute(
                "INSERT INTO xianshi_operations VALUES "
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    "legacy-operation",
                    "legacy-listing",
                    "buyer",
                    "seller",
                    1001,
                    "Test weapon",
                    "equipment",
                    1,
                    200,
                ),
            )
        repository = TradeRepository(legacy_database, max_goods_num=99)
        repository.initialize()

        result = repository.purchase_xianshi_item(
            "legacy-operation", "buyer", "legacy-listing", 1
        )

        self.assertEqual(result.status, "duplicate")
        self.assertEqual(result.stamina_charged, 0)
        with db_backend.connection(legacy_database) as conn:
            for column in (
                "stamina_operation_id",
                "stamina_cost",
                "stamina_charged",
            ):
                self.assertTrue(conn.column_exists("xianshi_operations", column))
            self.assertTrue(conn.table_exists("xianshi_stamina_operations"))

    def test_fast_purchase_command_has_no_stamina_precharge_or_refund(self) -> None:
        source_path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_trade"
            / "__init__.py"
        )
        source = source_path.read_text(encoding="utf-8")
        start = source.index("@xianshi_fast_buy.handle")
        command = source[start:source.index("@xian_shop_off_all.handle", start)]

        self.assertIn("Cooldown(cd_time=0)", command)
        self.assertNotIn("update_user_stamina", command)
        self.assertIn("stamina_operation_id=stamina_operation_id", command)
        self.assertIn("stamina_cost=10", command)


if __name__ == "__main__":
    unittest.main()
