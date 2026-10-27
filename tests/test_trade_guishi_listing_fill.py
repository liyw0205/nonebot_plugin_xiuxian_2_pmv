from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_listing_fill import (
    GuishiListingFillService,
)
from nonebot_plugin_xiuxian_2.features.trade.guishi_match_repository import (
    GuishiOrderMatchSqlRepository,
)


class GuishiListingFillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.trade_database = root / "trade.sqlite3"
        sqlite3.connect(self.game_database).close()
        with sqlite3.connect(self.trade_database) as connection:
            connection.execute(
                "CREATE TABLE guishi_item (id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,"
                "item_name TEXT,item_type TEXT,price INTEGER,quantity INTEGER,"
                "filled_quantity INTEGER DEFAULT 0)"
            )
            connection.execute(
                "CREATE TABLE guishi_info (user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
            )
            connection.execute(
                "CREATE TABLE guishi_order_create_operations (operation_id TEXT PRIMARY KEY,"
                "payload TEXT NOT NULL,order_id TEXT NOT NULL,order_type TEXT NOT NULL,"
                "amount INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            connection.execute(
                "CREATE TABLE guishi_match_operations (operation_id TEXT PRIMARY KEY,"
                "payload TEXT NOT NULL,result TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
        self.repository = GuishiOrderMatchSqlRepository(self.trade_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _order(
        self,
        order_id: str,
        *,
        user_id: str,
        item_type: str,
        price: int,
        quantity: int,
        filled: int = 0,
        item_name: str = "灵草",
        item_id: int = 1001,
        created_at: str | None = None,
    ) -> None:
        with sqlite3.connect(self.trade_database) as connection:
            connection.execute(
                "INSERT INTO guishi_item(id,user_id,item_id,item_name,item_type,price,"
                "quantity,filled_quantity) VALUES(?,?,?,?,?,?,?,?)",
                (order_id, user_id, item_id, item_name, item_type, price, quantity, filled),
            )
            if created_at is not None:
                connection.execute(
                    "INSERT INTO guishi_order_create_operations("
                    "operation_id,payload,order_id,order_type,amount,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        f"create-{order_id}",
                        "[]",
                        order_id,
                        "qiugou" if item_type in {"qiugou", "求购"} else "baitan",
                        price * quantity,
                        created_at,
                    ),
                )

    def _account(self, user_id: str) -> tuple[int, dict[str, int]]:
        with sqlite3.connect(self.trade_database) as connection:
            row = connection.execute(
                "SELECT stored_stone,items FROM guishi_info WHERE user_id=?", (user_id,)
            ).fetchone()
        if row is None:
            return 0, {}
        return int(row[0]), json.loads(row[1] or "{}")

    def _seed_stone(self, user_id: str, amount: int) -> None:
        with sqlite3.connect(self.trade_database) as connection:
            connection.execute(
                "INSERT INTO guishi_info(user_id,stored_stone,items) VALUES(?,?,'{}') "
                "ON CONFLICT(user_id) DO UPDATE SET stored_stone=?",
                (user_id, amount, amount),
            )

    def _rows(self) -> list[tuple[str, int, int]]:
        with sqlite3.connect(self.trade_database) as connection:
            return [
                (str(row[0]), int(row[1]), int(row[2]))
                for row in connection.execute(
                    "SELECT id,quantity,COALESCE(filled_quantity,0) FROM guishi_item ORDER BY id"
                ).fetchall()
            ]

    def _service(self) -> GuishiListingFillService:
        return GuishiListingFillService(self.trade_database, match=self.repository.match)

    def test_stall_fills_the_oldest_buy_order_first(self) -> None:
        self._order(
            "old",
            user_id="buyer-a",
            item_type="qiugou",
            price=30,
            quantity=6,
            created_at="2026-03-05 20:10:00",
        )
        self._order(
            "new",
            user_id="buyer-b",
            item_type="qiugou",
            price=40,
            quantity=4,
            created_at="2026-03-05 21:10:00",
        )
        self._seed_stone("buyer-a", 0)
        self._seed_stone("buyer-b", 0)
        self._order(
            "stall", user_id="seller", item_type="baitan", price=20, quantity=10
        )

        results = self._service().fill(baitan_order_id="stall")

        self.assertEqual(["old", "new"], [result.qiugou_order_id for result in results])
        self.assertEqual([6, 4], [result.quantity for result in results])
        self.assertEqual([], self._rows())
        self.assertEqual((60, {"1001": 6}), self._account("buyer-a"))
        self.assertEqual((80, {"1001": 4}), self._account("buyer-b"))
        self.assertEqual((200, {}), self._account("seller"))

    def test_partial_fill_keeps_the_remaining_buy_order(self) -> None:
        self._seed_stone("buyer", 0)
        self._order(
            "want",
            user_id="buyer",
            item_type="qiugou",
            price=25,
            quantity=8,
            created_at="2026-03-05 20:00:00",
        )
        self._order("stall", user_id="seller", item_type="baitan", price=20, quantity=5)

        results = self._service().fill(baitan_order_id="stall")

        self.assertEqual([5], [result.quantity for result in results])
        self.assertEqual([("want", 8, 5)], self._rows())
        self.assertEqual((25, {"1001": 5}), self._account("buyer"))
        self.assertEqual((100, {}), self._account("seller"))

    def test_stall_above_the_bid_does_not_match(self) -> None:
        self._seed_stone("buyer", 0)
        self._order(
            "want",
            user_id="buyer",
            item_type="qiugou",
            price=10,
            quantity=5,
            created_at="2026-03-05 20:00:00",
        )
        self._order("stall", user_id="seller", item_type="baitan", price=20, quantity=5)

        self.assertEqual([], self._service().fill(baitan_order_id="stall"))
        self.assertEqual([("stall", 5, 0), ("want", 5, 0)], self._rows())
        self.assertEqual((0, {}), self._account("buyer"))

    def test_a_player_cannot_fill_their_own_buy_order(self) -> None:
        self._seed_stone("trader", 0)
        self._order(
            "want",
            user_id="trader",
            item_type="qiugou",
            price=30,
            quantity=5,
            created_at="2026-03-05 20:00:00",
        )
        self._order("stall", user_id="trader", item_type="baitan", price=20, quantity=5)

        self.assertEqual([], self._service().fill(baitan_order_id="stall"))
        self.assertEqual([("stall", 5, 0), ("want", 5, 0)], self._rows())

    def test_unrelated_item_or_type_is_ignored(self) -> None:
        self._seed_stone("buyer", 0)
        self._order(
            "other-item",
            user_id="buyer",
            item_type="qiugou",
            price=50,
            quantity=5,
            item_name="灵果",
            item_id=1002,
            created_at="2026-03-05 20:00:00",
        )
        self._order("stall", user_id="seller", item_type="baitan", price=20, quantity=5)

        self.assertEqual([], self._service().fill(baitan_order_id="stall"))
        self.assertEqual(2, len(self._rows()))

    def test_buy_order_without_anchor_is_matched_after_anchored_ones(self) -> None:
        self._seed_stone("buyer-legacy", 0)
        self._seed_stone("buyer-new", 0)
        self._order(
            "legacy",
            user_id="buyer-legacy",
            item_type="qiugou",
            price=30,
            quantity=5,
            created_at=None,
        )
        self._order(
            "anchored",
            user_id="buyer-new",
            item_type="qiugou",
            price=30,
            quantity=5,
            created_at="2026-03-05 22:00:00",
        )
        self._order("stall", user_id="seller", item_type="baitan", price=20, quantity=10)

        results = self._service().fill(baitan_order_id="stall")

        self.assertEqual(
            ["anchored", "legacy"], [result.qiugou_order_id for result in results]
        )

    def test_sold_out_stall_stops_matching(self) -> None:
        self._seed_stone("buyer-a", 0)
        self._seed_stone("buyer-b", 0)
        for order_id, buyer in (("a", "buyer-a"), ("b", "buyer-b")):
            self._order(
                order_id,
                user_id=buyer,
                item_type="qiugou",
                price=30,
                quantity=5,
                created_at=f"2026-03-05 2{order_id == 'b'}:00:00",
            )
        self._order("stall", user_id="seller", item_type="baitan", price=20, quantity=5)

        results = self._service().fill(baitan_order_id="stall")

        self.assertEqual(1, len(results))
        self.assertEqual([("b", 5, 0)], self._rows())

    def test_repeated_fill_does_not_match_twice(self) -> None:
        self._seed_stone("buyer", 0)
        self._order(
            "want",
            user_id="buyer",
            item_type="qiugou",
            price=30,
            quantity=5,
            created_at="2026-03-05 20:00:00",
        )
        self._order("stall", user_id="seller", item_type="baitan", price=20, quantity=5)

        first = self._service().fill(baitan_order_id="stall")
        second = self._service().fill(baitan_order_id="stall")

        self.assertEqual(1, len(first))
        self.assertEqual([], second)
        self.assertEqual((50, {"1001": 5}), self._account("buyer"))
        self.assertEqual((100, {}), self._account("seller"))

    def test_missing_listing_is_a_noop(self) -> None:
        self.assertEqual([], self._service().fill(baitan_order_id="absent"))

    def test_missing_trade_database_is_not_created(self) -> None:
        missing = Path(self.temp_dir.name) / "absent.sqlite3"
        service = GuishiListingFillService(missing, match=self.repository.match)

        self.assertEqual([], service.fill(baitan_order_id="stall"))
        self.assertFalse(missing.exists())

    def test_application_matches_the_stall_it_just_created(self) -> None:
        self._seed_stone("buyer", 0)
        self._order(
            "want",
            user_id="buyer",
            item_type="qiugou",
            price=30,
            quantity=5,
            created_at="2026-03-05 20:00:00",
        )
        self._order("stall", user_id="seller", item_type="baitan", price=20, quantity=5)
        application = TradeApplication(self.game_database, self.trade_database)

        results = application.guishi_fill_standing_qiugou(baitan_order_id="stall")

        self.assertEqual(["want"], [result.qiugou_order_id for result in results])
        self.assertEqual(50, results[0].price_refund)
        self.assertEqual((50, {"1001": 5}), self._account("buyer"))


class GuishiListingFillWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.command_source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_trade"
            / "__init__.py"
        ).read_text(encoding="utf-8")

    def _slice(self, start: str, end: str) -> str:
        begin = self.command_source.index(start)
        return self.command_source[begin : self.command_source.index(end, begin)]

    def test_stall_handler_matches_standing_buy_orders(self) -> None:
        handler = self._slice("async def guishi_baitan_", "@guishi_shoutan.handle")

        self.assertIn("trade_application.guishi_baitan(", handler)
        self.assertIn("trade_application.guishi_fill_standing_qiugou(", handler)
        self.assertNotIn("match_guishi_orders(", handler)

    def test_both_match_paths_share_one_statistics_recorder(self) -> None:
        sweep = self._slice("async def process_guishi_transactions", "@scheduler.scheduled_job")

        self.assertIn("_record_guishi_match_event(result", sweep)
        self.assertNotIn('"鬼市成交"', sweep)


if __name__ == "__main__":
    unittest.main()
