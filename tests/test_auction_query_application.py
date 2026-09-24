from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.auction.query_application import (
    AuctionQueryApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class AuctionQueryApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE auction_current ("
                "id TEXT PRIMARY KEY,item_id INTEGER,name TEXT,start_price INTEGER,"
                "current_price INTEGER,seller_id TEXT,seller_name TEXT,bids TEXT,"
                "bid_times TEXT,is_system INTEGER,last_bid_time REAL)"
            )
            uow.execute(
                "CREATE TABLE auction_history ("
                "id INTEGER PRIMARY KEY,auction_id TEXT,item_name TEXT,status TEXT,"
                "end_time REAL,start_time REAL,winner_id TEXT,final_price INTEGER,"
                "seller_name TEXT,fee INTEGER)"
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_current_rows_keep_legacy_shape_and_json_fallback(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO auction_current VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                ("a1", 1, "法器", 100, 200, "seller", "卖家", '{"u":200}', '{"u":3}', 1, 3),
            )
            uow.execute(
                "INSERT INTO auction_current VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                ("a2", 2, "丹药", 10, 20, "seller", "卖家", "broken", '[1,2]', 0, None),
            )

        application = AuctionQueryApplication(self.database)

        self.assertEqual(application.get_current_auction("a1")["bids"], {"u": 200})
        rows = application.get_current_auction()
        self.assertEqual([row["id"] for row in rows], ["a1", "a2"])
        self.assertEqual(rows[1]["bids"], {})
        self.assertEqual(rows[1]["bid_times"], {})
        self.assertFalse(rows[1]["is_system"])

    def test_history_keeps_descending_order_and_filter(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany(
                "INSERT INTO auction_history(auction_id,item_name,status,end_time,start_time) "
                "VALUES(?,?,?,?,?)",
                (("a", "old", "流拍", 10, 1), ("a", "new", "成交", 20, 11), ("b", "other", "流拍", 15, 5)),
            )

        application = AuctionQueryApplication(self.database)

        self.assertEqual(
            [row["item_name"] for row in application.get_auction_history("a")],
            ["new", "old"],
        )
        self.assertEqual(
            [row["item_name"] for row in application.get_auction_history()],
            ["new", "other", "old"],
        )
        self.assertEqual(application.count_auction_history(), 3)

    def test_missing_database_or_tables_read_empty_without_schema_creation(self) -> None:
        missing = Path(self.temp.name) / "missing.db"
        self.assertEqual(AuctionQueryApplication(missing).get_current_auction(), [])
        self.assertEqual(AuctionQueryApplication(missing).get_auction_history(), [])
        self.assertFalse(missing.exists())

        empty = Path(self.temp.name) / "empty.db"
        with DatabaseUnitOfWork(empty):
            pass
        application = AuctionQueryApplication(empty)
        self.assertEqual(application.get_current_auction(), [])
        self.assertIsNone(application.get_current_auction("a"))
        self.assertEqual(application.get_auction_history(), [])
        with DatabaseUnitOfWork(empty) as uow:
            tables = uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        self.assertEqual(tables, [])

    def test_query_repository_opens_database_read_only(self) -> None:
        application = AuctionQueryApplication(self.database)
        application.get_current_auction()

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            with self.assertRaisesRegex(sqlite3.OperationalError, "readonly"):
                uow.execute("CREATE TABLE should_not_exist(value TEXT)")


class AuctionQueryEntryPointTests(unittest.TestCase):
    def test_view_info_and_activity_matchers_use_query_application(self) -> None:
        source_path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_trade"
            / "__init__.py"
        )
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        functions = {
            node.name: ast.unparse(node)
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

        view = functions["auction_view_"]
        info = functions["auction_info_"]
        activity = functions["auction_activity_"]
        self.assertIn("_auction_query_application().get_current_auction", view)
        self.assertIn("_auction_query_application().get_auction_history", view)
        self.assertIn("_auction_query_application().count_auction_history", info)
        self.assertIn("_auction_query_application().get_current_auction", activity)
        for handler in (view, info, activity):
            self.assertNotIn("xianshi_repository.get_current_auction", handler)
            self.assertNotIn("xianshi_repository.get_auction_history", handler)


if __name__ == "__main__":
    unittest.main()
