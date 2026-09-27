import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..application import TradeApplication
from ..guishi_query_repository import GuishiQuerySqlRepository


class GuishiQuerySqlRepositoryTest(unittest.TestCase):
    def test_application_reads_guishi_projections_from_trade_database(self):
        with tempfile.TemporaryDirectory() as directory:
            game_database = Path(directory) / "game.db"
            trade_database = Path(directory) / "trade.db"
            sqlite3.connect(game_database).close()
            with sqlite3.connect(trade_database) as connection:
                connection.execute(
                    "CREATE TABLE guishi_item (id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,"
                    "item_name TEXT,item_type TEXT,price INTEGER,quantity INTEGER,"
                    "filled_quantity INTEGER DEFAULT 0)"
                )
                connection.execute(
                    "INSERT INTO guishi_item(id,user_id,item_id,item_name,item_type,price,quantity) "
                    "VALUES('q1','buyer',1001,'灵草','qiugou',20,3)"
                )
                connection.execute(
                    "CREATE TABLE guishi_info (user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
                )
                connection.execute("INSERT INTO guishi_info VALUES('buyer',75,'{}')")

            application = TradeApplication(game_database, trade_database)

            orders = application.guishi_get_orders(user_id="buyer", order_type="qiugou")
            self.assertEqual(["q1"], [order["id"] for order in orders or []])
            self.assertEqual((75, {}), application.guishi_get_account("buyer"))

    def test_missing_database_is_not_created(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.db"
            repository = GuishiQuerySqlRepository(database)

            self.assertIsNone(repository.get_orders())
            self.assertEqual((0, {}), repository.get_account("user"))
            self.assertFalse(database.exists())

    def test_missing_tables_return_legacy_empty_values_without_ddl(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "trade.db"
            sqlite3.connect(database).close()
            repository = GuishiQuerySqlRepository(database)

            self.assertIsNone(repository.get_orders())
            self.assertEqual((0, {}), repository.get_account("user"))
            with sqlite3.connect(database) as connection:
                self.assertEqual([], connection.execute("SELECT name FROM sqlite_master").fetchall())

    def test_order_filters_preserve_type_aliases_and_row_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "trade.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE guishi_item (id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,"
                    "item_name TEXT,item_type TEXT,price INTEGER,quantity INTEGER,"
                    "filled_quantity INTEGER DEFAULT 0)"
                )
                connection.executemany(
                    "INSERT INTO guishi_item(id,user_id,item_id,item_name,item_type,price,quantity) "
                    "VALUES(?,?,?,?,?,?,?)",
                    [
                        ("q1", "buyer", 1001, "灵草", "求购", 20, 3),
                        ("q2", "buyer", 1002, "灵果", "qiugou", 30, 2),
                        ("b1", "seller", 1001, "灵草", "摆摊", 15, 1),
                    ],
                )
            repository = GuishiQuerySqlRepository(database)

            qiugou = repository.get_orders(user_id="buyer", order_type="qiugou")
            self.assertEqual({"q1", "q2"}, {row["id"] for row in qiugou or []})
            self.assertEqual(
                ["b1"],
                [
                    row["id"]
                    for row in repository.get_orders(
                        name="灵草", order_type="baitan", order_id="b1"
                    )
                    or []
                ],
            )
            self.assertEqual(
                ["id", "user_id", "item_id", "item_name", "item_type", "price", "quantity", "filled_quantity"],
                list(qiugou[0]) if qiugou else [],
            )
            self.assertIsNone(repository.get_orders(user_id="missing"))

    def test_account_defaults_and_json_shape_match_legacy_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "trade.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE guishi_info (user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
                )
                connection.executemany(
                    "INSERT INTO guishi_info VALUES(?,?,?)",
                    [("valid", 75, '{"1001":2}'), ("invalid", 11, "{"), ("array", 4, "[]")],
                )
            repository = GuishiQuerySqlRepository(database)

            self.assertEqual((75, {"1001": 2}), repository.get_account("valid"))
            self.assertEqual((11, {}), repository.get_account("invalid"))
            self.assertEqual((4, {}), repository.get_account("array"))
            self.assertEqual((0, {}), repository.get_account("missing"))


if __name__ == "__main__":
    unittest.main()
