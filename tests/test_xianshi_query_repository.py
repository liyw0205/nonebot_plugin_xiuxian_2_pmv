from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.xianshi_query_repository import (
    XianshiQuerySqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class XianshiQueryRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE xianshi_item ("
                "id TEXT PRIMARY KEY,user_id TEXT,goods_id INTEGER,name TEXT,type TEXT,"
                "price INTEGER,quantity INTEGER)"
            )
            uow.executemany(
                "INSERT INTO xianshi_item VALUES(?,?,?,?,?,?,?)",
                (
                    ("a", "seller-1", 1, "法器", "装备", 20, 2),
                    ("b", "seller-2", 2, "灵草", "药材", 10, 1),
                ),
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_reads_legacy_shape_and_filters(self) -> None:
        repository = XianshiQuerySqlRepository(self.database)
        self.assertEqual([row["id"] for row in repository.get_items()], ["a", "b"])
        self.assertEqual(
            repository.get_items(goods_type="装备")[0]["name"],
            "法器",
        )
        self.assertEqual(
            repository.get_items(listing_id="b", name="灵草")[0]["user_id"],
            "seller-2",
        )
        self.assertEqual(repository.get_items(user_id="missing"), [])

    def test_missing_database_or_table_does_not_create_schema(self) -> None:
        missing = Path(self.temp.name) / "missing.db"
        self.assertEqual(XianshiQuerySqlRepository(missing).get_items(), [])
        self.assertFalse(missing.exists())

        empty = Path(self.temp.name) / "empty.db"
        with DatabaseUnitOfWork(empty):
            pass
        self.assertEqual(XianshiQuerySqlRepository(empty).get_items(), [])
        with DatabaseUnitOfWork(empty) as uow:
            self.assertEqual(
                uow.query_all("SELECT name FROM sqlite_master WHERE type='table'"),
                [],
            )

    def test_query_path_is_read_only(self) -> None:
        XianshiQuerySqlRepository(self.database).get_items()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            with self.assertRaisesRegex(sqlite3.OperationalError, "readonly"):
                uow.execute("CREATE TABLE should_not_exist(value TEXT)")

    def test_trade_application_owns_default_query_repository(self) -> None:
        application = TradeApplication(self.database, Path(self.temp.name) / "trade.db")
        self.assertEqual(application.xianshi_get_items(name="法器")[0]["id"], "a")
        self.assertEqual(
            application.xianshi_query_repository.__class__.__module__,
            "nonebot_plugin_xiuxian_2.features.trade.xianshi_query_repository",
        )
