from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.errors import DomainError
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

    def test_minimum_prices_exact_projection_and_fresh_next_read(self) -> None:
        huge = 10 ** 40
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany("INSERT INTO xianshi_item VALUES(?,?,?,?,?,?,?)", (
                ("c", "seller", 1, "法器", "装备", b"19", 2),
                ("d", "seller", 2, "巨额", "装备", str(huge).encode("ascii"), 1),
                ("e", "seller", 2, "巨额", "装备", str(huge + 1).encode("ascii"), 1),
                ("unmatched", "seller", 3, "ignored", "装备", 1.5, 1),
            ))
        repository = XianshiQuerySqlRepository(self.database)
        self.assertEqual(repository.minimum_prices(iter(["法器", "法器", "巨额", "missing"])),
                         {"法器": 19, "巨额": huge})
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE xianshi_item SET price=7 WHERE id='c'")
        self.assertEqual(repository.minimum_prices(["法器"]), {"法器": 7})
        application = TradeApplication(self.database, Path(self.temp.name) / "trade.db")
        self.assertEqual(application.xianshi_minimum_prices(["法器"]), {"法器": 7})

    def test_minimum_prices_validate_nonminimum_rows_beyond_stream_batch(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany("INSERT INTO xianshi_item VALUES(?,?,?,?,?,?,?)", (
                (f"ok-{index}", "seller", 1, "法器", "装备", 30, 1)
                for index in range(130)
            ))
            uow.execute("INSERT INTO xianshi_item VALUES(?,?,?,?,?,?,?)",
                        ("bad", "seller", 1, "法器", "装备", float(2 ** 70), 1))
        repository = XianshiQuerySqlRepository(self.database)
        with self.assertRaises(DomainError) as old:
            repository.get_items(name="法器")
        with self.assertRaises(DomainError) as new:
            repository.minimum_prices(["法器"])
        self.assertEqual(new.exception.code, old.exception.code)
        self.assertEqual(new.exception.details, old.exception.details)
        self.assertEqual(new.exception.details["subject"], "bad")
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE xianshi_item SET price=10 WHERE id='bad'")
        self.assertEqual(repository.minimum_prices(["法器"]), {"法器": 10})

    def test_minimum_prices_one_read_scope_and_bounded_name_chunks(self) -> None:
        statements = []
        scopes = []

        class ObservedUnitOfWork(DatabaseUnitOfWork):
            def __enter__(self):
                scopes.append(self.read_only)
                return super().__enter__()

            def execute(self, sql, params=()):
                statements.append((sql, tuple(params)))
                return super().execute(sql, params)

        names = [f"name-{index}" for index in range(300)]
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany("INSERT INTO xianshi_item VALUES(?,?,?,?,?,?,?)", (
                (f"row-{index}", "seller", index, name, "药材", index, 1)
                for index, name in enumerate(names)
            ))
        with patch("nonebot_plugin_xiuxian_2.features.trade.xianshi_query_repository.DatabaseUnitOfWork",
                   ObservedUnitOfWork):
            result = XianshiQuerySqlRepository(self.database).minimum_prices(names + names)
        self.assertEqual(result, {name: index for index, name in enumerate(names)})
        self.assertEqual(scopes, [True])
        reads = [(sql, params) for sql, params in statements if "FROM xianshi_item" in sql]
        self.assertEqual([len(params) for _, params in reads], [256, 44])
        self.assertTrue(all(sql.startswith("SELECT id,name,price ") for sql, _ in reads))

    def test_minimum_prices_missing_file_table_and_empty_names(self) -> None:
        missing = Path(self.temp.name) / "missing.db"
        self.assertEqual(XianshiQuerySqlRepository(missing).minimum_prices(["a"]), {})
        self.assertFalse(missing.exists())
        empty = Path(self.temp.name) / "empty.db"
        with DatabaseUnitOfWork(empty):
            pass
        self.assertEqual(XianshiQuerySqlRepository(empty).minimum_prices(["a"]), {})
        with DatabaseUnitOfWork(empty, read_only=True) as uow:
            self.assertEqual(uow.query_all("SELECT name FROM sqlite_master WHERE type='table'"), [])
        with patch("nonebot_plugin_xiuxian_2.features.trade.xianshi_query_repository.DatabaseUnitOfWork") as factory:
            self.assertEqual(XianshiQuerySqlRepository(self.database).minimum_prices([]), {})
        factory.assert_not_called()
