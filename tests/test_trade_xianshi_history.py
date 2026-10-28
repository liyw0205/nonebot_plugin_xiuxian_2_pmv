from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.migrations import apply_trade_xianshi_purchase
from nonebot_plugin_xiuxian_2.features.trade.xianshi_history_view import render_xianshi_trades
from nonebot_plugin_xiuxian_2.features.trade.xianshi_query_repository import (
    XianshiQuerySqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class _HistoryFixture:
    def __init__(self, root: Path) -> None:
        self.database = root / "game.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_trade_xianshi_purchase(uow)
            uow.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT)"
            )
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name) VALUES(?,?)",
                (("u1", "甲"), ("u2", "乙"), ("u5", "丙"), ("u6", "丁")),
            )
            uow.executemany(
                "INSERT INTO xianshi_operations(operation_id,listing_id,buyer_id,"
                "seller_id,goods_id,name,goods_type,quantity,total_cost,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    ("op1", "l1", "u2", "u1", 1001, "灵剑", "装备", 1, 800, "2026-10-20 10:00:00"),
                    ("op2", "l2", "u1", "u2", 1002, "灵草", "药材", 2, 300, "2026-10-21 11:00:00"),
                    ("op3", "l3", "u1", "0", 1003, "系统丹", "丹药", 1, 100, "2026-10-22 12:00:00"),
                    ("op4", "l4", "u6", "u5", 1004, "别人的", "装备", 1, 999, "2026-10-23 13:00:00"),
                ),
            )


class XianshiHistoryRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fixture = _HistoryFixture(Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _trades(self, user_id: str = "u1", **kwargs):
        return XianshiQuerySqlRepository(self.fixture.database).my_trades(
            user_id, **kwargs
        )

    def test_returns_only_the_players_trades_in_reverse_insertion_order(self) -> None:
        trades = self._trades()

        self.assertEqual(["op3", "op2", "op1"], [row["operation_id"] for row in trades])
        self.assertNotIn("op4", [row["operation_id"] for row in trades])

    def test_roles_and_counterparties_cover_sales_purchases_and_system_seller(self) -> None:
        trades = {row["operation_id"]: row for row in self._trades()}

        self.assertEqual("buyer", trades["op3"]["role"])
        self.assertEqual("系统", trades["op3"]["counterparty"])
        self.assertEqual("buyer", trades["op2"]["role"])
        self.assertEqual("乙", trades["op2"]["counterparty"])
        self.assertEqual("seller", trades["op1"]["role"])
        self.assertEqual("乙", trades["op1"]["counterparty"])

    def test_trade_fields_are_projected(self) -> None:
        trade = self._trades()[2]

        self.assertEqual(("灵剑", "装备", 1, 800), (
            trade["name"], trade["goods_type"], trade["quantity"], trade["total_cost"]
        ))
        self.assertEqual("2026-10-20 10:00:00", trade["created_at"])

    def test_limit_keeps_the_most_recent_rows(self) -> None:
        trades = self._trades(limit=2)

        self.assertEqual(["op3", "op2"], [row["operation_id"] for row in trades])

    def test_missing_operations_table_returns_empty_without_ddl(self) -> None:
        empty = Path(self.temp_dir.name) / "empty.sqlite3"
        sqlite3.connect(empty).close()

        self.assertEqual([], XianshiQuerySqlRepository(empty).my_trades("u1"))
        with sqlite3.connect(empty) as connection:
            self.assertEqual(
                [], connection.execute("SELECT name FROM sqlite_master").fetchall()
            )

    def test_missing_database_returns_empty_without_creating_a_file(self) -> None:
        missing = Path(self.temp_dir.name) / "absent.sqlite3"

        self.assertEqual([], XianshiQuerySqlRepository(missing).my_trades("u1"))
        self.assertFalse(missing.exists())

    def test_missing_user_table_still_returns_trades_with_fallback_names(self) -> None:
        with DatabaseUnitOfWork(self.fixture.database) as uow:
            uow.execute("DROP TABLE user_xiuxian")

        trades = {row["operation_id"]: row for row in self._trades()}

        self.assertEqual("u2", trades["op1"]["counterparty"])
        self.assertEqual("u2", trades["op2"]["counterparty"])
        self.assertEqual("系统", trades["op3"]["counterparty"])

    def test_blank_user_returns_empty(self) -> None:
        self.assertEqual([], self._trades(""))


class XianshiHistoryRenderingTests(unittest.TestCase):
    def test_sale_and_purchase_lines_include_amounts_and_summary(self) -> None:
        lines = render_xianshi_trades(
            (
                {
                    "name": "灵剑",
                    "quantity": 1,
                    "total_cost": 800,
                    "role": "seller",
                    "counterparty": "乙",
                    "created_at": "2026-10-20 10:00:00",
                },
                {
                    "name": "灵草",
                    "quantity": 2,
                    "total_cost": 300,
                    "role": "buyer",
                    "counterparty": "乙",
                    "created_at": "2026-10-21 11:00:00",
                },
            )
        )

        self.assertEqual(
            "卖出｜灵剑 x1｜800灵石｜买家:乙｜10-20 10:00", lines[0]
        )
        self.assertEqual(
            "买入｜灵草 x2｜300灵石｜卖家:乙｜10-21 11:00", lines[1]
        )
        self.assertEqual("最近 2 笔：卖出 1 笔/收入 800｜买入 1 笔/支出 300", lines[2])

    def test_empty_records_render_nothing(self) -> None:
        self.assertEqual((), render_xianshi_trades(()))

    def test_bad_timestamp_falls_back(self) -> None:
        lines = render_xianshi_trades(
            (
                {
                    "name": "灵剑",
                    "quantity": 1,
                    "total_cost": 1,
                    "role": "seller",
                    "counterparty": "乙",
                    "created_at": "bad",
                },
            )
        )

        self.assertIn("时间未知", lines[0])


class XianshiHistoryWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.command_source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "__init__.py"
        ).read_text(encoding="utf-8")
        cls.help_source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "trade_help.py"
        ).read_text(encoding="utf-8")

    def test_my_xian_shop_exposes_the_record_view(self) -> None:
        start = self.command_source.index("async def my_xian_shop_(")
        handler = self.command_source[
            start : self.command_source.index("@xian_shop_remove.handle", start)
        ]

        self.assertIn("xianshi_my_trades(", handler)
        self.assertIn("render_xianshi_trades(", handler)
        self.assertIn("我的仙肆 记录", handler)
        self.assertIn("使用【我的仙肆 记录】查看最近成交", handler)

    def test_listing_pagination_is_unchanged(self) -> None:
        start = self.command_source.index("async def my_xian_shop_(")
        handler = self.command_source[
            start : self.command_source.index("@xian_shop_remove.handle", start)
        ]

        self.assertIn("per_page = 20", handler)
        self.assertIn("send_msg_handler(bot, event, '我的仙肆'", handler)

    def test_help_documents_the_record_action(self) -> None:
        self.assertIn("我的仙肆 记录", self.help_source)


if __name__ == "__main__":
    unittest.main()
