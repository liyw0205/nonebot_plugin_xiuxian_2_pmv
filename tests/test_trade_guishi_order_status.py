from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_order_status import (
    GuishiOrderStatusService,
)
from nonebot_plugin_xiuxian_2.features.trade.guishi_order_view import (
    format_remaining,
    order_status_line,
)


class _FixedClock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


class GuishiOrderLineTests(unittest.TestCase):
    def test_buy_order_line_shows_deadline_and_remaining_time(self) -> None:
        line = order_status_line(
            {
                "id": "q1",
                "item_name": "灵草",
                "item_type": "qiugou",
                "price": 12000,
                "quantity": 5,
                "filled_quantity": 2,
            },
            created_at="2026-03-05 21:30:00",
            now=datetime(2026, 3, 5, 23, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(
            "ID:q1 灵草 12,000灵石 x5 (待购:3) 到期：03-06 08:00（剩余 8小时30分）",
            line,
        )

    def test_stall_line_uses_the_same_window_with_a_stall_marker(self) -> None:
        line = order_status_line(
            {
                "id": "b1",
                "item_name": "灵果",
                "item_type": "baitan",
                "price": 500,
                "quantity": 3,
                "filled_quantity": 0,
            },
            created_at="2026-03-05 21:30:00",
            now=datetime(2026, 3, 5, 23, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(
            "ID:b1 灵果 500灵石 x3 (待售:3) 截止：03-06 08:00（剩余 9小时0分）",
            line,
        )

    def test_expired_order_is_marked_as_waiting_for_cleanup(self) -> None:
        line = order_status_line(
            {
                "id": "q1",
                "item_name": "灵草",
                "item_type": "qiugou",
                "price": 10,
                "quantity": 1,
                "filled_quantity": 0,
            },
            created_at="2026-03-05 21:30:00",
            now=datetime(2026, 3, 6, 9, 0, tzinfo=timezone.utc),
        )

        self.assertIn("到期：已到期（等待清理）", line)

    def test_order_without_anchor_says_the_deadline_is_unknown(self) -> None:
        line = order_status_line(
            {
                "id": "old",
                "item_name": "灵草",
                "item_type": "qiugou",
                "price": 10,
                "quantity": 1,
                "filled_quantity": 0,
            },
            created_at=None,
            now=datetime(2026, 3, 6, 9, 0, tzinfo=timezone.utc),
        )

        self.assertIn("到期：未知（历史订单）", line)
        self.assertNotIn("剩余", line)

    def test_remaining_formats_minutes_below_an_hour(self) -> None:
        self.assertEqual("45分", format_remaining(45 * 60 + 30))
        self.assertEqual("2小时5分", format_remaining(2 * 3600 + 5 * 60))
        self.assertEqual("0分", format_remaining(-10))


class GuishiOrderStatusServiceTests(unittest.TestCase):
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
                "CREATE TABLE guishi_order_create_operations (operation_id TEXT PRIMARY KEY,"
                "payload TEXT NOT NULL,order_id TEXT NOT NULL,order_type TEXT NOT NULL,"
                "amount INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            connection.executemany(
                "INSERT INTO guishi_item(id,user_id,item_id,item_name,item_type,price,"
                "quantity,filled_quantity) VALUES(?,?,?,?,?,?,?,?)",
                (
                    ("q1", "buyer", 1001, "灵草", "qiugou", 30, 5, 1),
                    ("b1", "buyer", 1002, "灵果", "baitan", 40, 2, 0),
                    ("other", "someone", 1001, "灵草", "qiugou", 30, 1, 0),
                ),
            )
            connection.executemany(
                "INSERT INTO guishi_order_create_operations("
                "operation_id,payload,order_id,order_type,amount,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (
                    ("create-q1", "[]", "q1", "qiugou", 150, "2026-03-05 21:00:00"),
                    ("create-b1", "[]", "b1", "baitan", 80, "2026-03-05 22:00:00"),
                ),
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_only_the_players_own_orders_are_rendered(self) -> None:
        service = GuishiOrderStatusService(
            self.trade_database,
            clock=_FixedClock(datetime(2026, 3, 5, 23, 0, tzinfo=timezone.utc)),
        )

        lines = service.lines_for("buyer")

        self.assertEqual(1, len(lines["qiugou"]))
        self.assertEqual(1, len(lines["baitan"]))
        self.assertTrue(lines["qiugou"][0].startswith("ID:q1 灵草 30灵石 x5 (待购:4)"))
        self.assertNotIn("other", " ".join(lines["qiugou"]))

    def test_missing_anchor_table_still_renders_unknown_deadlines(self) -> None:
        with sqlite3.connect(self.trade_database) as connection:
            connection.execute("DROP TABLE guishi_order_create_operations")
        service = GuishiOrderStatusService(
            self.trade_database,
            clock=_FixedClock(datetime(2026, 3, 6, 9, 0, tzinfo=timezone.utc)),
        )

        lines = service.lines_for("buyer")

        self.assertIn("到期：未知（历史订单）", lines["qiugou"][0])
        with sqlite3.connect(self.trade_database) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        self.assertNotIn("guishi_order_create_operations", tables)

    def test_empty_player_renders_no_sections(self) -> None:
        service = GuishiOrderStatusService(self.trade_database)

        self.assertEqual({"qiugou": [], "baitan": []}, service.lines_for("nobody"))

    def test_missing_database_is_not_created(self) -> None:
        missing = Path(self.temp_dir.name) / "absent.sqlite3"
        service = GuishiOrderStatusService(missing)

        self.assertEqual({"qiugou": [], "baitan": []}, service.lines_for("buyer"))
        self.assertFalse(missing.exists())

    def test_application_exposes_the_status_lines(self) -> None:
        application = TradeApplication(
            self.game_database,
            self.trade_database,
            clock=_FixedClock(datetime(2026, 3, 5, 23, 0, tzinfo=timezone.utc)),
        )

        lines = application.guishi_order_status_lines("buyer")

        self.assertIn("剩余 9小时0分", lines["qiugou"][0])


class GuishiOrderStatusWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.command_source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "__init__.py"
        ).read_text(encoding="utf-8")
        cls.help_source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "trade_help.py"
        ).read_text(encoding="utf-8")

    def _slice(self, start: str, end: str) -> str:
        begin = self.command_source.index(start)
        return self.command_source[begin : self.command_source.index(end, begin)]

    def test_info_handler_renders_the_lifecycle_view(self) -> None:
        info_handler = self._slice("async def guishi_info_(", "async def clear_all_guishi_")

        self.assertIn("trade_application.guishi_order_status_lines(user_id)", info_handler)
        self.assertIn("鬼市取消求购", info_handler)

    def test_stall_message_no_longer_promises_confiscation(self) -> None:
        handler = self._slice("async def guishi_baitan_", "@guishi_shoutan.handle")

        self.assertNotIn("物品不退还", handler)
        self.assertIn("未售出的物品会退回背包", handler)

    def test_help_documents_the_lifecycle_commands_and_deadline(self) -> None:
        self.assertIn("鬼市取消求购 订单ID", self.help_source)
        self.assertIn("鬼市取物品 物品名", self.help_source)
        self.assertIn("次日 8 点", self.help_source)


if __name__ == "__main__":
    unittest.main()
