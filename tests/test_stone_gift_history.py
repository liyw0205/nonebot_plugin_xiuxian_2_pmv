"""玩家「我的赠送流水」只读聚合与命令合同。"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.stone_gift import migrations as gift_migrations
from nonebot_plugin_xiuxian_2.features.stone_gift.history import (
    EMPTY_TEXT,
    StoneGiftHistoryApplication,
    format_stone_gift_history,
)
from nonebot_plugin_xiuxian_2.features.stone_gift.manifest import FEATURE
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
COMMANDS = ROOT / "nonebot_plugin_xiuxian_2" / "adapters" / "nonebot" / "commands.py"
HELP = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_base" / "__init__.py"

NOW = datetime(2026, 1, 10, 12, 0, 0)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class HistoryFixture:
    def __init__(self, root: Path, *, migrate: bool = True) -> None:
        self.database = root / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT)"
            )
            for user_id, name in (("u", "甲"), ("a", "乙"), ("b", "丙"), ("c", "丁"), ("d", "戊"), ("x", "己"), ("y", "庚")):
                conn.execute("INSERT INTO user_xiuxian VALUES (?, ?)", (user_id, name))
        if migrate:
            with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
                gift_migrations.apply_stone_gift(uow)

    def add(
        self,
        operation_id: str,
        sender: str,
        recipient: str,
        gross: int,
        net: int,
        fee: int,
        created_at: str,
    ) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO stone_gift_operations("
                "operation_id,sender_id,recipient_id,gross_amount,net_amount,fee_amount,created_at"
                ") VALUES (?,?,?,?,?,?,?)",
                (operation_id, sender, recipient, gross, net, fee, created_at),
            )

    def tables(self) -> set[str]:
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }


class StoneGiftHistoryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> HistoryFixture:
        fixture = HistoryFixture(root)
        # 7 天内：u 送出 2 笔、收到 2 笔；窗外 1 笔；他人 1 笔。
        fixture.add("s1", "u", "a", 100, 90, 10, "2026-01-09 12:00:00")
        fixture.add("s2", "u", "b", 200, 180, 20, "2026-01-08 12:00:00")
        fixture.add("s3", "u", "a", 50, 45, 5, "2026-01-01 12:00:00")
        fixture.add("r1", "c", "u", 300, 270, 30, "2026-01-07 12:00:00")
        fixture.add("r2", "d", "u", 10, 10, 0, "2026-01-11 00:00:00")
        fixture.add("x1", "x", "y", 999, 900, 99, "2026-01-09 12:00:00")
        return fixture

    def test_summary_counts_window_and_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = StoneGiftHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertTrue(summary["ok"])
            self.assertEqual(
                (
                    summary["sent_count"],
                    summary["sent_gross"],
                    summary["sent_fee"],
                    summary["sent_net"],
                ),
                (2, 300, 30, 270),
            )
            self.assertEqual(
                (summary["received_count"], summary["received_net"]), (2, 280)
            )
            self.assertEqual(
                [(row["user_id"], row["amount"]) for row in summary["top_sent"]],
                [("b", 180), ("a", 90)],
            )
            self.assertEqual(
                [(row["user_id"], row["amount"]) for row in summary["top_received"]],
                [("c", 270), ("d", 10)],
            )
            self.assertEqual(
                [row["created_at"] for row in summary["recent"]],
                [
                    "2026-01-11 00:00:00",
                    "2026-01-09 12:00:00",
                    "2026-01-08 12:00:00",
                    "2026-01-07 12:00:00",
                ],
            )
            self.assertEqual(summary["recent"][0]["direction"], "received")
            self.assertEqual(summary["names"]["b"], "丙")

    def test_other_players_do_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            app = StoneGiftHistoryApplication(fixture.database, clock=FixedClock())
            summary = app.summary("x")
            self.assertEqual((summary["sent_count"], summary["sent_gross"]), (1, 999))
            self.assertEqual((summary["received_count"], summary["received_net"]), (0, 0))
            self.assertEqual(
                [row["user_id"] for row in summary["top_sent"]], ["y"]
            )

    def test_missing_table_degrades_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp), migrate=False)
            summary = StoneGiftHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["notice"], EMPTY_TEXT)
            self.assertNotIn("stone_gift_operations", fixture.tables())

    def test_renderer_full_and_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            text = format_stone_gift_history(
                StoneGiftHistoryApplication(fixture.database, clock=FixedClock()).summary("u")
            )
            self.assertIn("近7天赠送流水", text)
            self.assertIn("送出：2 笔，共 300 灵石（手续费 30，实到 270）", text)
            self.assertIn("收到：2 笔，共 280 灵石", text)
            self.assertIn("主要送出：丙 180灵石/1笔、乙 90灵石/1笔", text)
            self.assertIn("最近：", text)
            self.assertIn("收到来自 丁 270灵石", text)
            self.assertEqual(
                format_stone_gift_history({"ok": True, "notice": None, "sent_count": 0, "received_count": 0}),
                EMPTY_TEXT,
            )

    def test_handler_and_manifest_contract(self) -> None:
        source = COMMANDS.read_text(encoding="utf-8")
        self.assertIn('on_command(\n        "我的赠送流水",', source)
        self.assertIn('aliases={"赠送流水", "灵石赠送流水"}', source)
        self.assertIn("async def _stone_history_handler(bot: Bot, event: Event):", source)
        self.assertIn("_build_stone_history", source)
        self.assertIn("context.user_id", source)
        self.assertIn("StoneGiftHistoryApplication", source)
        self.assertIn("我的赠送流水", [spec.name for spec in FEATURE.commands])
        help_text = HELP.read_text(encoding="utf-8")
        self.assertIn("- 我的赠送流水", help_text)
        self.assertIn("近7天送出与收到的灵石、主要对象与最近记录", help_text)


if __name__ == "__main__":
    unittest.main()
