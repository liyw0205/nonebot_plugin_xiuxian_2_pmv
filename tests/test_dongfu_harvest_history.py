"""玩家「我的洞府收获记录」只读聚合与命令合同。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.dongfu import migrations as dongfu_migrations
from nonebot_plugin_xiuxian_2.features.dongfu.harvest_history import (
    EMPTY_TEXT,
    DongfuHarvestHistoryApplication,
    format_dongfu_harvest_history,
)
from nonebot_plugin_xiuxian_2.features.dongfu.manifest import FEATURE
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
DONGFU = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_dongfu" / "__init__.py"
HELP = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_base" / "__init__.py"

NOW = datetime(2026, 1, 10, 12, 0, 0)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class HistoryFixture:
    def __init__(self, root: Path, *, migrate: bool = True) -> None:
        self.database = root / "game.db"
        if migrate:
            with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
                dongfu_migrations.apply_dongfu_operations(uow)

    def add(
        self,
        operation_id: str,
        user_id: str,
        items: list[list[object]],
        *,
        failed_slots: list[str] | None = None,
        created_at: str = "2026-01-09 12:00:00",
    ) -> None:
        payload = [
            user_id,
            ["slot-1"],
            [1],
            items,
            failed_slots or [],
        ]
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO dongfu_harvest_operations("
                "operation_id,payload,rewards,created_at) VALUES (?,?,?,?)",
                (
                    operation_id,
                    json.dumps(payload, ensure_ascii=False),
                    json.dumps([[item[0], item[3]] for item in items]),
                    created_at,
                ),
            )

    def raw(self, operation_id: str, payload: str, created_at: str) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO dongfu_harvest_operations("
                "operation_id,payload,rewards,created_at) VALUES (?,?,?,?)",
                (operation_id, payload, "[]", created_at),
            )

    def tables(self) -> set[str]:
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }


class DongfuHarvestHistoryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> HistoryFixture:
        fixture = HistoryFixture(root)
        fixture.add(
            "h1",
            "u",
            [[1, "灵草", "草药", 2], [2, "灵石袋", "道具", 1]],
            failed_slots=["slot-2"],
            created_at="2026-01-09 12:00:00",
        )
        fixture.add(
            "h2",
            "u",
            [[3, "灵果", "果实", 3]],
            created_at="2026-01-08 12:00:00",
        )
        fixture.add("h3", "u", [[1, "灵草", "草药", 9]], created_at="2026-01-01 12:00:00")
        fixture.add("h4", "x", [[4, "灵木", "材料", 1]], created_at="2026-01-09 12:00:00")
        return fixture

    def test_summary_window_aggregation_and_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = DongfuHarvestHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertTrue(summary["ok"])
            self.assertEqual(
                (
                    summary["total"],
                    summary["total_items"],
                    summary["distinct_items"],
                ),
                (2, 6, 3),
            )
            self.assertEqual(
                [row["created_at"] for row in summary["recent"]],
                ["2026-01-09 12:00:00", "2026-01-08 12:00:00"],
            )
            self.assertEqual(summary["recent"][0]["failed_slots"], 1)

    def test_other_players_do_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = DongfuHarvestHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("x")
            self.assertEqual((summary["total"], summary["total_items"]), (1, 1))

    def test_missing_table_degrades_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp), migrate=False)
            summary = DongfuHarvestHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["notice"], EMPTY_TEXT)
            self.assertNotIn("dongfu_harvest_operations", fixture.tables())

    def test_malformed_and_legacy_payloads_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp))
            fixture.raw("bad1", "not-json", "2026-01-09 12:00:00")
            fixture.raw(
                "bad2",
                json.dumps(["u", ["slot-1"]]),
                "2026-01-09 12:00:00",
            )
            fixture.raw(
                "bad3",
                json.dumps(["u", [], [], [], []]),
                "2026-01-09 12:00:00",
            )
            fixture.add("ok1", "u", [[1, "灵草", "草药", 1]])
            summary = DongfuHarvestHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertEqual((summary["total"], summary["total_items"]), (1, 1))

    def test_renderer_full_and_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            text = format_dongfu_harvest_history(
                DongfuHarvestHistoryApplication(
                    fixture.database, clock=FixedClock()
                ).summary("u")
            )
            self.assertIn("近7天洞府收获记录", text)
            self.assertIn("收获：2 次（共 6 件，3 种物品）", text)
            self.assertIn("最近：", text)
            self.assertIn("灵草×2、灵石袋×1（1 块未成熟）", text)
            self.assertIn("灵果×3", text)
            self.assertEqual(
                format_dongfu_harvest_history({"ok": True, "total": 0}), EMPTY_TEXT
            )

    def test_handler_manifest_and_help_contract(self) -> None:
        source = DONGFU.read_text(encoding="utf-8")
        self.assertIn(
            'on_command(\n    "我的洞府收获记录", aliases={"洞府收获记录", "灵田收获记录"}',
            source,
        )
        self.assertIn("dongfu_harvest_history_application.summary", source)
        self.assertIn("format_dongfu_harvest_history", source)
        self.assertIn("我的洞府收获记录", [spec.name for spec in FEATURE.commands])
        self.assertIn("- 我的洞府收获记录", source)
        self.assertIn("查看近7天收获物品", source)
        help_text = HELP.read_text(encoding="utf-8")
        self.assertIn("- 我的洞府收获记录", help_text)
        self.assertIn("近7天收获物品与数量", help_text)


if __name__ == "__main__":
    unittest.main()
