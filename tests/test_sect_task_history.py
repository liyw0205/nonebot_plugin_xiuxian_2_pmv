"""玩家「我的宗门任务记录」只读聚合与命令合同。"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.sect import migrations as sect_migrations
from nonebot_plugin_xiuxian_2.features.sect.manifest import FEATURE
from nonebot_plugin_xiuxian_2.features.sect.task_history import (
    EMPTY_TEXT,
    SectTaskHistoryApplication,
    format_sect_task_history,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
SECT = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_sect" / "__init__.py"
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
                sect_migrations.apply_sect_task_settlement_operations(uow)

    def add(
        self,
        operation_id: str,
        user_id: str,
        *,
        period: str = "2026-01-05",
        cost_type: str = "hp",
        cost: int = 100,
        exp_reward: int = 500,
        sect_reward: int = 20,
        materials_reward: int = 200,
        created_at: str = "2026-01-09 12:00:00",
    ) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO sect_task_settlement_operations("
                "operation_id,user_id,sect_id,period,cost_type,cost,"
                "exp_reward,sect_reward,materials_reward,created_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    operation_id,
                    user_id,
                    1,
                    period,
                    cost_type,
                    cost,
                    exp_reward,
                    sect_reward,
                    materials_reward,
                    created_at,
                ),
            )

    def tables(self) -> set[str]:
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }


class SectTaskHistoryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> HistoryFixture:
        fixture = HistoryFixture(root)
        fixture.add("r1", "u", created_at="2026-01-09 12:00:00")
        fixture.add(
            "r2",
            "u",
            cost_type="stone",
            cost=50,
            exp_reward=300,
            sect_reward=10,
            materials_reward=100,
            created_at="2026-01-08 12:00:00",
        )
        fixture.add("r3", "u", created_at="2026-01-01 12:00:00")
        fixture.add("r4", "x", created_at="2026-01-09 12:00:00")
        return fixture

    def test_summary_window_aggregation_and_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = SectTaskHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertTrue(summary["ok"])
            self.assertEqual(
                (
                    summary["total"],
                    summary["total_exp"],
                    summary["total_sect"],
                    summary["total_materials"],
                ),
                (2, 800, 30, 300),
            )
            self.assertEqual(
                [row["cost_type"] for row in summary["recent"]], ["hp", "stone"]
            )
            self.assertEqual(summary["recent"][0]["created_at"], "2026-01-09 12:00:00")

    def test_other_players_do_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = SectTaskHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("x")
            self.assertEqual((summary["total"], summary["total_exp"]), (1, 500))

    def test_missing_table_degrades_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp), migrate=False)
            summary = SectTaskHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["notice"], EMPTY_TEXT)
            self.assertNotIn("sect_task_settlement_operations", fixture.tables())

    def test_renderer_full_and_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            text = format_sect_task_history(
                SectTaskHistoryApplication(fixture.database, clock=FixedClock()).summary("u")
            )
            self.assertIn("近7天宗门任务记录", text)
            self.assertIn("完成：2 次", text)
            self.assertIn("累计奖励：修为 800 · 宗门贡献 30 · 宗门资材 300", text)
            self.assertIn("最近：", text)
            self.assertIn("消耗气血 100 → 修为 500 · 贡献 20 · 资材 200", text)
            self.assertIn("消耗灵石 50 → 修为 300 · 贡献 10 · 资材 100", text)
            self.assertEqual(
                format_sect_task_history({"ok": True, "total": 0}), EMPTY_TEXT
            )

    def test_handler_manifest_and_help_contract(self) -> None:
        source = SECT.read_text(encoding="utf-8")
        self.assertIn(
            'on_command(\n    "我的宗门任务记录", aliases={"宗门任务记录"}', source
        )
        self.assertIn("sect_task_history_application.summary", source)
        self.assertIn("format_sect_task_history", source)
        self.assertIn("我的宗门任务记录", [spec.name for spec in FEATURE.commands])
        self.assertIn("- 我的宗门任务记录", source)
        self.assertIn("查看近7天任务结算与奖励", source)
        help_text = HELP.read_text(encoding="utf-8")
        self.assertIn("- 我的宗门任务记录", help_text)
        self.assertIn("近7天宗门任务消耗与奖励", help_text)


if __name__ == "__main__":
    unittest.main()
