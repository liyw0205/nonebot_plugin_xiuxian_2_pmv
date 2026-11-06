"""玩家「我的历练记录」只读聚合与命令合同。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.training import migrations as training_migrations
from nonebot_plugin_xiuxian_2.features.training.event_history import (
    EMPTY_TEXT,
    TrainingEventHistoryApplication,
    format_training_event_history,
)
from nonebot_plugin_xiuxian_2.features.training.manifest import FEATURE
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
TRAINING = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_training" / "__init__.py"
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
                training_migrations.apply_training_event_operations(uow)

    def add(
        self,
        operation_id: str,
        user_id: str,
        *,
        stone: int = 10,
        exp: int = 20,
        hp: int = -5,
        rewards: list[list[object]] | None = None,
        message: str = "历练事件",
        status: str = "applied",
        created_at: str = "2026-01-09 12:00:00",
    ) -> None:
        payload = [
            user_id,
            {},
            {"progress": 1, "completed": 0, "max_progress": 12, "last_event": message},
            {"stone": 0, "exp": 0, "hp": 0, "mp": 0},
            stone,
            exp,
            hp,
            rewards or [],
            999,
        ]
        result = {"status": status, "message": message}
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO training_event_operations("
                "operation_id,payload,result_json,created_at) VALUES (?,?,?,?)",
                (
                    operation_id,
                    json.dumps(payload, ensure_ascii=False),
                    json.dumps(result, ensure_ascii=False),
                    created_at,
                ),
            )

    def raw(
        self,
        operation_id: str,
        payload: str,
        result: str,
        created_at: str = "2026-01-09 12:00:00",
    ) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO training_event_operations("
                "operation_id,payload,result_json,created_at) VALUES (?,?,?,?)",
                (operation_id, payload, result, created_at),
            )

    def tables(self) -> set[str]:
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }


class TrainingEventHistoryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> HistoryFixture:
        fixture = HistoryFixture(root)
        fixture.add(
            "t1",
            "u",
            stone=10,
            exp=20,
            hp=-5,
            rewards=[[1, "灵草", "草药", 2]],
            message="遇到灵草\n获得奖励",
            created_at="2026-01-09 12:00:00",
        )
        fixture.add(
            "t2",
            "u",
            stone=-3,
            exp=5,
            hp=0,
            message="遭遇战",
            created_at="2026-01-08 12:00:00",
        )
        fixture.add(
            "t3",
            "u",
            status="operation_conflict",
            message="冲突",
            created_at="2026-01-09 13:00:00",
        )
        fixture.add("t4", "u", created_at="2026-01-01 12:00:00")
        fixture.add("t5", "x", created_at="2026-01-09 12:00:00")
        return fixture

    def test_summary_window_aggregation_and_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = TrainingEventHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertTrue(summary["ok"])
            self.assertEqual(
                (
                    summary["total"],
                    summary["total_stone"],
                    summary["total_exp"],
                    summary["total_hp"],
                    summary["total_items"],
                ),
                (2, 7, 25, -5, 2),
            )
            self.assertEqual(
                [row["message"] for row in summary["recent"]],
                ["遇到灵草\n获得奖励", "遭遇战"],
            )

    def test_other_players_do_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = TrainingEventHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("x")
            self.assertEqual((summary["total"], summary["total_items"]), (1, 0))

    def test_missing_table_degrades_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp), migrate=False)
            summary = TrainingEventHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["notice"], EMPTY_TEXT)
            self.assertNotIn("training_event_operations", fixture.tables())

    def test_malformed_and_non_applied_rows_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp))
            fixture.raw("bad1", "not-json", json.dumps({"status": "applied"}))
            fixture.raw(
                "bad2",
                json.dumps(["u", {}, {}]),
                json.dumps({"status": "applied", "message": "short"}),
            )
            fixture.raw(
                "bad3",
                json.dumps(["u", {}, {}, {}, 0, 0, 0, [], 999]),
                json.dumps({"status": "operation_conflict", "message": "conflict"}),
            )
            fixture.add("ok1", "u")
            summary = TrainingEventHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertEqual((summary["total"], summary["total_exp"]), (1, 20))

    def test_renderer_full_and_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            text = format_training_event_history(
                TrainingEventHistoryApplication(
                    fixture.database, clock=FixedClock()
                ).summary("u")
            )
            self.assertIn("近7天历练记录", text)
            self.assertIn("事件：2 次", text)
            self.assertIn("累计变化：灵石 +7 · 修为 +25 · 气血 -5", text)
            self.assertIn("最近：", text)
            self.assertIn("遇到灵草 / 获得奖励", text)
            self.assertIn("（灵石 +10 · 修为 +20 · 气血 -5；奖励 灵草×2）", text)
            self.assertIn("（灵石 -3 · 修为 +5 · 气血 0）", text)
            self.assertEqual(
                format_training_event_history({"ok": True, "total": 0}), EMPTY_TEXT
            )

    def test_handler_manifest_and_help_contract(self) -> None:
        source = TRAINING.read_text(encoding="utf-8")
        self.assertIn(
            'on_command(\n    "我的历练记录", aliases={"历练记录"}', source
        )
        self.assertIn("training_event_history_application.summary", source)
        self.assertIn("format_training_event_history", source)
        self.assertIn("我的历练记录", [spec.name for spec in FEATURE.commands])
        self.assertIn("- 我的历练记录", source)
        self.assertIn("查看近7天事件、资源变化与奖励", source)
        help_text = HELP.read_text(encoding="utf-8")
        self.assertIn("- 我的历练记录", help_text)
        self.assertIn("近7天事件、资源变化与奖励", help_text)


if __name__ == "__main__":
    unittest.main()
