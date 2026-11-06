"""玩家「我的通天塔记录」只读聚合与命令合同。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.tower import migrations as tower_migrations
from nonebot_plugin_xiuxian_2.features.tower.history import (
    EMPTY_TEXT,
    TowerHistoryApplication,
    format_tower_history,
)
from nonebot_plugin_xiuxian_2.features.tower.manifest import FEATURE
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
TOWER = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_tower" / "__init__.py"
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
                tower_migrations.apply_tower_settlement(uow)

    def add(
        self,
        operation_id: str,
        user_id: str,
        floor: int,
        *,
        success: bool = True,
        score: int = 100,
        stone: int = 50,
        exp: int = 80,
        stamina: int = 10,
        rewards: list[list[object]] | None = None,
        created_at: str = "2026-01-09 12:00:00",
    ) -> None:
        payload = json.dumps([user_id, floor, stamina], ensure_ascii=True)
        result = json.dumps(
            {
                "score": score,
                "stone": stone,
                "exp": exp,
                "floor": floor,
                "stamina_cost": stamina,
                "challenge_succeeded": success,
                "rewards": rewards or [],
            },
            ensure_ascii=True,
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO tower_settlement_operations("
                "operation_id,payload,result_json,created_at) VALUES (?,?,?,?)",
                (operation_id, payload, result, created_at),
            )

    def raw(self, operation_id: str, payload: str, result: str, created_at: str) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO tower_settlement_operations("
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


class TowerHistoryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> HistoryFixture:
        fixture = HistoryFixture(root)
        fixture.add(
            "r1",
            "u",
            5,
            score=100,
            stone=50,
            exp=80,
            rewards=[[1, "灵草", "草药", 2]],
            created_at="2026-01-09 12:00:00",
        )
        fixture.add(
            "r2",
            "u",
            6,
            success=False,
            score=0,
            stone=0,
            exp=0,
            created_at="2026-01-08 12:00:00",
        )
        fixture.add("r3", "u", 0, score=0, stone=0, exp=0, created_at="2026-01-09 13:00:00")
        fixture.add("r4", "x", 9, created_at="2026-01-09 12:00:00")
        fixture.add("r5", "u", 3, created_at="2026-01-01 12:00:00")
        return fixture

    def test_summary_window_aggregation_and_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = TowerHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertTrue(summary["ok"])
            self.assertEqual(
                (
                    summary["total"],
                    summary["successes"],
                    summary["failures"],
                    summary["best_floor"],
                    summary["total_score"],
                    summary["total_stone"],
                    summary["total_exp"],
                ),
                (2, 1, 1, 6, 100, 50, 80),
            )
            self.assertEqual(
                [row["floor"] for row in summary["recent"]], [5, 6]
            )
            self.assertEqual(summary["recent"][0]["rewards"][0]["name"], "灵草")

    def test_other_players_do_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = TowerHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("x")
            self.assertEqual((summary["total"], summary["best_floor"]), (1, 9))

    def test_missing_table_degrades_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp), migrate=False)
            summary = TowerHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["notice"], EMPTY_TEXT)
            self.assertNotIn("tower_settlement_operations", fixture.tables())

    def test_malformed_and_rejected_rows_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp))
            fixture.raw("bad1", "not-json", json.dumps({"score": 100}), "2026-01-09 12:00:00")
            fixture.raw("bad2", json.dumps(["u", 4, 10]), "[]", "2026-01-09 12:00:00")
            fixture.raw(
                "bad3",
                json.dumps(["u", 0, 10]),
                json.dumps({"score": 0, "challenge_succeeded": True}),
                "2026-01-09 12:00:00",
            )
            fixture.add("ok1", "u", 4, created_at="2026-01-09 12:00:00")
            summary = TowerHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertEqual((summary["total"], summary["best_floor"]), (1, 4))

    def test_renderer_full_and_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            text = format_tower_history(
                TowerHistoryApplication(fixture.database, clock=FixedClock()).summary("u")
            )
            self.assertIn("近7天通天塔记录", text)
            self.assertIn("挑战：2 次（成功 1 / 失败 1）", text)
            self.assertIn("最高层：6 层", text)
            self.assertIn("累计：积分 100 · 灵石 50 · 修为 80", text)
            self.assertIn("最近：", text)
            self.assertIn("第5层 成功 +100积分，奖励 灵草×2", text)
            self.assertIn("第6层 失败 消耗体力 10", text)
            self.assertEqual(
                format_tower_history({"ok": True, "total": 0}), EMPTY_TEXT
            )

    def test_handler_manifest_and_help_contract(self) -> None:
        source = TOWER.read_text(encoding="utf-8")
        self.assertIn(
            'on_command(\n    "我的通天塔记录", aliases={"通天塔记录", "爬塔记录"}',
            source,
        )
        self.assertIn("tower_history_application.summary", source)
        self.assertIn("format_tower_history", source)
        self.assertIn("我的通天塔记录", [spec.name for spec in FEATURE.commands])
        self.assertIn("- 我的通天塔记录", source)
        self.assertIn("查看近7天爬塔结果与奖励", source)
        help_text = HELP.read_text(encoding="utf-8")
        self.assertIn("- 我的通天塔记录", help_text)
        self.assertIn("近7天爬塔胜负、层数与奖励", help_text)


if __name__ == "__main__":
    unittest.main()
