"""玩家「我的副本记录」只读聚合与命令合同。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.dungeon import migrations as dungeon_migrations
from nonebot_plugin_xiuxian_2.features.dungeon.explore_history import (
    EMPTY_TEXT,
    DungeonExploreHistoryApplication,
    format_dungeon_explore_history,
)
from nonebot_plugin_xiuxian_2.features.dungeon.manifest import FEATURE
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
DUNGEON = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_dungeon" / "__init__.py"
HELP = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_base" / "__init__.py"

NOW = datetime(2026, 1, 10, 12, 0, 0)


class FixedClock:
    def now(self) -> datetime:
        return NOW


def _identity(user_id: str) -> str:
    return json.dumps(
        {"action": "explore", "user_id": user_id}, ensure_ascii=True, sort_keys=True
    )


class HistoryFixture:
    def __init__(self, root: Path, *, migrate: bool = True) -> None:
        self.database = root / "game.db"
        if migrate:
            with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
                dungeon_migrations.apply_dungeon_explore(uow)

    def add(
        self,
        operation_id: str,
        user_id: str,
        *,
        layer: int = 2,
        dungeon_status: str = "exploring",
        result_status: str = "applied",
        message: str = "探索结果",
        battles: int = 1,
        updated_at: str = "2026-01-09 12:00:00",
    ) -> None:
        response = {
            "message": message,
            "battle_messages": [f"battle-{index}" for index in range(battles)],
        }
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO dungeon_explore_operations("
                "operation_id,request_identity,phase,prepared_json,result_status,"
                "result_json,current_layer,dungeon_status,created_at,updated_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    operation_id,
                    _identity(user_id),
                    "completed",
                    "{}",
                    result_status,
                    json.dumps(response, ensure_ascii=True),
                    layer,
                    dungeon_status,
                    updated_at,
                    updated_at,
                ),
            )

    def raw(
        self,
        operation_id: str,
        identity: str,
        result_json: str,
        *,
        result_status: str = "applied",
        updated_at: str = "2026-01-09 12:00:00",
    ) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO dungeon_explore_operations("
                "operation_id,request_identity,phase,prepared_json,result_status,"
                "result_json,current_layer,dungeon_status,created_at,updated_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    operation_id,
                    identity,
                    "completed",
                    "{}",
                    result_status,
                    result_json,
                    1,
                    "exploring",
                    updated_at,
                    updated_at,
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


class DungeonExploreHistoryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> HistoryFixture:
        fixture = HistoryFixture(root)
        fixture.add(
            "r1",
            "u",
            layer=2,
            dungeon_status="exploring",
            message="获得灵石 100\n继续前进",
            battles=2,
            updated_at="2026-01-09 12:00:00",
        )
        fixture.add(
            "r2",
            "u",
            layer=3,
            dungeon_status="completed",
            message="通关",
            battles=1,
            updated_at="2026-01-08 12:00:00",
        )
        fixture.add(
            "r3",
            "u",
            result_status="state_changed",
            message="状态冲突",
            updated_at="2026-01-09 13:00:00",
        )
        fixture.add("r4", "x", layer=9, updated_at="2026-01-09 12:00:00")
        fixture.add("r5", "u", layer=1, updated_at="2026-01-01 12:00:00")
        return fixture

    def test_summary_window_aggregation_and_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = DungeonExploreHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertTrue(summary["ok"])
            self.assertEqual(
                (
                    summary["total"],
                    summary["cleared"],
                    summary["best_layer"],
                    summary["battles"],
                ),
                (2, 1, 3, 3),
            )
            self.assertEqual(
                [row["current_layer"] for row in summary["recent"]], [2, 3]
            )
            self.assertEqual(
                summary["recent"][0]["message"], "获得灵石 100\n继续前进"
            )

    def test_other_players_do_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = DungeonExploreHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("x")
            self.assertEqual((summary["total"], summary["best_layer"]), (1, 9))

    def test_missing_table_degrades_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp), migrate=False)
            summary = DungeonExploreHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["notice"], EMPTY_TEXT)
            self.assertNotIn("dungeon_explore_operations", fixture.tables())

    def test_malformed_and_wrong_identity_rows_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = HistoryFixture(Path(temp))
            fixture.raw("bad1", _identity("u"), "not-json")
            fixture.raw("bad2", _identity("u"), "[]")
            fixture.raw("bad3", _identity("other"), json.dumps({"message": "x"}))
            fixture.add("ok1", "u", updated_at="2026-01-09 12:00:00")
            summary = DungeonExploreHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertEqual((summary["total"], summary["best_layer"]), (1, 2))

    def test_renderer_full_and_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            text = format_dungeon_explore_history(
                DungeonExploreHistoryApplication(
                    fixture.database, clock=FixedClock()
                ).summary("u")
            )
            self.assertIn("近7天副本探索记录", text)
            self.assertIn("探索：2 次（通关 1）", text)
            self.assertIn("最高层：3 层 · 战斗 3 场", text)
            self.assertIn("最近：", text)
            self.assertIn("第2层 探索中：获得灵石 100 / 继续前进", text)
            self.assertIn("第3层 通关：通关", text)
            self.assertEqual(
                format_dungeon_explore_history({"ok": True, "total": 0}), EMPTY_TEXT
            )

    def test_handler_manifest_and_help_contract(self) -> None:
        source = DUNGEON.read_text(encoding="utf-8")
        self.assertIn(
            'on_command(\n    "我的副本记录", aliases={"副本记录", "副本探索记录"}',
            source,
        )
        self.assertIn("dungeon_explore_history_application.summary", source)
        self.assertIn("format_dungeon_explore_history", source)
        self.assertIn("我的副本记录", [spec.name for spec in FEATURE.commands])
        self.assertIn("- 我的副本记录", source)
        self.assertIn("查看近7天探索结果与奖励摘要", source)
        help_text = HELP.read_text(encoding="utf-8")
        self.assertIn("- 我的副本记录", help_text)
        self.assertIn("近7天副本探索层数与结果摘要", help_text)


if __name__ == "__main__":
    unittest.main()
