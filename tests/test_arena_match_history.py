"""玩家「我的竞技场战绩」只读聚合与命令合同。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.arena import migrations as arena_migrations
from nonebot_plugin_xiuxian_2.features.arena.manifest import FEATURE
from nonebot_plugin_xiuxian_2.features.arena.match_history import (
    EMPTY_TEXT,
    ArenaMatchHistoryApplication,
    format_arena_match_history,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_arena" / "__init__.py"
HELP = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_base" / "__init__.py"

NOW = datetime(2026, 1, 10, 12, 0, 0)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class MatchFixture:
    def __init__(self, root: Path, *, migrate: bool = True) -> None:
        self.database = root / "game.db"
        if migrate:
            with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
                arena_migrations.apply_arena_settlement(uow)

    def add(
        self,
        operation_id: str,
        challenger: str,
        opponent: str | None,
        outcome: str,
        *,
        status: str = "applied",
        delta: int = 0,
        score: int = 1000,
        rank: str = "青铜",
        challenged_at: str = "2026-01-09 12:00:00",
        created_at: str = "2026-01-09 12:00:00",
    ) -> None:
        payload = json.dumps(
            {
                "challenger_id": challenger,
                "opponent_id": opponent,
                "outcome": outcome,
                "challenge_cap": 10,
                "stamina_cost": 100,
                "challenged_at": challenged_at,
            },
            ensure_ascii=True,
        )
        result = json.dumps(
            {
                "status": status,
                "outcome": outcome,
                "challenger_score": score,
                "challenger_rank": rank,
                "score_delta": delta,
            },
            ensure_ascii=True,
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO arena_challenge_settlement_operations("
                "operation_id,challenger_id,payload,result_json,created_at"
                ") VALUES (?,?,?,?,?)",
                (operation_id, challenger, payload, result, created_at),
            )

    def raw(self, operation_id: str, challenger: str, payload: str, result: str) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO arena_challenge_settlement_operations("
                "operation_id,challenger_id,payload,result_json,created_at"
                ") VALUES (?,?,?,?,?)",
                (operation_id, challenger, payload, result, "2026-01-09 12:00:00"),
            )

    def tables(self) -> set[str]:
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }


def _resolver(mapping: dict[str, str]):
    return lambda user_id: mapping.get(user_id)


class ArenaMatchHistoryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> MatchFixture:
        fixture = MatchFixture(root)
        fixture.add(
            "m1", "u", "a", "win", delta=20, score=1180, rank="白银",
            challenged_at="2026-01-09 12:00:00.123456",
        )
        fixture.add("m2", "u", "b", "loss", score=1180, created_at="2026-01-08 12:00:00")
        fixture.add("m3", "u", "c", "draw", score=1180, created_at="2026-01-07 12:00:00")
        fixture.add(
            "m4", "u", None, "no_match", delta=5, score=1185,
            created_at="2026-01-06 12:00:00",
        )
        fixture.add(
            "m5", "u", "d", "win", status="limit_reached", delta=20,
            created_at="2026-01-09 13:00:00",
        )
        fixture.add("m6", "u", "e", "win", delta=20, created_at="2026-01-01 12:00:00")
        fixture.add("m7", "x", "f", "win", delta=20, created_at="2026-01-09 12:00:00")
        return fixture

    def test_summary_window_aggregation_and_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = ArenaMatchHistoryApplication(
                fixture.database,
                clock=FixedClock(),
                name_resolver=_resolver({"a": "甲", "b": "乙", "c": "丙"}),
            ).summary("u")
            self.assertTrue(summary["ok"])
            self.assertEqual(
                (
                    summary["total"],
                    summary["wins"],
                    summary["losses"],
                    summary["draws"],
                    summary["no_match"],
                    summary["net_delta"],
                ),
                (4, 1, 1, 1, 1, 25),
            )
            self.assertEqual(
                [row["opponent_id"] for row in summary["recent"]],
                ["a", "b", "c", ""],
            )
            self.assertEqual(summary["recent"][0]["challenged_at"], "2026-01-09 12:00:00")
            self.assertEqual(summary["names"], {"a": "甲", "b": "乙", "c": "丙"})

    def test_other_players_do_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            summary = ArenaMatchHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("x")
            self.assertEqual((summary["total"], summary["wins"]), (1, 1))
            self.assertEqual([row["opponent_id"] for row in summary["recent"]], ["f"])

    def test_missing_table_degrades_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = MatchFixture(Path(temp), migrate=False)
            summary = ArenaMatchHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["notice"], EMPTY_TEXT)
            self.assertNotIn("arena_challenge_settlement_operations", fixture.tables())

    def test_malformed_receipts_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = MatchFixture(Path(temp))
            fixture.raw("bad1", "u", "not-json", json.dumps({"status": "applied"}))
            fixture.raw("bad2", "u", json.dumps({"outcome": "win"}), "[]")
            fixture.raw(
                "bad3",
                "u",
                json.dumps({"outcome": "unknown"}),
                json.dumps({"status": "applied"}),
            )
            fixture.add("ok1", "u", "a", "win", delta=20, score=1020)
            summary = ArenaMatchHistoryApplication(
                fixture.database, clock=FixedClock()
            ).summary("u")
            self.assertEqual((summary["total"], summary["wins"]), (1, 1))

    def test_name_resolver_failure_falls_back_to_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = MatchFixture(Path(temp))
            fixture.add("m1", "u", "a", "win", delta=20)

            def broken(_user_id: str):
                raise RuntimeError("profile unavailable")

            summary = ArenaMatchHistoryApplication(
                fixture.database, clock=FixedClock(), name_resolver=broken
            ).summary("u")
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["names"], {})
            self.assertIn("胜 a +20", format_arena_match_history(summary))

    def test_renderer_full_and_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = self._fixture(Path(temp))
            text = format_arena_match_history(
                ArenaMatchHistoryApplication(
                    fixture.database,
                    clock=FixedClock(),
                    name_resolver=_resolver({"a": "甲", "b": "乙", "c": "丙"}),
                ).summary("u")
            )
            self.assertIn("近7天竞技场战绩", text)
            self.assertIn("挑战：4 场（胜 1 / 负 1 / 平 1 / 未匹配 1）", text)
            self.assertIn("积分净变化：+25", text)
            self.assertIn("最近：", text)
            self.assertIn("- 2026-01-09 12:00:00 胜 甲 +20（当前 1180 · 白银）", text)
            self.assertIn("未匹配 无匹配 +5（当前 1185 · 青铜）", text)
            self.assertEqual(
                format_arena_match_history({"ok": True, "total": 0}), EMPTY_TEXT
            )

    def test_handler_manifest_and_help_contract(self) -> None:
        source = ARENA.read_text(encoding="utf-8")
        self.assertIn(
            'on_command(\n    "我的竞技场战绩", aliases={"竞技场战绩", "竞技场记录"}',
            source,
        )
        self.assertIn(
            "async def arena_record_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):",
            source,
        )
        self.assertIn("arena_match_history_application.summary", source)
        self.assertIn("format_arena_match_history", source)
        self.assertIn("我的竞技场战绩", [spec.name for spec in FEATURE.commands])
        self.assertIn("- 我的竞技场战绩", source)
        self.assertIn("查看近7天挑战结果与积分变化", source)
        help_text = HELP.read_text(encoding="utf-8")
        self.assertIn("- 我的竞技场战绩", help_text)
        self.assertIn("近7天挑战胜负与积分变化", help_text)


if __name__ == "__main__":
    unittest.main()
