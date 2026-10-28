from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.arena.honor_settlement_view import (
    render_honor_settlements,
)
from nonebot_plugin_xiuxian_2.features.arena.migrations import (
    apply_arena_daily_reward_player,
    apply_arena_season_reward,
)
from nonebot_plugin_xiuxian_2.features.arena.season_reward_application import (
    ArenaSeasonRewardApplication,
)
from nonebot_plugin_xiuxian_2.features.arena.season_reward_repository import (
    ArenaHonorSettlement,
    ArenaSeasonRewardRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


def _payload(
    user_id: str,
    season_key: str,
    *,
    base: int,
    bonus: int,
    rank: str = "黄金",
    position: int = 3,
) -> str:
    return json.dumps(
        [
            user_id,
            season_key,
            2000,
            rank,
            position,
            100,
            200,
            base,
            bonus,
            [],
            99,
            None,
        ],
        ensure_ascii=True,
        sort_keys=True,
    )


class ArenaHonorSettlementHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_arena_season_reward(uow)
        self.repository = ArenaSeasonRewardRepository(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def insert(
        self,
        season_key: str,
        *,
        user_id: str = "u1",
        honor: int = 600,
        honor_points: int = 1600,
        earned: int = 2600,
        payload: str | None = None,
    ) -> None:
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute(
                "INSERT INTO arena_season_reward_operations("
                "operation_id,payload,season_key,user_id,honor,honor_points,total_honor_earned"
                ") VALUES(?,?,?,?,?,?,?)",
                (
                    f"{user_id}:{season_key}",
                    payload if payload is not None else _payload(user_id, season_key, base=300, bonus=300),
                    season_key,
                    user_id,
                    honor,
                    honor_points,
                    earned,
                ),
            )

    def test_recent_settlements_are_newest_first_and_limited(self) -> None:
        self.insert("2026-10-27")
        self.insert("2026-10-29")
        self.insert("2026-10-28")
        self.insert("2026-10-29", user_id="u2")

        rows = self.repository.recent_settlements("u1", limit=2)

        self.assertEqual([row.season_key for row in rows], ["2026-10-29", "2026-10-28"])
        self.assertEqual((rows[0].total_honor, rows[0].honor_after), (600, 1600))
        self.assertEqual((), self.repository.recent_settlements("u1", limit=0))
        self.assertEqual(rows, self.repository.recent_settlements("u1", limit=2))

    def test_claim_persists_history_read_back_by_repository(self) -> None:
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            uow.execute("INSERT INTO user_xiuxian(user_id) VALUES('u1')")
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            apply_arena_daily_reward_player(uow)
            uow.execute(
                "INSERT INTO arena(user_id,score,rank,honor_points,total_honor_earned,"
                "daily_challenges_used,daily_extra_challenges,daily_challenge_buys,"
                "last_reset_date,last_buy_date) VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("u1", 2000, "黄金", 10, 20, 3, 1, 1, "old", "old"),
            )
        application = ArenaSeasonRewardApplication(self.game, self.player)

        result = application.claim(
            "claim-history",
            "u1",
            "2026-10-29",
            2000,
            "黄金",
            1,
            10,
            20,
            300,
            500,
            (),
            99,
            expected_reset={
                "daily_challenges_used": 3,
                "daily_extra_challenges": 1,
                "daily_challenge_buys": 1,
                "last_reset_date": "old",
                "last_buy_date": "old",
            },
        )

        rows = self.repository.recent_settlements("u1")
        self.assertEqual(result.status, "applied")
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].details_available)
        self.assertEqual(
            (
                rows[0].season_key,
                rows[0].base_honor,
                rows[0].ranking_bonus,
                rows[0].total_honor,
                rows[0].honor_after,
                rows[0].total_honor_earned_after,
                rows[0].rank,
                rows[0].position,
            ),
            ("2026-10-29", 300, 500, 800, 810, 820, "黄金", 1),
        )

    def test_settlement_payload_details_are_parsed(self) -> None:
        self.insert(
            "2026-10-29",
            honor=800,
            payload=_payload("u1", "2026-10-29", base=300, bonus=500, position=1),
        )

        row = self.repository.recent_settlements("u1")[0]

        self.assertTrue(row.details_available)
        self.assertEqual((row.base_honor, row.ranking_bonus), (300, 500))
        self.assertEqual((row.rank, row.position), ("黄金", 1))
        self.assertEqual((row.total_honor_earned_after, row.total_honor), (2600, 800))

    def test_bad_or_mismatched_payload_keeps_total_only(self) -> None:
        self.insert("2026-10-28", payload="not-json")
        self.insert("2026-10-29", payload=_payload("u1", "2026-10-29", base=100, bonus=100))

        bad, mismatched = self.repository.recent_settlements("u1")

        self.assertFalse(bad.details_available)
        self.assertEqual((bad.total_honor, bad.honor_after), (600, 1600))
        self.assertFalse(mismatched.details_available)
        self.assertEqual((mismatched.base_honor, mismatched.ranking_bonus), (0, 0))

    def test_missing_table_returns_empty_without_ddl(self) -> None:
        empty = Path(self.temp.name) / "empty.db"
        with DatabaseUnitOfWork(empty, immediate=True) as uow:
            uow.execute("CREATE TABLE filler (id INTEGER)")
        repository = ArenaSeasonRewardRepository(empty, self.player)

        self.assertEqual((), repository.recent_settlements("u1"))
        with DatabaseUnitOfWork(empty) as uow:
            self.assertIsNone(
                uow.query_one(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='arena_season_reward_operations'"
                )
            )

    def test_missing_database_file_is_fail_closed(self) -> None:
        missing = Path(self.temp.name) / "missing.db"
        repository = ArenaSeasonRewardRepository(missing, self.player)

        self.assertEqual((), repository.recent_settlements("u1"))
        self.assertFalse(missing.exists())

    def test_application_delegates_recent_settlements(self) -> None:
        calls = []

        class FakeRepository:
            def recent_settlements(self, user_id, limit=5):
                calls.append((user_id, limit))
                return ("sentinel",)

        application = ArenaSeasonRewardApplication(
            self.game, self.player, repository=FakeRepository()
        )

        self.assertEqual(("sentinel",), application.recent_settlements("u1", limit=3))
        self.assertEqual([("u1", 3)], calls)

    def test_empty_renderer_shows_auto_settlement_note(self) -> None:
        text = render_honor_settlements(())

        self.assertIn("【最近荣誉结算】", text)
        self.assertIn("暂无荣誉结算记录", text)

    def test_renderer_formats_details_and_legacy_rows(self) -> None:
        detailed = ArenaHonorSettlement(
            "2026-10-29", 600, 1600, 2600, 300, 300, "黄金", 3, True
        )
        legacy = ArenaHonorSettlement("2026-10-28", 100, 1100, 1200, 0, 0, "", 0, False)

        text = render_honor_settlements((detailed, legacy), limit=5)

        self.assertIn("2026-10-29：基础 300 + 排名 300 = 本次 600", text)
        self.assertIn("黄金第3名", text)
        self.assertIn("2026-10-28：本次 +100 荣誉", text)
        self.assertIn("明细不可用", text)
        self.assertIn("最多显示最近 5 次", text)

    def test_honor_handler_keeps_estimate_and_appends_history(self) -> None:
        source_path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_arena"
            / "__init__.py"
        )
        source = source_path.read_text(encoding="utf-8")
        start = source.index("async def arena_honor_")
        block = source[start : source.index("async def arena_buy_challenge_", start)]

        self.assertIn("recent_settlements(user_id)", block)
        self.assertIn("render_honor_settlements(settlements)", block)
        self.assertIn("明日预计奖励", block)


if __name__ == "__main__":
    unittest.main()
