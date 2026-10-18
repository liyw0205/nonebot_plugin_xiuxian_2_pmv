"""Per-boss-activity rank positions in the overview snapshot.

With multiple configured boss activities, one shared ``board_positions["boss"]``
position let one boss's damage rank match another boss's reward tiers. Each
boss is now judged by its own position (``boss_board_positions``), keyed by the
boss activity's own key — the same key the boss damage tables use. Written as
``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
"""

from __future__ import annotations

from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    _claimable_boss_rank_count,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import (
    ActivityReadModelSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)


NOW = datetime(2026, 10, 15, 12)


def _boss(key: str, rank_max: int = 3) -> dict:
    return {
        "key": key,
        "type": "activity_boss",
        "enabled": True,
        "name": f"首领-{key}",
        "start_time": "0",
        "end_time": "无限",
        "rank_rewards": [
            {"rank_min": 1, "rank_max": rank_max, "name": "前三", "reward": "灵石x20"}
        ],
    }


def _config(bosses: list[dict]) -> dict:
    return {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "daily_tasks": [],
        "weekly_tasks": [],
        "extensions": {"activity_pass": {"enabled": False}},
        "gameplay_activities": bosses,
    }


class BossRankPerActivityTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_activity_state_schema(uow)

    def _seed_damage(self, rows: list[tuple[str, str, int]]) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            for activity_key, user_id, damage in rows:
                uow.execute(
                    "INSERT INTO activity_boss_damage(activity_key,user_id,total_damage,update_time) "
                    "VALUES(?,?,?,'2026-10-15 10:00:00')",
                    (activity_key, user_id, damage),
                )

    def _snapshot(self, bosses: list[dict], user_id: str = "u1") -> dict:
        self._application(bosses)  # ensure config parses; snapshot is direct
        return ActivityReadModelSqlRepository(self.database).overview_snapshot(
            user_id,
            "spring",
            include_tasks=False,
            include_pass=False,
            collect_activity_keys=[],
            board_sources=["boss"],
            boss_activity_keys=[boss["key"] for boss in bosses],
        )

    def _application(self, bosses: list[dict]) -> ActivityReadModelApplication:
        return ActivityReadModelApplication(
            self.database,
            config_loader=lambda: _config(bosses),
            clock=SimpleNamespace(now=lambda: NOW),
        )

    def test_snapshot_carries_a_position_per_boss_activity(self) -> None:
        self._seed_damage([
            ("boss_a", "u1", 500),
            ("boss_b", "other", 900),
        ])
        bosses = [_boss("boss_a"), _boss("boss_b")]
        snapshot = self._snapshot(bosses)
        self.assertEqual(snapshot["boss_board_positions"], {"boss_a": 1, "boss_b": 0})

    def test_each_boss_is_judged_by_its_own_rank(self) -> None:
        """Ranked on A only: B must not become claimable via A's position."""
        self._seed_damage([("boss_a", "u1", 500)])
        bosses = [_boss("boss_a"), _boss("boss_b")]
        snapshot = self._snapshot(bosses)
        config = _config(bosses)
        self.assertEqual(_claimable_boss_rank_count(config, snapshot), 1)
        self.assertEqual(snapshot["boss_board_positions"]["boss_b"], 0)

    def test_both_bosses_claimable_with_independent_ranks(self) -> None:
        self._seed_damage([
            ("boss_a", "u1", 500),
            ("boss_b", "u1", 700),
            ("boss_b", "other", 900),
        ])
        bosses = [_boss("boss_a"), _boss("boss_b")]
        snapshot = self._snapshot(bosses)
        self.assertEqual(
            snapshot["boss_board_positions"], {"boss_a": 1, "boss_b": 2}
        )
        config = _config(bosses)
        self.assertEqual(_claimable_boss_rank_count(config, snapshot), 2)

    def test_missing_per_activity_key_falls_back_to_the_shared_position(self) -> None:
        """Older snapshots carry only board_positions["boss"]; single-boss
        configs keep their previous behavior."""
        bosses = [_boss("boss_a")]
        config = _config(bosses)
        legacy = {
            "board_positions": {"boss": 2},
            "boss_rank_claimed": set(),
        }
        self.assertEqual(_claimable_boss_rank_count(config, legacy), 1)
        empty = {"board_positions": {"boss": 0}, "boss_rank_claimed": set()}
        self.assertEqual(_claimable_boss_rank_count(config, empty), 0)

    def test_claimed_tier_is_not_recounted(self) -> None:
        self._seed_damage([("boss_a", "u1", 500)])
        bosses = [_boss("boss_a")]
        snapshot = self._snapshot(bosses)
        # The claim lives in the per-activity boss_progress entry — the same
        # representation the repository writes.
        snapshot["boss_progress"]["boss_a"]["rank_claimed"] = {"1-3"}
        config = _config(bosses)
        self.assertEqual(_claimable_boss_rank_count(config, snapshot), 0)

    def test_single_boss_single_unit_snapshot_still_feeds_the_center(self) -> None:
        """The center text renders the boss rank entry from one snapshot."""
        self._seed_damage([("boss_a", "u1", 500)])
        bosses = [_boss("boss_a")]
        application = self._application(bosses)
        text = application.activity_center_text("u1")
        self.assertIn("活动首领领奖 排行", text)


if __name__ == "__main__":
    unittest.main()
