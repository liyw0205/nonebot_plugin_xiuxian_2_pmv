"""Per-boss-activity claim scoping in the overview snapshot.

Claim receipts are keyed by ``(activity_key, user_id, key)``, so each boss's
unlocked/claimed sets are independent. A shared union let one boss's claim
suppress another boss's milestone and one boss's unlock advertise another
boss's configured-but-unlocked milestone. Written as ``unittest.TestCase``
because the CI quality job collects tests with ``python -m unittest discover``.
"""

from __future__ import annotations

import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    _boss_claim_state_by_activity,
    _claimable_boss_milestone_count,
    _claimable_boss_rank_count,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)


def _boss(key: str, milestone: bool = True, rank_max: int = 3) -> dict:
    activity = {
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
    if milestone:
        activity["server_milestones"] = [
            {"key": "m1", "hp_percent": 30, "name": "进度一", "reward": "灵石x10"}
        ]
    return activity


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


def _snapshot(bosses: dict[str, dict]) -> dict:
    """bosses: activity_key -> boss_progress entry."""
    return {
        "boss_progress": bosses,
        "boss_board_positions": {key: 1 for key in bosses},
    }


class BossClaimScopingTests(unittest.TestCase):
    def test_a_claimed_milestone_does_not_suppress_b(self) -> None:
        """A's m1 claimed, B's m1 unlocked: B stays claimable."""
        bosses = [_boss("boss_a"), _boss("boss_b")]
        config = _config(bosses)
        snapshot = _snapshot({
            "boss_a": {
                "milestones_unlocked": {"m1"},
                "milestones_claimed": {"m1"},
                "rank_claimed": set(),
            },
            "boss_b": {
                "milestones_unlocked": {"m1"},
                "milestones_claimed": set(),
                "rank_claimed": set(),
            },
        })
        self.assertEqual(_claimable_boss_milestone_count(config, snapshot), 1)

    def test_a_unlocked_milestone_does_not_adopt_b(self) -> None:
        """A's m1 unlocked, B's m1 NOT unlocked: B must not be advertised."""
        bosses = [_boss("boss_a"), _boss("boss_b")]
        config = _config(bosses)
        snapshot = _snapshot({
            "boss_a": {
                "milestones_unlocked": {"m1"},
                "milestones_claimed": set(),
                "rank_claimed": set(),
            },
            "boss_b": {
                "milestones_unlocked": set(),
                "milestones_claimed": set(),
                "rank_claimed": set(),
            },
        })
        self.assertEqual(_claimable_boss_milestone_count(config, snapshot), 1)

    def test_a_claimed_rank_tier_does_not_suppress_b(self) -> None:
        bosses = [_boss("boss_a", milestone=False), _boss("boss_b", milestone=False)]
        config = _config(bosses)
        snapshot = _snapshot({
            "boss_a": {
                "milestones_unlocked": set(),
                "milestones_claimed": set(),
                "rank_claimed": {"1-3"},
            },
            "boss_b": {
                "milestones_unlocked": set(),
                "milestones_claimed": set(),
                "rank_claimed": set(),
            },
        })
        self.assertEqual(_claimable_boss_rank_count(config, snapshot), 1)

    def test_flat_snapshot_keeps_the_legacy_union_semantics(self) -> None:
        """Old flat snapshots fall back to the shared projection."""
        bosses = [_boss("boss_a")]
        config = _config(bosses)
        snapshot = {
            "boss_milestone_unlocked": {"m1"},
            "boss_milestone_claimed": set(),
            "boss_rank_claimed": set(),
        }
        self.assertEqual(_claimable_boss_milestone_count(config, snapshot), 1)
        self.assertEqual(_claimable_boss_rank_count(config, snapshot), 0)
        # No boss_progress at all and no flat keys: nothing claimable.
        self.assertEqual(_claimable_boss_milestone_count(config, {}), 0)

    def test_state_helper_scopes_entries_by_activity_key(self) -> None:
        snapshot = _snapshot({
            "boss_a": {
                "milestones_unlocked": {"m1"},
                "milestones_claimed": {"m1"},
                "rank_claimed": {"1-3"},
            },
            "boss_b": {
                "milestones_unlocked": {"m1"},
                "milestones_claimed": set(),
                "rank_claimed": set(),
            },
        })
        states = _boss_claim_state_by_activity(snapshot)
        self.assertEqual(states["boss_a"][1], {"m1"})
        self.assertEqual(states["boss_b"][1], set())
        self.assertEqual(states["boss_b"][0], {"m1"})
        flat = _boss_claim_state_by_activity({"boss_milestone_unlocked": {"m1"}})
        self.assertEqual(flat, {"": ({"m1"}, set(), set())})

    def test_reward_catalog_marks_claims_per_boss(self) -> None:
        """The reward catalog shows B's milestone as 可领取 while A's is 已领取."""
        from datetime import datetime
        from pathlib import Path
        from types import SimpleNamespace
        import tempfile

        from nonebot_plugin_xiuxian_2.features.activity.migrations import (
            apply_activity_state_schema,
        )
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_activity_state_schema(uow)
        bosses = [_boss("boss_a"), _boss("boss_b")]
        application = ActivityReadModelApplication(
            database,
            config_loader=lambda: _config(bosses),
            clock=SimpleNamespace(now=lambda: datetime(2026, 10, 15, 12)),
        )
        snapshot = _snapshot({
            "boss_a": {
                "milestones_unlocked": {"m1"},
                "milestones_claimed": {"m1"},
                "rank_claimed": set(),
            },
            "boss_b": {
                "milestones_unlocked": {"m1"},
                "milestones_claimed": set(),
                "rank_claimed": set(),
            },
        })
        application.repository.overview_snapshot = lambda *args, **kwargs: snapshot
        text = application.reward_catalog_text("u1")
        a_section = text.split("【首领-boss_a · 进度奖励】")[1].split("【")[0]
        b_section = text.split("【首领-boss_b · 进度奖励】")[1].split("【")[0]
        self.assertIn("已领取", a_section)
        self.assertIn("可领取", b_section)


if __name__ == "__main__":
    unittest.main()
