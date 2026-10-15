"""Activity center claimable coverage: every claim source drives 奖励领取.

The one-click 活动领取 covers tasks, the battle pass, stage, rank, boss
milestones and boss rank. The center card must report 奖励领取 and offer the
领取奖励 entry whenever any of those sources has something claimable.

Written as ``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    ENDED,
    REWARD_CLAIM,
    build_activity_center_items,
    claimable_reward_count,
)
from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import (
    ActivityReadModelSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

NOW = datetime(2026, 10, 15, 12)

ENDED_STAGE = {
    "key": "open",
    "name": "攻坚期",
    "stage_type": "boss",
    "start_time": "2026-09-01",
    "end_time": "2026-10-01",
    "features": ["sign"],
    "multiplier": 1.5,
    "rewards": [{"key": "r1", "name": "阶段礼", "reward": "灵石x100"}],
}
BOARDS = [
    {"key": "sign", "label": "签到榜", "source": "sign", "limit": 10},
    {"key": "points", "label": "积分榜", "source": "points", "limit": 2},
]
REWARDS = [
    {
        "board": "points",
        "rank_min": 1,
        "rank_max": 1,
        "key": "champion",
        "name": "榜首",
        "reward": "灵石x1000",
    },
]
BOSS_ACTIVITY = {
    "key": "boss1",
    "type": "activity_boss",
    "enabled": True,
    "name": "活动首领",
    "start_time": "0",
    "end_time": "无限",
    "server_milestones": [
        {"key": "m1", "hp_percent": 30, "name": "进度一", "reward": "灵石x10"}
    ],
    "rank_rewards": [
        {"rank_min": 1, "rank_max": 3, "name": "前三", "reward": "灵石x20"}
    ],
}


def _config(*, stages=(), rank=None, boss=None, enabled: bool = True) -> dict:
    extensions: dict = {}
    if stages:
        extensions["stages"] = list(stages)
    if rank is not None:
        extensions["rank"] = rank
    config = {
        "template_key": "spring",
        "enabled": enabled,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "extensions": extensions,
    }
    if boss is not None:
        config["gameplay_activities"] = [boss]
    return config


class ActivityCenterClaimableTests(unittest.TestCase):
    def _database(self) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_activity_state_schema(uow)
        return database

    # --- 阶段二：六来源可领取计算 -------------------------------------

    def test_snapshot_without_claim_state_keeps_the_legacy_count(self) -> None:
        """Older callers pass no claim state and keep their previous count."""
        config = _config(stages=[ENDED_STAGE], rank={"boards": BOARDS, "rewards": REWARDS})
        snapshot = {"tasks": {}, "pass_total_exp": 0, "pass_claimed_levels": set()}
        self.assertEqual(claimable_reward_count(config, snapshot, NOW), 0)

    def test_stage_reward_makes_the_center_claimable(self) -> None:
        config = _config(stages=[ENDED_STAGE])
        self.assertEqual(
            claimable_reward_count(
                config, {"stage_participated": {"open"}, "stage_claimed": set()}, NOW
            ),
            1,
        )
        self.assertEqual(
            claimable_reward_count(
                config, {"stage_participated": {"open"}, "stage_claimed": {"open"}}, NOW
            ),
            0,
        )
        # 未参与该阶段时不计入。
        self.assertEqual(
            claimable_reward_count(
                config, {"stage_participated": set(), "stage_claimed": set()}, NOW
            ),
            0,
        )

    def test_rank_tier_makes_the_center_claimable(self) -> None:
        config = _config(rank={"boards": BOARDS, "rewards": REWARDS})
        self.assertEqual(
            claimable_reward_count(
                config, {"board_positions": {"points": 1}, "rank_claimed": set()}, NOW
            ),
            1,
        )
        self.assertEqual(
            claimable_reward_count(
                config,
                {
                    "board_positions": {"points": 1},
                    "rank_claimed": {("points", "champion")},
                },
                NOW,
            ),
            0,
        )
        # 名次不在档位内时不计入。
        self.assertEqual(
            claimable_reward_count(
                config, {"board_positions": {"points": 9}, "rank_claimed": set()}, NOW
            ),
            0,
        )

    def test_boss_milestone_makes_the_center_claimable(self) -> None:
        config = _config(boss=BOSS_ACTIVITY)
        self.assertEqual(
            claimable_reward_count(
                config,
                {"boss_milestone_unlocked": {"m1"}, "boss_milestone_claimed": set()},
                NOW,
            ),
            1,
        )
        self.assertEqual(
            claimable_reward_count(
                config,
                {"boss_milestone_unlocked": {"m1"}, "boss_milestone_claimed": {"m1"}},
                NOW,
            ),
            0,
        )
        # 未解锁的全服进度不计入。
        self.assertEqual(
            claimable_reward_count(
                config,
                {"boss_milestone_unlocked": set(), "boss_milestone_claimed": set()},
                NOW,
            ),
            0,
        )

    def test_boss_rank_tier_makes_the_center_claimable(self) -> None:
        config = _config(boss=BOSS_ACTIVITY)
        self.assertEqual(
            claimable_reward_count(
                config, {"board_positions": {"boss": 2}, "boss_rank_claimed": set()}, NOW
            ),
            1,
        )
        self.assertEqual(
            claimable_reward_count(
                config,
                {"board_positions": {"boss": 2}, "boss_rank_claimed": {"1-3"}},
                NOW,
            ),
            0,
        )
        self.assertEqual(
            claimable_reward_count(
                config, {"board_positions": {"boss": 0}, "boss_rank_claimed": set()}, NOW
            ),
            0,
        )

    def test_sources_add_up(self) -> None:
        config = _config(
            stages=[ENDED_STAGE],
            rank={"boards": BOARDS, "rewards": REWARDS},
            boss=BOSS_ACTIVITY,
        )
        snapshot = {
            "stage_participated": {"open"},
            "stage_claimed": set(),
            "board_positions": {"points": 1, "boss": 2},
            "rank_claimed": set(),
            "boss_milestone_unlocked": {"m1"},
            "boss_milestone_claimed": set(),
            "boss_rank_claimed": set(),
        }
        self.assertEqual(claimable_reward_count(config, snapshot, NOW), 4)

    # --- 阶段三：卡片状态与入口 ---------------------------------------

    def test_only_stage_claimable_still_shows_the_reward_claim_entry(self) -> None:
        config = _config(stages=[ENDED_STAGE])
        snapshot = {"stage_participated": {"open"}, "stage_claimed": set()}
        claimable = bool(claimable_reward_count(config, snapshot, NOW))
        self.assertTrue(claimable)
        items = build_activity_center_items(config, NOW, claimable=claimable)
        self.assertEqual(items[0].status, REWARD_CLAIM)
        self.assertIn(("领取奖励", "活动领取"), items[0].commands)

    def test_disabled_activity_reports_ended_without_entries(self) -> None:
        config = _config(stages=[ENDED_STAGE], enabled=False)
        snapshot = {"stage_participated": {"open"}, "stage_claimed": set()}
        items = build_activity_center_items(
            config, NOW, claimable=bool(claimable_reward_count(config, snapshot, NOW))
        )
        self.assertEqual(items[0].status, ENDED)
        self.assertEqual(items[0].commands, ())

    # --- 阶段一：只读快照携带领取状态 ---------------------------------

    def test_overview_snapshot_carries_claim_state(self) -> None:
        database = self._database()
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_stage_claim(activity_key,user_id,stage_key,reward,create_time) "
                "VALUES('spring','u1','open','灵石x100','2026-10-02 00:00:00')"
            )
            uow.execute(
                "INSERT INTO activity_rank_claim(activity_key,user_id,board_key,tier_key,reward,create_time) "
                "VALUES('spring','u1','points','champion','灵石x1000','2026-10-02 00:00:00')"
            )
            uow.execute(
                "INSERT INTO activity_boss_milestone(activity_key,milestone_key,unlocked_time) "
                "VALUES('spring','m1','2026-10-02 00:00:00')"
            )
            uow.execute(
                "INSERT INTO activity_boss_rank_claim(activity_key,user_id,tier_key,create_time) "
                "VALUES('spring','u1','1-3','2026-10-02 00:00:00')"
            )
        snapshot = ActivityReadModelSqlRepository(database).overview_snapshot(
            "u1",
            "spring",
            include_tasks=False,
            include_pass=False,
            collect_activity_keys=[],
        )
        self.assertEqual(snapshot["stage_claimed"], {"open"})
        self.assertEqual(snapshot["rank_claimed"], {("points", "champion")})
        self.assertEqual(snapshot["boss_milestone_unlocked"], {"m1"})
        self.assertEqual(snapshot["boss_milestone_claimed"], set())
        self.assertEqual(snapshot["boss_rank_claimed"], {"1-3"})
        self.assertEqual(snapshot["stage_participated"], set())
        self.assertEqual(snapshot["board_positions"], {})

    def test_overview_snapshot_without_claim_tables_is_empty(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE IF NOT EXISTS activity_user "
                "(user_id TEXT PRIMARY KEY,sign_days INTEGER,total_sign_days INTEGER,last_sign_date TEXT)"
            )
        snapshot = ActivityReadModelSqlRepository(database).overview_snapshot(
            "u1",
            "spring",
            include_tasks=False,
            include_pass=False,
            collect_activity_keys=[],
        )
        self.assertEqual(snapshot["stage_claimed"], set())
        self.assertEqual(snapshot["rank_claimed"], set())
        self.assertEqual(snapshot["boss_milestone_unlocked"], set())
        self.assertEqual(snapshot["boss_rank_claimed"], set())


if __name__ == "__main__":
    unittest.main()
