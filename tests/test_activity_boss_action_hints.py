"""Activity boss action hints: the status block tells the player what to do.

`活动首领` already renders the boss name, hp, damage, item inventory and
milestone/rank states, but never the commands themselves: item-raid bosses
did not mention `活动讨伐 [首领名] 道具名`, cooperative bosses did not
mention `活动讨伐`, and a claimable rank or progress tier had no hint. With
several active bosses every claim needs the boss name (the claim command
refuses ambiguous queries), so multi-boss hints must carry it.

Written as ``unittest.TestCase`` because the CI quality job collects tests
with ``python -m unittest discover``.
"""

from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)

NOW = datetime(2026, 10, 15, 12)

BOSS = {
    "key": "boss1",
    "type": "activity_boss",
    "enabled": True,
    "name": "年兽来袭",
    "boss_name": "年兽",
    "mode": "item_raid",
    "items": [{"id": "firecracker", "name": "爆竹"}],
    "start_time": "0",
    "end_time": "无限",
    "max_hp": 1000,
    "server_milestones": [
        {"key": "m1", "hp_percent": 30, "name": "进度一", "reward": "灵石x10"}
    ],
    "rank_rewards": [{"rank_min": 1, "rank_max": 3, "name": "前三", "reward": "灵石x30"}],
}

COOP = {
    **BOSS,
    "key": "boss2",
    "name": "巨龟临渊",
    "boss_name": "巨龟",
    "mode": "cooperative",
    "items": [],
}


def _row(**overrides) -> dict:
    row = {
        "hp_left": 500,
        "stored_max_hp": 1000,
        "damage": 120,
        "fight_count": 1,
        "inventory": {"firecracker": 2},
        "milestones_unlocked": set(),
        "milestones_claimed": set(),
        "rank_claimed": set(),
    }
    row.update(overrides)
    return row


class BossActionHintTests(unittest.TestCase):
    # --- 行动行 -------------------------------------------------------

    def test_item_raid_block_hints_the_item_command(self) -> None:
        text = ActivityReadModelApplication._status_block(BOSS, _row())
        self.assertIn("讨伐：活动讨伐 年兽 爆竹", text)

    def test_cooperative_block_hints_the_plain_command(self) -> None:
        text = ActivityReadModelApplication._status_block(COOP, _row())
        self.assertIn("讨伐：活动讨伐", text)
        self.assertNotIn("活动讨伐 巨龟", text)

    def test_both_mode_hints_both_commands(self) -> None:
        boss = {**BOSS, "mode": "both"}
        text = ActivityReadModelApplication._status_block(boss, _row())
        self.assertIn("讨伐：活动讨伐 年兽 爆竹", text)
        self.assertIn("讨伐：活动讨伐\n", text)

    # --- 排行档领取提示 ------------------------------------------------

    def test_rank_hint_when_position_reaches_an_unclaimed_tier(self) -> None:
        text = ActivityReadModelApplication._status_block(BOSS, _row(), rank_position=2)
        self.assertIn("可用 活动首领领奖 排行 领取", text)

    def test_no_rank_hint_when_unranked(self) -> None:
        text = ActivityReadModelApplication._status_block(BOSS, _row(), rank_position=0)
        self.assertNotIn("活动首领领奖 排行", text)

    def test_no_rank_hint_when_position_below_every_tier(self) -> None:
        text = ActivityReadModelApplication._status_block(BOSS, _row(), rank_position=9)
        self.assertNotIn("活动首领领奖 排行", text)

    def test_no_rank_hint_when_tier_already_claimed(self) -> None:
        text = ActivityReadModelApplication._status_block(
            BOSS, _row(rank_claimed={"1-3"}), rank_position=2
        )
        self.assertNotIn("活动首领领奖 排行", text)

    # --- 进度档领取提示（多首领带名） -----------------------------------

    def test_progress_hint_carries_the_boss_name_with_several_active_bosses(self) -> None:
        text = ActivityReadModelApplication._status_block(
            BOSS, _row(milestones_unlocked={"m1"}), multiple_active=True
        )
        self.assertIn("可用 活动首领领奖 进度 年兽 领取已解锁的进度奖励", text)

    def test_progress_hint_stays_nameless_for_a_single_boss(self) -> None:
        text = ActivityReadModelApplication._status_block(
            BOSS, _row(milestones_unlocked={"m1"})
        )
        self.assertIn("可用 活动首领领奖 进度 领取已解锁的进度奖励", text)
        self.assertNotIn("活动首领领奖 进度 年兽", text)

    def test_rank_hint_carries_the_boss_name_with_several_active_bosses(self) -> None:
        text = ActivityReadModelApplication._status_block(
            BOSS, _row(), rank_position=2, multiple_active=True
        )
        self.assertIn("可用 活动首领领奖 排行 年兽 领取", text)

    def test_rank_hint_stays_nameless_for_a_single_boss(self) -> None:
        text = ActivityReadModelApplication._status_block(BOSS, _row(), rank_position=2)
        self.assertIn("可用 活动首领领奖 排行 领取", text)
        self.assertNotIn("活动首领领奖 排行 年兽", text)

    # --- 无可领/无配置的静默 -------------------------------------------

    def test_no_claim_hints_without_pending_milestones_or_rank(self) -> None:
        text = ActivityReadModelApplication._status_block(BOSS, _row())
        self.assertNotIn("活动首领领奖", text)


if __name__ == "__main__":
    unittest.main()
