"""Activity center progress line: stage and rank coverage.

The center progress line already covered sign-in, tasks, the battle pass, the
collect pity, event points and boss progress; this module pins the stage and
rank segments that complete the read-only progress coverage.

Written as ``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
"""

from __future__ import annotations

import unittest
from datetime import datetime

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    claimable_reward_count,
    personal_progress_summary,
)

NOW = datetime(2026, 10, 15, 12)

BOUNDED_STAGE = {
    "key": "open",
    "name": "攻坚期",
    "stage_type": "boss",
    "start_time": "2026-10-01",
    "end_time": "2026-11-01",
    "features": ["sign"],
    "multiplier": 1.5,
}
FUTURE_STAGE = {
    "key": "settle",
    "name": "结算期",
    "stage_type": "settlement",
    "start_time": "2026-12-01",
    "end_time": "2026-12-31",
    "features": ["claim"],
    "multiplier": 1.0,
}
UNLIMITED_STAGE = {
    "key": "live",
    "name": "正式期",
    "stage_type": "open",
    "start_time": "0",
    "end_time": "无限",
    "features": ["sign", "task"],
    "multiplier": 1.0,
}
BOARDS = [{"key": "points", "label": "积分榜", "source": "points", "limit": 2}]
REWARDS = [
    {
        "board": "points",
        "rank_min": 1,
        "rank_max": 3,
        "key": "top3",
        "name": "前三",
        "reward": "灵石x500",
    }
]


def _config(*, stages=(), rank=None) -> dict:
    extensions: dict = {}
    if stages:
        extensions["stages"] = list(stages)
    if rank is not None:
        extensions["rank"] = rank
    return {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "extensions": extensions,
    }


class ActivityCenterProgressTests(unittest.TestCase):
    # --- 阶段段 --------------------------------------------------------

    def test_stage_segment_shows_name_and_countdown(self) -> None:
        line = personal_progress_summary(_config(stages=[BOUNDED_STAGE]), {}, NOW)
        self.assertIn("阶段 攻坚期 剩余", line)

    def test_stage_segment_omits_countdown_for_unlimited_windows(self) -> None:
        line = personal_progress_summary(_config(stages=[UNLIMITED_STAGE]), {}, NOW)
        self.assertIn("阶段 正式期", line)
        self.assertNotIn("剩余", line)

    def test_stage_segment_absent_without_a_running_stage(self) -> None:
        # 未开始的阶段没有可展示的当前阶段；未配置 stages 时按默认阶段集处理。
        line = personal_progress_summary(_config(stages=[FUTURE_STAGE]), {}, NOW)
        self.assertNotIn("阶段 ", line)

    # --- 排行段 --------------------------------------------------------

    def test_rank_segment_shows_own_position(self) -> None:
        config = _config(rank={"boards": BOARDS, "rewards": REWARDS})
        line = personal_progress_summary(config, {"board_positions": {"points": 2}}, NOW)
        self.assertIn("积分榜 第 2 名", line)

    def test_rank_segment_absent_when_unranked(self) -> None:
        config = _config(rank={"boards": BOARDS, "rewards": REWARDS})
        self.assertNotIn("积分榜", personal_progress_summary(config, {}, NOW))
        self.assertNotIn(
            "积分榜",
            personal_progress_summary(config, {"board_positions": {"points": 0}}, NOW),
        )

    def test_rank_segment_absent_without_configured_boards(self) -> None:
        line = personal_progress_summary(_config(), {"board_positions": {"points": 2}}, NOW)
        self.assertNotIn("第 2 名", line)

    # --- 组合 ----------------------------------------------------------

    def test_claimable_count_still_closes_the_line(self) -> None:
        config = _config(stages=[BOUNDED_STAGE], rank={"boards": BOARDS, "rewards": REWARDS})
        snapshot = {"board_positions": {"points": 2}}
        line = personal_progress_summary(config, snapshot, NOW)
        self.assertIn("阶段 攻坚期", line)
        self.assertIn("积分榜 第 2 名", line)
        self.assertTrue(line.endswith("可领 1 项"), line)
        self.assertEqual(claimable_reward_count(config, snapshot, NOW), 1)


if __name__ == "__main__":
    unittest.main()
