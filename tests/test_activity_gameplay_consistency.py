"""The center card and the gameplay detail commands must agree.

Written as ``unittest.TestCase`` on purpose: the CI quality job collects tests
with ``python -m unittest discover``, so plain pytest-style functions would
never execute there.

Phase 1 covers the collect (集字) pity progress: one shared computation feeds
both the card segment and the 活动背包 detail line.
"""

from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    build_activity_center_items,
    claimable_reward_count,
    personal_progress_summary,
)
from nonebot_plugin_xiuxian_2.features.activity.gameplay_progress import (
    claimable_pass_rewards,
    closest_pity_entry,
    collect_pity_entries,
    gameplay_status_text,
    pass_level,
    task_claimable,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
    strip_md_command_links,
)

NOW = datetime(2026, 10, 15, 12)

COLLECT_ACTIVITY = {
    "key": "spring_collect",
    "type": "collect_words",
    "enabled": True,
    "name": "春日集字",
    "start_time": "0",
    "end_time": "无限",
    "pity_threshold": 10,
    "drop_events": ["boss", "work"],
}


def _config(**overrides) -> dict:
    config = {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "daily_tasks": [],
        "weekly_tasks": [],
        "extensions": {"activity_pass": {"enabled": False}},
        "gameplay_activities": [dict(COLLECT_ACTIVITY)],
    }
    config.update(overrides)
    return config


class GameplayProgressConsistencyTests(unittest.TestCase):
    def test_collect_pity_entries_follow_config_order_and_cap(self) -> None:
        entries = collect_pity_entries(
            COLLECT_ACTIVITY,
            {("spring_collect", "boss"): 3, ("spring_collect", "work"): 99},
        )
        # 顺序按配置 drop_events，miss 以阈值截断，label 用事件标签。
        self.assertEqual(
            entries, [("世界BOSS", 3, 10), ("悬赏令", 10, 10)]
        )
        self.assertEqual(collect_pity_entries(COLLECT_ACTIVITY, {}), [
            ("世界BOSS", 0, 10),
            ("悬赏令", 0, 10),
        ])
        # 无阈值或无掉落事件时不产生条目。
        self.assertEqual(
            collect_pity_entries(dict(COLLECT_ACTIVITY, pity_threshold=0), {}), []
        )
        self.assertEqual(
            collect_pity_entries(dict(COLLECT_ACTIVITY, drop_events=[]), {}), []
        )

    def test_closest_pity_entry_prefers_largest_miss(self) -> None:
        self.assertEqual(
            closest_pity_entry([("世界BOSS", 3, 10), ("悬赏令", 7, 10)]),
            ("悬赏令", 7, 10),
        )
        # 并列时保持配置顺序（max 取第一个最大值）。
        self.assertEqual(
            closest_pity_entry([("世界BOSS", 5, 10), ("悬赏令", 5, 10)]),
            ("世界BOSS", 5, 10),
        )
        self.assertIsNone(closest_pity_entry([]))

    def test_card_pity_matches_activity_bag_for_the_same_event(self) -> None:
        config = _config()
        pity = {("spring_collect", "boss"): 3, ("spring_collect", "work"): 7}
        snapshot = {
            "tasks": {},
            "pass_total_exp": 0,
            "pass_claimed_levels": set(),
            "sign": {"last_sign_date": "2026-10-15"},
            "collect_pity": pity,
        }
        line = personal_progress_summary(config, snapshot, NOW)
        # 卡片取最接近保底的事件（悬赏令 7/10）。
        self.assertIn("春日集字 保底 7/10", line)

        app = ActivityReadModelApplication(
            "/tmp/activity-gameplay-consistency-missing.db",
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: NOW),
        )
        app.repository.collect_state = lambda *a, **k: {
            "inventory": {},
            "claims": {},
            "pity": pity,
        }
        bag = app.collect_bag_text("u-1")
        self.assertIn("保底进度：世界BOSS 3/10、悬赏令 7/10", bag)
        # 两侧同源：卡片数字必须等于背包中同一事件的值。
        self.assertIn("悬赏令 7/10", bag)
        self.assertIn("春日集字 保底 7/10", line)

    def test_card_pity_segment_absent_without_pity_state(self) -> None:
        config = _config()
        snapshot = {
            "tasks": {},
            "pass_total_exp": 0,
            "pass_claimed_levels": set(),
        }
        line = personal_progress_summary(config, snapshot, NOW)
        self.assertNotIn("保底", line)

    # --- 阶段二：积分余额一致性 -------------------------------------------

    def test_card_points_match_points_detail(self) -> None:
        config = _config(
            gameplay_activities=[
                {
                    "key": "spring_points",
                    "type": "event_points",
                    "enabled": True,
                    "name": "春日积分",
                    "point_name": "春分",
                    "start_time": "0",
                    "end_time": "无限",
                }
            ]
        )
        snapshot = {
            "tasks": {},
            "pass_total_exp": 0,
            "pass_claimed_levels": set(),
            "point_balances": {"spring_points": 120},
        }
        line = personal_progress_summary(config, snapshot, NOW)
        self.assertIn("春分 120", line)

        app = ActivityReadModelApplication(
            "/tmp/activity-gameplay-consistency-missing.db",
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: NOW),
        )
        app.repository.point_balances = lambda *a, **k: {
            "spring_points": {"points": 120, "total_points": 300}
        }
        detail = app.points_text("u-1")
        self.assertIn("春分：120", detail)
        # 两侧同源：卡片数字必须等于详情余额。
        self.assertIn("春分 120", line)

    # --- 阶段三：玩法状态一致性 -------------------------------------------

    def test_card_gameplay_status_matches_detail_status(self) -> None:
        cases = [
            (dict(COLLECT_ACTIVITY, enabled=False), "活动未开启"),
            (dict(COLLECT_ACTIVITY, start_time="2099-01-01"), "活动尚未开始"),
            (dict(COLLECT_ACTIVITY, end_time="2000-01-01"), "活动已结束"),
            (dict(COLLECT_ACTIVITY), "进行中"),
        ]
        for activity, expected_prefix in cases:
            with self.subTest(expected=expected_prefix):
                # 卡片与详情共用同一状态来源（含时间的原因文本逐字一致）。
                status = gameplay_status_text(activity)
                self.assertTrue(
                    status.startswith(expected_prefix), (status, expected_prefix)
                )
                config = _config(gameplay_activities=[activity])
                item = build_activity_center_items(config, NOW)[0]
                self.assertIn(f"春日集字·{status}", item.gameplay_summary)

                app = ActivityReadModelApplication(
                    "/tmp/activity-gameplay-consistency-missing.db",
                    config_loader=lambda: config,
                    clock=SimpleNamespace(now=lambda: NOW),
                )
                app.repository.collect_state = lambda *a, **k: {
                    "inventory": {},
                    "claims": {},
                    "pity": {},
                }
                bag = app.collect_bag_text("u-1")
                # 详情命令的状态文本与卡片逐字一致（不再出现“已结束 vs 未开启”）。
                self.assertIn(f"【春日集字】{status}", bag)

    # --- 阶段四：可领数量一致性 -------------------------------------------

    def test_card_claimable_count_matches_claim_rules(self) -> None:
        pass_config = {
            "enabled": True,
            "level_exp": 100,
            "max_level": 12,
            "level_rewards": [
                {"level": 1, "name": "初入庆典", "reward": "灵石x1"},
                {"level": 2, "name": "勤修补给", "reward": "灵石x2"},
                {"level": 3, "name": "历练补给", "reward": "灵石x3"},
            ],
        }
        config = _config(
            daily_tasks=[{"key": "d1", "target": 2}],
            weekly_tasks=[],
            extensions={"activity_pass": pass_config},
        )
        snapshot = {
            "tasks": {("daily", "2026-10-15", "d1"): {"progress": 2, "claimed": False}},
            "pass_total_exp": 250,
            "pass_claimed_levels": {1},
        }
        # 任务 1 项（达标未领）+ 战令 Lv2 未领 1 项 = 2。
        self.assertEqual(claimable_reward_count(config, snapshot, NOW), 2)
        self.assertTrue(task_claimable(snapshot["tasks"][("daily", "2026-10-15", "d1")], 2))
        self.assertEqual(
            claimable_pass_rewards(pass_config, 250, {1}),
            [2],
        )
        self.assertEqual(pass_level(pass_config, 250), 2)

        # 已领 / 未达标都不计入。
        claimed = {
            "tasks": {("daily", "2026-10-15", "d1"): {"progress": 2, "claimed": True}},
            "pass_total_exp": 250,
            "pass_claimed_levels": {1, 2},
        }
        self.assertEqual(claimable_reward_count(config, claimed, NOW), 0)
        below = {
            "tasks": {("daily", "2026-10-15", "d1"): {"progress": 1, "claimed": False}},
            "pass_total_exp": 0,
            "pass_claimed_levels": set(),
        }
        self.assertEqual(claimable_reward_count(config, below, NOW), 0)
        # 卡片计数为 0 时进度行不出现“可领”。
        self.assertNotIn(
            "可领", personal_progress_summary(config, below, NOW)
        )

    # --- 阶段五：呈现与帮助集成 -------------------------------------------

    def test_help_documents_gameplay_detail_entries(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_activity"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index('ACTIVITY_HELP = """') + len('ACTIVITY_HELP = """')
        help_text = source[start : source.index('"""', start)]
        for command in (
            "活动背包",
            "活动兑换",
            "活动积分",
            "活动商店",
            "活动首领",
            "活动首领排行",
        ):
            self.assertIn(command, help_text)

    def test_card_gameplay_entries_are_plain_text_safe(self) -> None:
        config = _config()
        app = ActivityReadModelApplication(
            "/tmp/activity-gameplay-consistency-missing.db",
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: NOW),
        )
        text = app.activity_center_text()
        plain = strip_md_command_links(text)
        # 新文案不得引入 markdown 语法或蓝字 URL 残留。
        self.assertNotIn("mqqapi", plain)
        self.assertNotIn("**", plain)
        self.assertIn("集字背包", plain)


if __name__ == "__main__":
    unittest.main()
