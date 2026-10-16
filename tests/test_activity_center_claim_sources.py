"""Focused activity-center links for each existing claim source."""

from __future__ import annotations

from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from urllib.parse import quote

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    build_activity_center_items,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
    strip_md_command_links,
)


NOW = datetime(2026, 10, 15, 12)

STAGE = {
    "key": "closed",
    "name": "攻坚期",
    "start_time": "2026-09-01",
    "end_time": "2026-10-01",
    "features": ["sign"],
    "rewards": [{"key": "stage", "name": "阶段礼", "reward": "灵石x100"}],
}

RANK_CONFIG = {
    "boards": [{"key": "points", "label": "积分榜", "source": "points", "limit": 10}],
    "rewards": [
        {
            "board": "points",
            "rank_min": 1,
            "rank_max": 1,
            "key": "champion",
            "name": "榜首",
            "reward": "灵石x1000",
        }
    ],
}

BOSS = {
    "key": "boss",
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
    }
    config.update(overrides)
    return config


def _commands(config: dict, snapshot: dict) -> tuple[tuple[str, str], ...]:
    return build_activity_center_items(
        config, NOW, claimable=True, snapshot=snapshot
    )[0].commands


class ActivityCenterClaimSourcesTests(unittest.TestCase):
    def test_each_specialized_source_gets_only_its_existing_claim_command(self) -> None:
        pass_config = {
            "enabled": True,
            "level_exp": 100,
            "level_rewards": [{"level": 1, "name": "一阶", "reward": "灵石x10"}],
        }
        cases = [
            (
                _config(extensions={"activity_pass": pass_config}),
                {"pass_total_exp": 100, "pass_claimed_levels": set()},
                ("活动战令领取", "活动战令领取"),
            ),
            (
                _config(gameplay_activities=[BOSS]),
                {"boss_milestone_unlocked": {"m1"}, "boss_milestone_claimed": set()},
                ("活动首领领奖 进度", "活动首领领奖 进度"),
            ),
            (
                _config(gameplay_activities=[BOSS]),
                {"board_positions": {"boss": 1}, "boss_rank_claimed": set()},
                ("活动首领领奖 排行", "活动首领领奖 排行"),
            ),
        ]
        for config, snapshot, expected in cases:
            commands = _commands(config, snapshot)
            self.assertIn(expected, commands)
            self.assertEqual(sum(command == expected[1] for _, command in commands), 1)
            self.assertEqual(
                commands[:2], (("领取奖励", "活动领取"), ("活动任务", "活动任务"))
            )

    def test_multiple_sources_keep_stable_order_without_duplicate_targets(self) -> None:
        config = _config(
            extensions={
                "stages": [STAGE],
                "rank": RANK_CONFIG,
                "activity_pass": {
                    "enabled": True,
                    "level_exp": 100,
                    "level_rewards": [{"level": 1, "reward": "灵石x10"}],
                },
            },
            gameplay_activities=[BOSS, dict(BOSS, key="boss-copy")],
        )
        snapshot = {
            "stage_participated": {"closed"},
            "stage_claimed": set(),
            "board_positions": {"points": 1, "boss": 1},
            "rank_claimed": set(),
            "pass_total_exp": 100,
            "pass_claimed_levels": set(),
            "boss_milestone_unlocked": {"m1"},
            "boss_milestone_claimed": set(),
            "boss_rank_claimed": set(),
        }
        commands = _commands(config, snapshot)
        self.assertEqual(
            commands,
            (
                ("领取奖励", "活动领取"),
                ("活动任务", "活动任务"),
                ("活动阶段领取", "活动阶段领取"),
                ("活动排行领取", "活动排行领取"),
                ("活动战令领取", "活动战令领取"),
                ("活动首领领奖 进度", "活动首领领奖 进度"),
                ("活动首领领奖 排行", "活动首领领奖 排行"),
            ),
        )
        self.assertEqual(len({command for _, command in commands}), len(commands))

    def test_missing_or_already_claimed_state_does_not_show_specialized_links(self) -> None:
        config = _config(
            extensions={
                "stages": [STAGE],
                "rank": RANK_CONFIG,
                "activity_pass": {"enabled": False},
            },
            gameplay_activities=[BOSS],
        )
        # 配置存在但 snapshot 没有资格状态：不得把查看配置误报成可领奖。
        self.assertEqual(
            _commands(config, {}),
            (("领取奖励", "活动领取"), ("活动任务", "活动任务")),
        )
        claimed = {
            "stage_participated": {"closed"},
            "stage_claimed": {"closed"},
            "board_positions": {"points": 1, "boss": 1},
            "rank_claimed": {("points", "champion")},
            "boss_milestone_unlocked": {"m1"},
            "boss_milestone_claimed": {"m1"},
            "boss_rank_claimed": {"1-3"},
        }
        self.assertEqual(
            _commands(config, claimed),
            (("领取奖励", "活动领取"), ("活动任务", "活动任务")),
        )

    def test_source_links_render_with_argument_and_plain_text_fallback(self) -> None:
        config = _config(gameplay_activities=[BOSS])
        app = ActivityReadModelApplication(
            "/tmp/activity-center-claim-sources-missing.db",
            config_loader=lambda: config,
            clock=type("Clock", (), {"now": lambda self: NOW})(),
        )
        app.repository.overview_snapshot = lambda *args, **kwargs: {
            "tasks": {},
            "pass_total_exp": 0,
            "pass_claimed_levels": set(),
            "boss_milestone_unlocked": {"m1"},
            "boss_milestone_claimed": set(),
            "boss_progress": {},
        }
        text = app.activity_center_text("u1")
        self.assertIn("活动首领领奖 进度", strip_md_command_links(text))
        self.assertNotIn("mqqapi://aio/inlinecmd", strip_md_command_links(text))
        self.assertIn(quote("活动首领领奖 进度"), text)

    def test_old_game_database_without_optional_claim_tables_stays_compatible(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "old-game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                uow.execute(
                    "CREATE TABLE activity_user ("
                    "user_id TEXT PRIMARY KEY, sign_days INTEGER, total_sign_days INTEGER, "
                    "last_sign_date TEXT)"
                )
            config = _config(
                extensions={
                    "stages": [STAGE],
                    "rank": RANK_CONFIG,
                    "activity_pass": {"enabled": False},
                },
                gameplay_activities=[BOSS],
            )
            app = ActivityReadModelApplication(
                database,
                config_loader=lambda: config,
                clock=type("Clock", (), {"now": lambda self: NOW})(),
            )
            text = app.activity_center_text("u1")
            plain = strip_md_command_links(text)
            self.assertNotIn("奖励领取", plain)
            self.assertNotIn("活动阶段领取", plain)
            self.assertNotIn("活动排行领取", plain)
            self.assertNotIn("活动首领领奖", plain)
