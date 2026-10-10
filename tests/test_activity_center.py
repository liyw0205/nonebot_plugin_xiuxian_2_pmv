from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    ENDED,
    IN_PROGRESS,
    NOT_STARTED,
    REWARD_CLAIM,
    build_activity_center_items,
    claimable_reward_count,
    resolve_status,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
    strip_md_command_links,
)


def test_activity_center_resolves_four_lifecycle_states() -> None:
    before = datetime(2026, 10, 1, 12)
    during = datetime(2026, 10, 15, 12)
    after = datetime(2026, 11, 1, 12)
    assert resolve_status("2026-10-10", "2026-10-31", before) == NOT_STARTED
    assert resolve_status("2026-10-10", "2026-10-31", during) == IN_PROGRESS
    assert resolve_status("2026-10-10", "2026-10-31", after) == ENDED
    assert resolve_status("2026-10-10", "2026-10-31", during, claimable=True) == REWARD_CLAIM


def test_activity_center_is_config_driven_and_counts_claimable_rewards() -> None:
    config = {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "start_time": "2026-10-01",
        "end_time": "2026-10-31",
        "daily_tasks": [{"key": "daily", "target": 1}],
        "weekly_tasks": [],
        "extensions": {"activity_pass": {"enabled": False}},
        "gameplay_activities": [{
            "key": "spring_collect",
            "type": "collect_words",
            "enabled": True,
            "name": "春日集字",
            "description": "配置中的玩法说明",
            "start_time": "2026-10-01",
            "end_time": "2026-10-31",
            "phrases": [{"phrase": "春风", "name": "春风", "reward": "灵石x1"}],
        }],
    }
    snapshot = {
        "tasks": {("daily", "2026-10-15", "daily"): {"progress": 1, "claimed": False}},
        "pass_total_exp": 0,
        "pass_claimed_levels": set(),
    }
    now = datetime(2026, 10, 15, 12)
    assert claimable_reward_count(config, snapshot, now) == 1
    items = build_activity_center_items(config, now, claimable=True)
    assert [item.name for item in items] == ["春日庆典", "春日集字"]
    assert items[0].status == REWARD_CLAIM
    assert items[1].status == IN_PROGRESS
    assert "灵石x1" in items[1].reward_hint


def test_activity_center_message_has_qq_links_and_plain_fallback() -> None:
    config = {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "description": "配置说明",
        "start_time": "0",
        "end_time": "无限",
        "daily_tasks": [],
        "weekly_tasks": [],
        "gameplay_activities": [],
        "extensions": {"activity_pass": {"enabled": False}},
    }
    app = ActivityReadModelApplication(
        "/tmp/activity-center-missing.db",
        config_loader=lambda: config,
        clock=SimpleNamespace(now=lambda: datetime(2026, 10, 15, 12)),
    )
    markdown = app.activity_center_text()
    assert "**活动中心**" in markdown
    assert "mqqapi://aio/inlinecmd" in markdown
    assert "活动签到" in strip_md_command_links(markdown)
    assert "mqqapi://aio/inlinecmd" not in strip_md_command_links(markdown)
