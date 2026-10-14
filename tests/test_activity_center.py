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
        "start_time": "2026-10-10",
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
    # QQ short-message contract: one aggregated card, gameplay folded into
    # a single status line instead of duplicated cards and links.
    assert [item.name for item in items] == ["春日庆典"]
    assert items[0].status == REWARD_CLAIM
    assert "春日集字·进行中" in items[0].gameplay_summary
    assert "灵石x1" in items[0].reward_hint


def test_center_entry_commands_follow_lifecycle_state() -> None:
    from nonebot_plugin_xiuxian_2.features.activity.center import commands_for_status

    # 未开始只能看详情；进行中给出参与/任务/战令；可领奖给领取；已结束无入口。
    assert commands_for_status(NOT_STARTED) == (("查看详情", "活动信息"),)
    assert [label for label, _ in commands_for_status(IN_PROGRESS)] == [
        "活动签到", "活动任务", "活动战令",
    ]
    assert ("领取奖励", "活动领取") in commands_for_status(REWARD_CLAIM)
    assert commands_for_status(ENDED) == ()
    gameplay = {"type": "collect_words", "enabled": True}
    assert commands_for_status(IN_PROGRESS, gameplay) == (("集字背包", "活动背包"), ("集字兑换", "活动兑换"))
    assert commands_for_status(ENDED, gameplay) == ()
    # Every mapped command must be an existing activity command entry.
    all_targets = {
        command
        for state in (NOT_STARTED, IN_PROGRESS, REWARD_CLAIM, ENDED)
        for _, command in commands_for_status(state)
    }
    assert all_targets <= {
        "活动信息", "活动签到", "活动任务", "活动战令", "活动领取",
    }


def test_personal_progress_summary_is_read_only_line() -> None:
    from nonebot_plugin_xiuxian_2.features.activity.center import (
        personal_progress_summary,
    )

    config = {
        "daily_tasks": [{"key": "d1", "target": 2}],
        "weekly_tasks": [{"key": "w1", "target": 5}],
        "extensions": {"activity_pass": {"enabled": True, "level_exp": 100}},
    }
    snapshot = {
        "tasks": {
            ("daily", "2026-10-15", "d1"): {"progress": 2, "claimed": True},
            ("weekly", "2026-41", "w1"): {"progress": 1, "claimed": False},
        },
        "pass_total_exp": 250,
        "pass_claimed_levels": set(),
    }
    line = personal_progress_summary(config, snapshot, datetime(2026, 10, 15, 12))
    assert line.startswith("进度：")
    assert "日常任务 1/1" in line
    assert "周常任务 0/1" in line
    assert "战令 Lv2" in line


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
    # 无限/无时限配置不显示倒计时提示。
    assert "剩余" not in markdown
    assert "后开启" not in markdown


def test_activity_center_time_hint_is_config_driven() -> None:
    from nonebot_plugin_xiuxian_2.features.activity.center import time_hint

    now = datetime(2026, 10, 15, 12)
    # 日期型 end_time 解析为当日 23:59:59，因此剩余按向下取整天/小时显示。
    assert time_hint(IN_PROGRESS, "2026-10-01", "2026-10-20", now) == "剩余 5 天"
    assert time_hint(IN_PROGRESS, "2026-10-01", "2026-10-15", now) == "剩余 11 小时"
    assert time_hint(NOT_STARTED, "2026-10-18", "2026-10-31", now) == "2 天后开启"
    # 无截止时间（"无限"）与已结束/可领奖状态不显示提示。
    assert time_hint(IN_PROGRESS, "0", "无限", now) == ""
    assert time_hint(ENDED, "2026-10-01", "2026-10-10", now) == ""
    assert time_hint(REWARD_CLAIM, "2026-10-01", "2026-10-10", now) == ""

    config = {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "start_time": "2026-10-01",
        "end_time": "2026-10-20",
        "daily_tasks": [],
        "weekly_tasks": [],
        "gameplay_activities": [],
        "extensions": {"activity_pass": {"enabled": False}},
    }
    item = build_activity_center_items(config, now)[0]
    assert item.time_hint == "剩余 5 天"
    app = ActivityReadModelApplication(
        "/tmp/activity-center-missing.db",
        config_loader=lambda: config,
        clock=SimpleNamespace(now=lambda: now),
    )
    assert "剩余 5 天" in app.activity_center_text()
