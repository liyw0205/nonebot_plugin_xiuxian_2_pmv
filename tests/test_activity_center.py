from __future__ import annotations

import unittest

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


class ActivityCenterTests(unittest.TestCase):
    def test_activity_center_resolves_four_lifecycle_states(self) -> None:
        before = datetime(2026, 10, 1, 12)
        during = datetime(2026, 10, 15, 12)
        after = datetime(2026, 11, 1, 12)
        assert resolve_status("2026-10-10", "2026-10-31", before) == NOT_STARTED
        assert resolve_status("2026-10-10", "2026-10-31", during) == IN_PROGRESS
        assert resolve_status("2026-10-10", "2026-10-31", after) == ENDED
        assert resolve_status("2026-10-10", "2026-10-31", during, claimable=True) == REWARD_CLAIM


    def test_activity_center_is_config_driven_and_counts_claimable_rewards(self) -> None:
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


    def test_center_entry_commands_follow_lifecycle_state(self) -> None:
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


    def test_personal_progress_summary_is_read_only_line(self) -> None:
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


    def test_activity_center_message_has_qq_links_and_plain_fallback(self) -> None:
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


    def test_activity_center_time_hint_is_config_driven(self) -> None:
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


    def test_personal_progress_line_carries_today_sign_state(self) -> None:
        from nonebot_plugin_xiuxian_2.features.activity.center import (
            personal_progress_summary,
        )

        now = datetime(2026, 10, 15, 12)
        config = {
            "daily_tasks": [],
            "weekly_tasks": [],
            "extensions": {"activity_pass": {"enabled": False}},
        }

        def summary(sign: dict | None) -> str:
            snapshot: dict = {"tasks": {}, "pass_total_exp": 0,
                              "pass_claimed_levels": set()}
            if sign is not None:
                snapshot["sign"] = sign
            return personal_progress_summary(config, snapshot, now)

        # 今日已签 / 昨日签 / 快照缺 sign 三种情形（只读，复用 overview 快照字段）。
        # 默认阶段集含进行中的「正式期」，进度行在签到状态后追加阶段段。
        assert summary({"last_sign_date": "2026-10-15"}) == "进度：签到 已签、暂无任务、阶段 正式期"
        assert summary({"last_sign_date": "2026-10-14"}) == "进度：签到 未签、暂无任务、阶段 正式期"
        assert summary(None) == "进度：签到 未签、暂无任务、阶段 正式期"

        app_config = {
            "template_key": "spring",
            "enabled": True,
            "name": "春日庆典",
            "start_time": "0",
            "end_time": "无限",
            "daily_tasks": [],
            "weekly_tasks": [],
            "gameplay_activities": [],
            "extensions": {"activity_pass": {"enabled": False}},
        }

        def render(user_id: str | None) -> str:
            app = ActivityReadModelApplication(
                "/tmp/activity-center-missing.db",
                config_loader=lambda: app_config,
                clock=SimpleNamespace(now=lambda: now),
            )
            if user_id is None:
                return app.activity_center_text()
            app.repository.overview_snapshot = lambda *a, **k: {
                "sign": {"last_sign_date": "2026-10-15", "sign_days": 3},
                "tasks": {},
                "pass_total_exp": 0,
                "pass_claimed_levels": set(),
                "pass_highest_level": 0,
                "collect_pity": {},
            }
            return app.activity_center_text(user_id)

        # 无用户不渲染进度行；有用户且今日已签时进度行带签到段。
        assert "进度：" not in render(None)
        assert "进度：签到 已签" in render("u-1")


    def test_progress_line_carries_collect_pity_and_points(self) -> None:
        from nonebot_plugin_xiuxian_2.features.activity.center import (
            personal_progress_summary,
        )

        now = datetime(2026, 10, 15, 12)
        config = {
            "daily_tasks": [],
            "weekly_tasks": [],
            "extensions": {"activity_pass": {"enabled": False}},
            "gameplay_activities": [
                {
                    "key": "spring_collect",
                    "type": "collect_words",
                    "enabled": True,
                    "name": "春日集字",
                    "pity_threshold": 10,
                    "drop_events": ["boss", "work"],
                },
                {
                    "key": "spring_points",
                    "type": "event_points",
                    "enabled": True,
                    "name": "春日积分",
                    "point_name": "春分",
                },
            ],
        }
        snapshot = {
            "tasks": {},
            "pass_total_exp": 0,
            "pass_claimed_levels": set(),
            "sign": {"last_sign_date": "2026-10-15"},
            "collect_pity": {("spring_collect", "boss"): 3, ("spring_collect", "work"): 7},
            "point_balances": {"spring_points": 120},
        }
        line = personal_progress_summary(config, snapshot, now)
        # 卡片取最接近保底的事件（悬赏令 7/10，与活动背包同源）。
        assert "春日集字 保底 7/10" in line
        assert "春分 120" in line
        # 保底不超过阈值。
        capped = dict(snapshot, collect_pity={("spring_collect", "boss"): 99})
        assert "春日集字 保底 10/10" in personal_progress_summary(config, capped, now)
        # 快照未携带对应键时不产生段落（历史调用方不受影响）。
        bare = {"tasks": {}, "pass_total_exp": 0, "pass_claimed_levels": set()}
        assert "保底" not in personal_progress_summary(config, bare, now)
        assert "春分" not in personal_progress_summary(config, bare, now)


    def test_center_text_renders_gameplay_progress_from_snapshot(self) -> None:
        now = datetime(2026, 10, 15, 12)
        config = {
            "template_key": "spring",
            "enabled": True,
            "name": "春日庆典",
            "start_time": "0",
            "end_time": "无限",
            "daily_tasks": [],
            "weekly_tasks": [],
            "extensions": {"activity_pass": {"enabled": False}},
            "gameplay_activities": [
                {
                    "key": "spring_collect",
                    "type": "collect_words",
                    "enabled": True,
                    "name": "春日集字",
                    "pity_threshold": 10,
                    "drop_events": ["boss"],
                },
                {
                    "key": "spring_points",
                    "type": "event_points",
                    "enabled": True,
                    "name": "春日积分",
                    "point_name": "春分",
                },
            ],
        }
        app = ActivityReadModelApplication(
            "/tmp/activity-center-missing.db",
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: now),
        )
        captured: dict = {}

        def fake_snapshot(user_id, activity_key, **kwargs):
            captured.update(kwargs)
            return {
                "sign": {"last_sign_date": "2026-10-15"},
                "tasks": {},
                "pass_total_exp": 0,
                "pass_claimed_levels": set(),
                "pass_highest_level": 0,
                "collect_pity": {("spring_collect", "boss"): 4},
                "point_balances": {"spring_points": 88},
            }

        app.repository.overview_snapshot = fake_snapshot
        text = app.activity_center_text("u-1")
        # 只读快照按玩法类型请求集字与积分键，且渲染在进度行内。
        assert captured["collect_activity_keys"] == ["spring_collect"]
        assert captured["point_activity_keys"] == ["spring_points"]
        assert "春日集字 保底 4/10" in text
        assert "春分 88" in text


    def test_center_card_offers_entries_for_advertised_gameplay(self) -> None:
        now = datetime(2026, 10, 15, 12)
        config = {
            "template_key": "spring",
            "enabled": True,
            "name": "春日庆典",
            "start_time": "0",
            "end_time": "无限",
            "daily_tasks": [],
            "weekly_tasks": [],
            "extensions": {"activity_pass": {"enabled": False}},
            "gameplay_activities": [
                {
                    "key": "spring_collect",
                    "type": "collect_words",
                    "enabled": True,
                    "name": "春日集字",
                    "start_time": "0",
                    "end_time": "无限",
                },
                {
                    "key": "spring_points",
                    "type": "event_points",
                    "enabled": True,
                    "name": "春日积分",
                    "start_time": "0",
                    "end_time": "无限",
                },
                {
                    # 与主入口同命令：不得重复出现在链接行。
                    "key": "spring_extra",
                    "type": "collect_words",
                    "enabled": True,
                    "name": "重复玩法",
                    "start_time": "0",
                    "end_time": "无限",
                    "commands": [{"label": "活动任务", "command": "活动任务"}],
                },
            ],
        }
        item = build_activity_center_items(config, now)[0]
        assert item.status == IN_PROGRESS
        labels = [label for label, _ in item.gameplay_commands]
        assert labels == ["集字背包", "集字兑换", "活动积分", "活动商店"]
        assert "活动任务" not in [command for _, command in item.gameplay_commands]

        # 卡片把它自己广告的玩法接到既有入口：链接行含玩法入口。
        app = ActivityReadModelApplication(
            "/tmp/activity-center-missing.db",
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: now),
        )
        text = app.activity_center_text()
        plain = strip_md_command_links(text)
        # 纯文本降级保留的是入口标签（命令目标只在蓝字链接里）。
        for label in ("集字背包", "集字兑换", "活动积分", "活动商店"):
            assert label in plain
        assert "mqqapi" not in plain

        # 未开始 / 已结束不给玩法入口（只给状态允许的入口）。
        assert build_activity_center_items(dict(config, start_time="2099-01-01"), now)[0].gameplay_commands == ()
        assert build_activity_center_items(dict(config, end_time="2000-01-01"), now)[0].gameplay_commands == ()

if __name__ == "__main__":
    unittest.main()
