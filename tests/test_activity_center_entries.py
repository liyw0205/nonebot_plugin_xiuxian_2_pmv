"""Lifecycle-specific command entries on the activity center card."""

from __future__ import annotations

import unittest
from datetime import datetime

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    ENDED,
    IN_PROGRESS,
    NOT_STARTED,
    REWARD_CLAIM,
    build_activity_center_items,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config import (
    activity_rank_config,
)

NOW = datetime(2026, 10, 15, 12)


def _config(*, start_time: str = "2026-10-01", end_time: str = "2026-10-31") -> dict:
    return {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "start_time": start_time,
        "end_time": end_time,
        "daily_tasks": [],
        "weekly_tasks": [],
        "extensions": {
            "stages": [
                {"key": "opening", "name": "开启期", "start_time": "0", "end_time": "无限"}
            ],
            "rank": {"boards": [{"key": "points", "label": "积分榜", "source": "points"}]},
            "activity_pass": {"enabled": False},
        },
        "gameplay_activities": [],
    }


class ActivityCenterEntryTests(unittest.TestCase):
    def test_in_progress_appends_configured_stage_and_rank_entries(self) -> None:
        item = build_activity_center_items(_config(), NOW)[0]

        self.assertEqual(item.status, IN_PROGRESS)
        self.assertEqual(
            item.commands,
            (
                ("活动签到", "活动签到"),
                ("活动任务", "活动任务"),
                ("活动战令", "活动战令"),
                ("活动阶段", "活动阶段"),
                ("活动排行", "活动排行"),
            ),
        )

    def test_not_started_and_ended_cards_do_not_show_source_entries(self) -> None:
        not_started = build_activity_center_items(
            _config(start_time="2026-10-20"), NOW
        )[0]
        ended = build_activity_center_items(
            _config(end_time="2026-10-10"), NOW
        )[0]

        self.assertEqual(not_started.status, NOT_STARTED)
        self.assertEqual(not_started.commands, (("查看详情", "活动信息"),))
        self.assertEqual(ended.status, ENDED)
        self.assertEqual(ended.commands, ())

    def test_reward_claim_keeps_one_click_claim_first_and_adds_source_claims(self) -> None:
        item = build_activity_center_items(_config(), NOW, claimable=True)[0]

        self.assertEqual(item.status, REWARD_CLAIM)
        self.assertEqual(item.commands[0], ("领取奖励", "活动领取"))
        self.assertEqual(
            item.commands,
            (
                ("领取奖励", "活动领取"),
                ("活动任务", "活动任务"),
                ("活动阶段领取", "活动阶段领取"),
                ("活动排行领取", "活动排行领取"),
            ),
        )

    def test_explicitly_empty_stage_and_rank_configuration_has_no_entries(self) -> None:
        config = _config()
        config["extensions"]["stages"] = []
        config["extensions"]["rank"]["boards"] = []

        self.assertEqual(activity_rank_config(config)["boards"], [])
        item = build_activity_center_items(config, NOW)[0]
        targets = {command for _, command in item.commands}
        self.assertNotIn("活动阶段", targets)
        self.assertNotIn("活动排行", targets)

    def test_missing_rank_configuration_keeps_legacy_default_sign_board(self) -> None:
        config = _config()
        del config["extensions"]["rank"]

        item = build_activity_center_items(config, NOW)[0]

        self.assertIn(("活动排行", "活动排行"), item.commands)

    def test_gameplay_entries_remain_deduplicated_against_card_sources(self) -> None:
        config = _config()
        config["gameplay_activities"] = [
            {
                "key": "points-one",
                "type": "event_points",
                "enabled": True,
                "commands": [
                    {"label": "活动排行重复", "command": "活动排行"},
                    {"label": "积分查看", "command": "我的活动积分"},
                ],
            },
            {
                "key": "points-two",
                "type": "event_points",
                "enabled": True,
                "commands": [
                    {"label": "积分查看重复", "command": "我的活动积分"},
                ],
            },
        ]

        item = build_activity_center_items(config, NOW)[0]

        self.assertIn(("活动排行", "活动排行"), item.commands)
        self.assertEqual(item.gameplay_commands, (("积分查看", "我的活动积分"),))


if __name__ == "__main__":
    unittest.main()
