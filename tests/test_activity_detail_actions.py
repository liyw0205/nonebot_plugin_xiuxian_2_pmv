"""Detail-view action summary: same claim sources as the center card.

The detail view (``activity_info_text``) must surface the same action tips as
the center card for the same player, covering every source the one-click claim
covers. Written as ``unittest.TestCase`` because the CI quality job collects
tests with ``python -m unittest discover``.
"""

from __future__ import annotations

from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    claimable_reward_count,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)


NOW = datetime(2026, 10, 15, 12)
ACTIVITY_KEY = "spring"

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

COLLECT = {
    "key": "collect",
    "type": "collect_words",
    "enabled": True,
    "name": "集字活动",
    "start_time": "0",
    "end_time": "无限",
    "pity_threshold": 5,
    "drop_events": ["sign"],
}

PASS_CONFIG = {
    "enabled": True,
    "level_exp": 100,
    "max_level": 12,
    "level_rewards": [{"level": 1, "name": "一阶", "reward": "灵石x10"}],
}


def _config(**overrides) -> dict:
    extensions = {
        "activity_pass": {"enabled": False},
        "activity_info_mode": "full",
    }
    extensions.update(overrides.pop("extensions", {}))
    config = {
        "template_key": ACTIVITY_KEY,
        "enabled": True,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "daily_tasks": [],
        "weekly_tasks": [],
        "extensions": extensions,
    }
    config.update(overrides)
    return config


class ActivityDetailActionTipsTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_activity_state_schema(uow)

    def _application(self, config: dict) -> ActivityReadModelApplication:
        return ActivityReadModelApplication(
            self.database,
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: NOW),
        )

    @staticmethod
    def _seed(uow: DatabaseUnitOfWork, rows: list[tuple[str, tuple]]) -> None:
        for sql, params in rows:
            uow.execute(sql, params)

    def test_detail_action_summary_covers_each_claim_source(self) -> None:
        cases = [
            (
                _config(extensions={"stages": [STAGE]}),
                [(
                    "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                    "VALUES('u1','2026-09-20','2026-09-20 10:00:00')",
                    (),
                )],
                "阶段奖励可领：活动阶段领取",
            ),
            (
                _config(extensions={"rank": RANK_CONFIG}),
                [(
                    "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) "
                    "VALUES('spring','u1',50,50)",
                    (),
                )],
                "排行奖励可领：活动排行领取",
            ),
            (
                _config(gameplay_activities=[BOSS]),
                [(
                    "INSERT INTO activity_boss_milestone(activity_key,milestone_key) "
                    "VALUES('boss','m1')",
                    (),
                )],
                "首领进度奖励可领：活动首领领奖 进度",
            ),
            (
                _config(gameplay_activities=[BOSS]),
                [(
                    "INSERT INTO activity_boss_damage(activity_key,user_id,total_damage,update_time) "
                    "VALUES('boss','u1',500,'2026-10-15 10:00:00')",
                    (),
                )],
                "首领排行奖励可领：活动首领领奖 排行",
            ),
        ]
        for config, rows, expected in cases:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                self._seed(uow, rows)
            application = self._application(config)
            text = application.activity_info_text("u1")
            self.assertIn("【行动建议】", text, config)
            self.assertIn(expected, text, config)

    def test_detail_and_center_agree_on_one_snapshot(self) -> None:
        config = _config(
            extensions={
                "stages": [STAGE],
                "rank": RANK_CONFIG,
                "activity_pass": PASS_CONFIG,
            },
            gameplay_activities=[BOSS, COLLECT],
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._seed(uow, [
                (
                    "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                    "VALUES('u1','2026-09-20','2026-09-20 10:00:00')",
                    (),
                ),
                (
                    "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) "
                    "VALUES('spring','u1',50,50)",
                    (),
                ),
                (
                    "INSERT INTO activity_boss_milestone(activity_key,milestone_key) "
                    "VALUES('boss','m1')",
                    (),
                ),
                (
                    "INSERT INTO activity_pass_balance(activity_key,user_id,level,total_exp) "
                    "VALUES('spring','u1',1,100)",
                    (),
                ),
            ])
        application = self._application(config)
        snapshot = application.repository.overview_snapshot(
            "u1", ACTIVITY_KEY, include_tasks=True, include_pass=True,
            collect_activity_keys=[],
            stage_windows=[("closed", "2026-09-01 00:00:00", "2026-10-01 23:59:59")],
            board_sources=["points", "boss"],
            boss_activity_keys=["boss"],
        )
        self.assertGreater(claimable_reward_count(config, snapshot, NOW), 0)
        detail = application.activity_info_text("u1")
        center = application.activity_center_text("u1")
        # The detail view states each source as a tip; the center card maps the
        # same sources to their claim commands on one link row.
        self.assertIn("阶段奖励可领：活动阶段领取", detail)
        self.assertIn("排行奖励可领：活动排行领取", detail)
        self.assertIn("活动阶段领取", center)
        self.assertIn("活动排行领取", center)

    def test_boss_rank_tip_uses_the_boss_activity_key(self) -> None:
        """Boss damage tables are keyed by the boss activity's own key; the
        boss board position must not read the main activity key (0 always)."""
        config = _config(gameplay_activities=[dict(BOSS, key="festival_world_boss")])
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._seed(uow, [(
                "INSERT INTO activity_boss_damage(activity_key,user_id,total_damage,update_time) "
                "VALUES('festival_world_boss','u1',500,'2026-10-15 10:00:00')",
                (),
            )])
        application = self._application(config)
        text = application.activity_info_text("u1")
        self.assertIn("首领排行奖励可领：活动首领领奖 排行", text)
        center = application.activity_center_text("u1")
        self.assertIn("活动首领领奖 排行", center)

    def test_detail_without_user_or_claim_state_keeps_old_output(self) -> None:
        config = _config(
            extensions={"stages": [STAGE], "rank": RANK_CONFIG},
            gameplay_activities=[BOSS],
        )
        application = self._application(config)
        public = application.activity_info_text(None)
        self.assertNotIn("【行动建议】", public)
        self.assertNotIn("阶段奖励可领", public)
        empty = application.activity_info_text("u1")
        self.assertNotIn("阶段奖励可领", empty)
        self.assertNotIn("排行奖励可领", empty)
        self.assertNotIn("首领进度奖励可领", empty)


if __name__ == "__main__":
    unittest.main()
