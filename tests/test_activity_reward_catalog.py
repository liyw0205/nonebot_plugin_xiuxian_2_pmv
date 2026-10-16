"""活动奖励目录：六来源档位 + 本人状态（只读）。

`rewards_text()` 必须与旧 `build_activity_rewards_text()` 逐字相等（配置口径），
个人状态因此走新增的 `reward_catalog_text(user_id)`。
CI 只跑 `unittest discover`，所以用 `unittest.TestCase`。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_event_receipts,
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config, service

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
ACTIVE_STAGE = {
    "key": "live",
    "name": "正式期",
    "stage_type": "open",
    "start_time": "0",
    "end_time": "无限",
    "features": ["sign", "task"],
    "multiplier": 1.0,
}
RANK = {
    "boards": [{"key": "sign", "label": "签到榜", "source": "sign", "limit": 10}],
    "rewards": [
        {
            "board": "sign",
            "rank_min": 1,
            "rank_max": 1,
            "key": "champion",
            "name": "榜首",
            "reward": "灵石x1000",
        }
    ],
}


class ActivityRewardCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        self.config = activity_config._migrate_config(
            activity_config._load_default_config()
        )[0]
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            apply_activity_event_receipts(uow)
            uow.execute(
                "INSERT INTO activity_user(user_id,sign_days,total_sign_days,last_sign_date) "
                "VALUES('user-1',4,9,'2026-10-05')"
            )
            self.activity_key = activity_config._activity_config_key(self.config)
            task = service.get_activity_tasks(self.config, "daily")[0]
            self.first_task = task
            uow.execute(
                "INSERT INTO activity_task_progress(activity_key,user_id,scope_type,scope_key,"
                "task_key,progress,target,claimed) VALUES(?,?,?,?,?,?,?,0)",
                (
                    self.activity_key,
                    "user-1",
                    "daily",
                    service._task_scope_key("daily"),
                    task["key"],
                    task["target"],
                    task["target"],
                ),
            )
            uow.execute(
                "INSERT INTO activity_pass_balance(activity_key,user_id,exp,total_exp,level) "
                "VALUES(?,?,0,100,1)",
                (self.activity_key, "user-1"),
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _app(self, config=None) -> ActivityReadModelApplication:
        return ActivityReadModelApplication(
            self.database, config_loader=lambda: config or self.config
        )

    # --- 阶段二：目录渲染 -------------------------------------------------

    def test_catalog_renders_every_configured_source(self) -> None:
        text = self._app().reward_catalog_text("user-1")
        self.assertIn("【节日活动季 · 奖励】", text)
        for section in (
            "【每日签到奖励】",
            "【累计签到奖励】",
            "【每日任务奖励】",
            "【周常任务奖励】",
            "【战令奖励】",
            "【庆典全服首领 · 进度奖励】",
            "【庆典全服首领 · 排行奖励】",
        ):
            self.assertIn(section, text)
        self.assertIn("我的累计签到 9 天，已达成 2/2 档", text)

    def test_catalog_marks_task_and_pass_states(self) -> None:
        text = self._app().reward_catalog_text("user-1")
        # 进度已达标的每日任务可领取；未达标的标未达成。
        self.assertIn("（可领取）", text)
        self.assertIn("（未达成）", text)
        self.assertIn("- Lv1 初入庆典：灵石x80000（可领取）", text)
        self.assertIn("- Lv2 勤修补给：灵石x120000（未达成）", text)
        # 首领进度档位在未解锁时标未达成。
        self.assertIn("初战告捷（血量≤90.0%）：灵石x150000（未达成）", text)

    def test_catalog_marks_claimed_pass_level(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO activity_pass_reward_claim(activity_key,user_id,level) "
                "VALUES(?,?,1)",
                (self.activity_key, "user-1"),
            )
        text = self._app().reward_catalog_text("user-1")
        self.assertIn("- Lv1 初入庆典：灵石x80000（已领取）", text)
        self.assertIn("- Lv2 勤修补给：灵石x120000（未达成）", text)

    def test_catalog_omits_unconfigured_sources(self) -> None:
        # 默认配置没有阶段奖励与排行奖励：不渲染空段。
        text = self._app().reward_catalog_text("user-1")
        self.assertNotIn("【阶段奖励】", text)
        self.assertNotIn("【排行奖励】", text)

    def test_catalog_without_user_has_no_personal_state(self) -> None:
        text = self._app().reward_catalog_text()
        self.assertNotIn("（可领取）", text)
        self.assertNotIn("（未达成）", text)
        self.assertNotIn("（已领取）", text)
        self.assertNotIn("我的累计签到", text)
        self.assertNotIn("我的名次", text)
        # 配置档位仍然完整。
        self.assertIn("- 第1天 首日签到：灵石x50000", text)
        self.assertIn("- Lv1 初入庆典：灵石x80000", text)

    def test_catalog_renders_stage_rewards_with_state(self) -> None:
        config = dict(self.config)
        config["extensions"] = dict(config.get("extensions") or {})
        config["extensions"]["stages"] = [ENDED_STAGE, ACTIVE_STAGE]
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('user-1','2026-09-15','2026-09-15 10:00:00')"
            )
        text = self._app(config).reward_catalog_text("user-1")
        self.assertIn("【阶段奖励】", text)
        self.assertIn("- 攻坚期 阶段礼：灵石x100（可领取）", text)

    def test_catalog_marks_claimed_stage_reward(self) -> None:
        config = dict(self.config)
        config["extensions"] = dict(config.get("extensions") or {})
        config["extensions"]["stages"] = [ENDED_STAGE, ACTIVE_STAGE]
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('user-1','2026-09-15','2026-09-15 10:00:00')"
            )
            uow.execute(
                "INSERT INTO activity_stage_claim(activity_key,user_id,stage_key,reward,"
                "create_time) VALUES(?,?,?,?,?)",
                (self.activity_key, "user-1", "open", "灵石x100", "2026-10-02 10:00:00"),
            )
        text = self._app(config).reward_catalog_text("user-1")
        self.assertIn("- 攻坚期 阶段礼：灵石x100（已领取）", text)

    def test_catalog_renders_rank_rewards_with_position(self) -> None:
        config = dict(self.config)
        config["extensions"] = dict(config.get("extensions") or {})
        config["extensions"]["rank"] = RANK
        text = self._app(config).reward_catalog_text("user-1")
        self.assertIn("【排行奖励】", text)
        self.assertIn("- 签到榜 第1-1名：灵石x1000（可领取）", text)
        self.assertIn("我的名次：第 1 名", text)

    # --- 阶段三：旧文案不受影响 -------------------------------------------

    def test_rewards_text_stays_config_only(self) -> None:
        text = self._app().rewards_text()
        self.assertIn("【每日签到奖励】", text)
        self.assertIn("【累计签到奖励】", text)
        self.assertNotIn("【战令奖励】", text)
        self.assertNotIn("（可领取）", text)
        self.assertNotIn("我的累计签到", text)


if __name__ == "__main__":
    unittest.main()
