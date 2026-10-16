"""Activity stage feature: config, timeline, participation, claim, help.

Written as ``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.stage import (
    ENDED,
    IN_PROGRESS,
    NOT_STARTED,
    claimable_stage_rows,
    stage_remaining_text,
    stage_reward_rows,
    stage_status,
    stage_timeline,
    stage_window_bounds,
)
from nonebot_plugin_xiuxian_2.features.activity.stage_claim_application import (
    ActivityStageClaimApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.stage_claim_repository import (
    ActivityStageClaimRepository,
)
from nonebot_plugin_xiuxian_2.features.activity.stage_participation import (
    ActivityStageParticipationRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config import (
    _activity_stages_config,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
    strip_md_command_links,
)

NOW = datetime(2026, 10, 15, 12)
ROOT = Path(__file__).resolve().parents[1]
ACTIVITY_MODULE = (
    ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_activity" / "__init__.py"
)

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
# 开放「领奖」的阶段：领取类用例必须让当前阶段开放 claim，
# 否则阶段门禁会先于业务校验拒绝（与旧路径同一语义）。
CLAIM_STAGE = {
    "key": "claim_live",
    "name": "领奖期",
    "stage_type": "settlement",
    "start_time": "0",
    "end_time": "无限",
    "features": ["claim"],
    "multiplier": 1.0,
}
NEXT_STAGE = {
    "key": "settle",
    "name": "结算期",
    "stage_type": "settlement",
    "start_time": "2026-12-01",
    "end_time": "2026-12-31",
    "features": ["claim"],
    "multiplier": 1.0,
}


def _config(stages, *, enabled: bool = True) -> dict:
    return {
        "template_key": "spring",
        "enabled": enabled,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "extensions": {"stages": stages},
    }


class ActivityStageTests(unittest.TestCase):
    def _database(self) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_activity_state_schema(uow)
        return database

    # --- 阶段一：配置与数据 ---------------------------------------------

    def test_stage_rewards_are_normalized(self) -> None:
        stages = _activity_stages_config(
            _config(
                [
                    {
                        "key": "open",
                        "rewards": [
                            {"key": "r1", "name": "阶段礼", "reward": "灵石x100"},
                            {"reward": ""},
                            "灵石x50",
                            {"key": "r1", "reward": "重复键"},
                            {"name": "无名", "reward": "灵石x10"},
                        ],
                    }
                ]
            )
        )
        rewards = stages[0]["rewards"]
        self.assertEqual(
            rewards,
            [
                {"key": "r1", "name": "阶段礼", "reward": "灵石x100"},
                {"key": "reward_3", "name": "", "reward": "灵石x50"},
                {"key": "reward_5", "name": "无名", "reward": "灵石x10"},
            ],
        )
        # 未配置 rewards 时保持向后兼容。
        self.assertEqual(
            _activity_stages_config(_config([{"key": "open"}]))[0]["rewards"], []
        )
        self.assertEqual(stage_reward_rows({"rewards": "bad"}), [])

    def test_migration_creates_stage_claim_table_idempotently(self) -> None:
        database = self._database()
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_activity_state_schema(uow)  # 重复 apply 幂等
            tables = {
                str(row["name"])
                for row in uow.query_all(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertIn("activity_stage_claim", tables)

    # --- 阶段二：阶段时间线 ---------------------------------------------

    def test_timeline_resolves_status_remaining_and_picks(self) -> None:
        self.assertEqual(stage_status(ENDED_STAGE, NOW), ENDED)
        self.assertEqual(stage_status(ACTIVE_STAGE, NOW), IN_PROGRESS)
        self.assertEqual(stage_status(NEXT_STAGE, NOW), NOT_STARTED)
        self.assertEqual(stage_remaining_text(ENDED_STAGE, NOW), "")
        self.assertEqual(stage_remaining_text(ACTIVE_STAGE, NOW), "")
        self.assertTrue(stage_remaining_text(NEXT_STAGE, NOW).endswith("后开启"))
        self.assertEqual(stage_window_bounds(ENDED_STAGE),
                         ("2026-09-01 00:00:00", "2026-10-01 23:59:59"))

        timeline = stage_timeline(_config([ENDED_STAGE, ACTIVE_STAGE, NEXT_STAGE]), NOW)
        self.assertEqual(timeline["active"]["key"], "live")
        self.assertEqual([s["key"] for s in timeline["ended"]], ["open"])
        self.assertEqual(timeline["next"]["key"], "settle")
        # 无阶段配置时全部为空。
        empty = stage_timeline({"extensions": {"stages": []}}, NOW)
        self.assertIsNone(empty["active"])
        self.assertEqual(empty["ended"], [])

    # --- 阶段三：参与证据 -----------------------------------------------

    def test_participation_uses_existing_logs_within_the_window(self) -> None:
        database = self._database()
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('u1','2026-09-15','2026-09-15 10:00:00')"
            )
            uow.execute(
                "INSERT INTO activity_point_event_log"
                "(activity_key,user_id,event_key,points,record_date,create_time) "
                "VALUES('spring','u1','work',10,'2026-10-05','2026-10-05 09:00:00')"
            )
        repository = ActivityStageParticipationRepository(database)
        windows = [
            ("open", *stage_window_bounds(ENDED_STAGE)),
            ("live", "2026-10-02 00:00:00", "2026-10-20 23:59:59"),
        ]
        # 签到记录落在 open 窗口，积分记录落在 live 窗口。
        self.assertEqual(
            repository.participated_stages("u1", "spring", windows), {"open", "live"}
        )
        # 只保留 open 窗口时，live 的积分记录不再计入。
        self.assertEqual(
            repository.participated_stages("u1", "spring", [windows[0]]), {"open"}
        )
        # 窗口外的时间戳不算参与。
        self.assertEqual(
            repository.participated_stages(
                "u1", "spring", [("x", "2020-01-01 00:00:00", "2020-12-31 23:59:59")]
            ),
            set(),
        )
        # 缺失日志表不影响判定。
        self.assertEqual(
            ActivityStageParticipationRepository("/tmp/no-such-activity.db")
            .participated_stages("u1", "spring", windows),
            set(),
        )

    # --- 阶段四：结算与领取 ---------------------------------------------

    def test_claim_delivers_once_and_repeat_is_a_noop(self) -> None:
        database = self._database()
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('u1','2026-09-15','2026-09-15 10:00:00')"
            )
        config = _config([ENDED_STAGE, CLAIM_STAGE])
        sent: list[tuple[str, str]] = []
        app = ActivityStageClaimApplication(
            database,
            clock=SimpleNamespace(now=lambda: NOW),
            config_loader=lambda: config,
            reward_sender=lambda uid, text: sent.append((uid, text)) or ["发放成功"],
        )
        ok, text = app.claim("u1")
        self.assertTrue(ok, text)
        self.assertIn("活动阶段奖励领取成功", text)
        self.assertIn("攻坚期", text)
        self.assertEqual(sent, [("u1", "灵石x100")])
        self.assertEqual(
            ActivityStageClaimRepository(database).claimed_stages("u1", "spring"),
            {"open"},
        )
        # 重复领取不重复发放。
        ok, text = app.claim("u1")
        self.assertFalse(ok)
        self.assertEqual(text, "阶段奖励已领取")
        self.assertEqual(len(sent), 1)

    def test_claim_failure_boundaries(self) -> None:
        database = self._database()
        config = _config([ENDED_STAGE, CLAIM_STAGE])

        def app_for(cfg, *, sender=None):
            return ActivityStageClaimApplication(
                database,
                clock=SimpleNamespace(now=lambda: NOW),
                config_loader=lambda: cfg,
                reward_sender=sender or (lambda uid, text: ["ok"]),
            )

        # 活动未开启。
        ok, text = app_for(_config([ENDED_STAGE], enabled=False)).claim("u1")
        self.assertFalse(ok)
        self.assertIn("活动未开启", text)
        # 没有已结束阶段。
        ok, text = app_for(_config([CLAIM_STAGE])).claim("u1")
        self.assertFalse(ok)
        self.assertEqual(text, "当前没有已结束的活动阶段")
        # 已结束阶段没有配置奖励。
        ok, text = app_for(_config([dict(ENDED_STAGE, rewards=[]), CLAIM_STAGE])).claim("u1")
        self.assertFalse(ok)
        self.assertEqual(text, "已结束阶段没有配置奖励")
        # 有奖励但本人没有参与记录。
        ok, text = app_for(config).claim("u1")
        self.assertFalse(ok)
        self.assertEqual(text, "你在已结束阶段没有参与记录")
        # 指定阶段名不匹配。
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('u1','2026-09-15','2026-09-15 10:00:00')"
            )
        ok, text = app_for(config).claim("u1", query="不存在的阶段")
        self.assertFalse(ok)
        self.assertIn("没有可领取的阶段奖励", text)

    # --- 阶段五：帮助与呈现 ---------------------------------------------

    def test_help_documents_stage_commands(self) -> None:
        source = ACTIVITY_MODULE.read_text(encoding="utf-8")
        start = source.index('ACTIVITY_HELP = """') + len('ACTIVITY_HELP = """')
        help_text = source[start : source.index('"""', start)]
        self.assertIn("活动阶段", help_text)
        self.assertIn("活动阶段领取", help_text)

    def test_stage_view_reports_current_stage_and_claimable(self) -> None:
        database = self._database()
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('u1','2026-09-15','2026-09-15 10:00:00')"
            )
        config = _config([ENDED_STAGE, ACTIVE_STAGE, NEXT_STAGE])
        app = ActivityReadModelApplication(
            database,
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: NOW),
        )
        text = app.activity_stage_text("u1")
        self.assertIn("正式期", text)
        self.assertIn("开放内容：签到、任务", text)
        self.assertIn("阶段倍率：1.00x", text)
        self.assertIn("下一阶段：结算期", text)
        self.assertIn("可领阶段奖励：攻坚期｜活动阶段领取", text)
        # 纯文本降级不泄漏 markdown 或蓝字 URL。
        plain = strip_md_command_links(text)
        self.assertNotIn("mqqapi", plain)
        self.assertNotIn("**", plain)
        # 活动未开启时给原因。
        closed = ActivityReadModelApplication(
            database,
            config_loader=lambda: _config([ACTIVE_STAGE], enabled=False),
            clock=SimpleNamespace(now=lambda: NOW),
        )
        self.assertIn("活动未开启", closed.activity_stage_text())


if __name__ == "__main__":
    unittest.main()
