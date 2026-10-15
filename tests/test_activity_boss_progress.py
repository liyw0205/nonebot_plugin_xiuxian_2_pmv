"""Activity boss progress: server-progress milestone view and center segment.

`活动首领` must show every 全服进度奖励 tier with the player's own state
(已领取 / 可领取 / 未解锁), and the activity center progress line must show the
boss segment so the boss journey is not the only gameplay without progress.

Written as ``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    claimable_reward_count,
    personal_progress_summary,
)
from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import (
    ActivityReadModelSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

NOW = datetime(2026, 10, 15, 12)

BOSS = {
    "key": "boss1",
    "type": "activity_boss",
    "enabled": True,
    "name": "年兽来袭",
    "boss_name": "年兽",
    "start_time": "0",
    "end_time": "无限",
    "max_hp": 1000,
    "server_milestones": [
        {"key": "m1", "hp_percent": 30, "name": "进度一", "reward": "灵石x10"},
        {"key": "m2", "hp_percent": 60, "name": "进度二", "reward": "灵石x20"},
    ],
    "rank_rewards": [{"rank_min": 1, "rank_max": 3, "name": "前三", "reward": "灵石x30"}],
}


def _config(*, boss=BOSS, enabled: bool = True) -> dict:
    config = {
        "template_key": "spring",
        "enabled": enabled,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "extensions": {},
    }
    if boss is not None:
        config["gameplay_activities"] = [boss]
    return config


def _row(**overrides) -> dict:
    row = {
        "hp_left": 500,
        "stored_max_hp": 1000,
        "damage": 120,
        "fight_count": 1,
        "inventory": {},
        "milestones_unlocked": set(),
        "milestones_claimed": set(),
    }
    row.update(overrides)
    return row


class ActivityBossProgressTests(unittest.TestCase):
    def _database(self) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_activity_state_schema(uow)
        return database

    # --- 阶段二：首领进度视图 -----------------------------------------

    def test_status_block_lists_every_milestone_state(self) -> None:
        text = ActivityReadModelApplication._status_block(
            BOSS,
            _row(milestones_unlocked={"m1"}, milestones_claimed={"m2"}),
        )
        self.assertIn("全服进度奖励：", text)
        self.assertIn("进度一（血量≤30%）灵石x10｜可领取", text)
        self.assertIn("进度二（血量≤60%）灵石x20｜已领取", text)

    def test_status_block_marks_locked_milestones(self) -> None:
        text = ActivityReadModelApplication._status_block(BOSS, _row())
        self.assertIn("进度一（血量≤30%）灵石x10｜未解锁", text)
        self.assertIn("进度二（血量≤60%）灵石x20｜未解锁", text)
        self.assertNotIn("活动首领领奖 进度", text)

    def test_status_block_hints_the_claim_command_only_when_pending(self) -> None:
        pending = ActivityReadModelApplication._status_block(
            BOSS, _row(milestones_unlocked={"m1"})
        )
        self.assertIn("可用 活动首领领奖 进度 领取已解锁的进度奖励", pending)
        settled = ActivityReadModelApplication._status_block(
            BOSS, _row(milestones_unlocked={"m1"}, milestones_claimed={"m1"})
        )
        self.assertNotIn("可用 活动首领领奖 进度", settled)

    def test_status_block_omits_the_section_without_configured_milestones(self) -> None:
        boss = dict(BOSS)
        boss.pop("server_milestones")
        text = ActivityReadModelApplication._status_block(boss, _row())
        self.assertNotIn("全服进度奖励：", text)
        self.assertIn("我的累计伤害 120", text)

    # --- 阶段一：只读快照携带领取状态 ---------------------------------

    def test_boss_snapshot_carries_milestone_sets(self) -> None:
        database = self._database()
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_boss_milestone(activity_key,milestone_key,unlocked_time) "
                "VALUES('boss1','m1','2026-10-02 00:00:00')"
            )
            uow.execute(
                "INSERT INTO activity_boss_milestone_claim(activity_key,user_id,milestone_key,create_time) "
                "VALUES('boss1','u1','m1','2026-10-02 00:00:00')"
            )
        rows = ActivityReadModelSqlRepository(database).boss_snapshot(
            "u1", ["boss1"], "2026-10-15"
        )
        self.assertEqual(rows["boss1"]["milestones_unlocked"], {"m1"})
        self.assertEqual(rows["boss1"]["milestones_claimed"], {"m1"})

    def test_boss_snapshot_without_milestone_tables_returns_empty(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE IF NOT EXISTS activity_boss_state "
                "(activity_key TEXT PRIMARY KEY,hp_left INTEGER NOT NULL,max_hp INTEGER NOT NULL,update_time TEXT DEFAULT '')"
            )
            uow.execute(
                "CREATE TABLE IF NOT EXISTS activity_boss_damage "
                "(activity_key TEXT NOT NULL,user_id TEXT NOT NULL,total_damage INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id))"
            )
            uow.execute(
                "CREATE TABLE IF NOT EXISTS activity_boss_fight_log "
                "(activity_key TEXT NOT NULL,user_id TEXT NOT NULL,fight_date TEXT DEFAULT '',source TEXT DEFAULT '')"
            )
        rows = ActivityReadModelSqlRepository(database).boss_snapshot(
            "u1", ["boss1"], "2026-10-15"
        )
        self.assertEqual(rows, {})

    def test_overview_snapshot_carries_boss_progress(self) -> None:
        database = self._database()
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_boss_state(activity_key,hp_left,max_hp) VALUES('boss1',400,1000)"
            )
            uow.execute(
                "INSERT INTO activity_boss_damage(activity_key,user_id,total_damage) VALUES('boss1','u1',250)"
            )
            uow.execute(
                "INSERT INTO activity_boss_milestone(activity_key,milestone_key,unlocked_time) "
                "VALUES('boss1','m1','2026-10-02 00:00:00')"
            )
        snapshot = ActivityReadModelSqlRepository(database).overview_snapshot(
            "u1",
            "spring",
            include_tasks=False,
            include_pass=False,
            collect_activity_keys=[],
            boss_activity_keys=["boss1"],
        )
        progress = snapshot["boss_progress"]["boss1"]
        self.assertEqual(progress["hp_left"], 400)
        self.assertEqual(progress["max_hp"], 1000)
        self.assertEqual(progress["damage"], 250)
        self.assertEqual(progress["milestones_unlocked"], {"m1"})
        self.assertEqual(progress["milestones_claimed"], set())

    def test_overview_snapshot_without_boss_keys_keeps_empty_progress(self) -> None:
        database = self._database()
        snapshot = ActivityReadModelSqlRepository(database).overview_snapshot(
            "u1",
            "spring",
            include_tasks=False,
            include_pass=False,
            collect_activity_keys=[],
        )
        self.assertEqual(snapshot["boss_progress"], {})

    # --- 阶段三：中心进度行 -------------------------------------------

    def test_center_progress_includes_the_boss_segment(self) -> None:
        config = _config()
        snapshot = {
            "boss_progress": {
                "boss1": {
                    "hp_left": 500,
                    "max_hp": 1000,
                    "damage": 120,
                    "milestones_unlocked": {"m1"},
                    "milestones_claimed": set(),
                }
            }
        }
        line = personal_progress_summary(config, snapshot, NOW)
        self.assertIn("年兽 血量 50%、我的伤害 120、进度奖励 1 项可领", line)
        self.assertIn("可领 1 项", line)

    def test_center_progress_omits_the_boss_segment_without_progress(self) -> None:
        config = _config()
        line = personal_progress_summary(config, {"boss_progress": {}}, NOW)
        self.assertNotIn("血量", line)

    def test_center_progress_boss_count_matches_claimable_reward_count(self) -> None:
        config = _config()
        snapshot = {
            "boss_progress": {
                "boss1": {
                    "hp_left": 500,
                    "max_hp": 1000,
                    "damage": 120,
                    "milestones_unlocked": {"m1", "m2"},
                    "milestones_claimed": {"m2"},
                }
            }
        }
        line = personal_progress_summary(config, snapshot, NOW)
        self.assertIn("进度奖励 1 项可领", line)
        self.assertEqual(claimable_reward_count(config, snapshot, NOW), 1)


if __name__ == "__main__":
    unittest.main()
