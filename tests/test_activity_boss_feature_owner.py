from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.boss_settlement_repository import (
    ActivityBossSettlementSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.activity.migrations import apply_activity_state_schema
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import ActivityReadModelApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class ActivityBossFeatureOwnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.database = Path(self.tempdir.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,atk INTEGER)"
            )
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name,atk) VALUES(?,?,?)",
                (("u1", "甲", 100), ("u2", "乙", 100)),
            )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    @staticmethod
    def _config() -> dict:
        return {
            "enabled": True,
            "gameplay_activities": [{
                "type": "activity_boss", "key": "boss", "enabled": True,
                "name": "测试首领活动", "boss_name": "月魔", "max_hp": 100000,
                "mode": "both", "daily_fight_limit": 3,
                "items": [{"id": "fire", "name": "爆竹", "damage_min": 10, "damage_max": 20}],
            }],
        }

    def test_read_model_status_and_rank_are_bounded_and_compatible(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO activity_boss_state VALUES('boss',10000,100000,'')")
            uow.executemany(
                "INSERT INTO activity_boss_damage(activity_key,user_id,total_damage) VALUES(?,?,?)",
                (("boss", "u1", 10000), ("boss", "u2", 20)),
            )
        application = ActivityReadModelApplication(self.database, config_loader=self._config)
        status = application.boss_status_text("u1")
        rank = application.boss_rank_text("", 1)
        self.assertIn("1万", status)
        self.assertIn("甲 伤害 1万", rank)
        self.assertNotIn("10,000", status + rank)

    def test_settlement_replay_conflict_and_cas(self) -> None:
        repository = ActivityBossSettlementSqlRepository(self.database)
        kwargs = {
            "operation_id": "boss-op-1", "user_id": "u1", "activity_key": "boss",
            "expected_hp": 1000, "expected_max_hp": 1000, "expected_fight_count": 0,
            "daily_limit": 3, "fixed_damage": 100, "fight_date": "2026-10-06",
            "timestamp": "2026-10-06 12:00:00", "milestones": (),
        }
        first = repository.settle_cooperative(**kwargs)
        duplicate = repository.settle_cooperative(**kwargs)
        conflict = repository.settle_cooperative(**{**kwargs, "fixed_damage": 101})
        changed = repository.settle_cooperative(**{**kwargs, "operation_id": "boss-op-2", "expected_hp": 1000})
        self.assertEqual("applied", first.status)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual("state_changed", changed.status)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(900, uow.query_one("SELECT hp_left FROM activity_boss_state")["hp_left"])
            self.assertEqual(1, uow.query_one("SELECT COUNT(*) AS count FROM activity_boss_fight_log")["count"])

    def test_settlement_failure_rolls_back_all_state(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TRIGGER fail_boss_receipt BEFORE INSERT ON activity_boss_settlement_operations "
                "BEGIN SELECT RAISE(ABORT, 'receipt failure'); END"
            )
        repository = ActivityBossSettlementSqlRepository(self.database)
        with self.assertRaisesRegex(Exception, "receipt failure"):
            repository.settle_cooperative(
                operation_id="boss-op-fail", user_id="u1", activity_key="boss",
                expected_hp=1000, expected_max_hp=1000, expected_fight_count=0,
                daily_limit=3, fixed_damage=100, fight_date="2026-10-06",
                timestamp="2026-10-06 12:00:00", milestones=(),
            )
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertIsNone(uow.query_one("SELECT * FROM activity_boss_state"))
            self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS count FROM activity_boss_fight_log")["count"])


if __name__ == "__main__":
    unittest.main()
