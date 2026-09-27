from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_sect_weekly, apply_sect_weekly_player
from ..weekly_reward_repository import SectWeeklyRewardSqlRepository


class _Clock:
    def now(self):
        return datetime(2026, 7, 20, 12, 30, 0)


class SectWeeklyRewardRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,stone INTEGER,exp INTEGER,sect_contribution INTEGER)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('u',1,10,20,30)")
            uow.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_scale INTEGER,sect_materials INTEGER)")
            uow.execute("INSERT INTO sects VALUES(1,100,200)")
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            uow.execute(
                "CREATE TABLE sect_weekly_goal(sect_id INTEGER,week_key TEXT,goal_key TEXT,progress INTEGER,target INTEGER,participants TEXT,updated_at TEXT,PRIMARY KEY(sect_id,week_key,goal_key))"
            )
            uow.executemany(
                "INSERT INTO sect_weekly_goal VALUES(1,'2026-W29',?,?,?,'{}','')",
                (("g1", 10, 10), ("g2", 20, 20), ("pending", 1, 5)),
            )
            apply_sect_weekly(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY)")
            uow.execute("INSERT INTO boss_limit VALUES('u')")
            apply_sect_weekly_player(uow)
        self.repository = SectWeeklyRewardSqlRepository(self.game, self.player, clock=_Clock())
        self.goals = [
            {
                "key": "g1",
                "name": "目标一",
                "target": 10,
                "rewards": {
                    "stone": 5,
                    "exp": 6,
                    "sect_contribution": 7,
                    "sect_scale": 8,
                    "sect_materials": 9,
                    "boss_integral": 10,
                    "items": [{"id": 101, "name": "周常令", "type": "道具", "amount": 2}],
                },
            },
            {
                "key": "g2",
                "name": "目标二",
                "target": 20,
                "rewards": {
                    "stone": 11,
                    "sect_contribution": 12,
                    "sect_scale": 13,
                    "sect_materials": 14,
                    "boss_integral": 15,
                    "items": [{"id": 101, "name": "周常令", "type": "道具", "amount": 3}],
                },
            },
        ]

    def test_startup_migrations_preserve_weekly_progress_and_existing_boss_limit_users(self):
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            row = uow.query_one("SELECT progress,target,claimed_users FROM sect_weekly_goal WHERE goal_key='g1'")
            self.assertEqual((10, 10, "[]"), tuple(row.values()))
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(0, uow.query_one("SELECT integral FROM boss_limit WHERE user_id='u'")["integral"])

    def tearDown(self):
        self.tmp.cleanup()

    def claim(self, operation_id="op", **changes):
        args = dict(
            operation_id=operation_id,
            user_id="u",
            sect_id=1,
            week_key="2026-W29",
            goals=self.goals,
            max_goods_num=100,
        )
        args.update(changes)
        return self.repository.claim(**args)

    def test_batch_claim_updates_game_and_player_state_and_replays(self):
        result = self.claim()
        self.assertEqual("applied", result.status)
        self.assertEqual("duplicate", self.claim().status)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            user = uow.query_one("SELECT stone,exp,sect_contribution FROM user_xiuxian WHERE user_id='u'")
            sect = uow.query_one("SELECT sect_scale,sect_materials FROM sects WHERE sect_id=1")
            self.assertEqual((26, 26, 49), tuple(user.values()))
            self.assertEqual((121, 223), tuple(sect.values()))
            self.assertEqual(5, uow.query_one("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=101")["goods_num"])
            self.assertEqual(
                ['["u"]', '["u"]'],
                [row["claimed_users"] for row in uow.query_all("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key IN ('g1','g2') ORDER BY goal_key")],
            )
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(25, uow.query_one("SELECT integral FROM boss_limit WHERE user_id='u'")["integral"])

    def test_rejects_changed_state_and_capacity_without_partial_updates(self):
        self.assertEqual("not_completed", self.claim(goals=[{**self.goals[0], "key": "pending", "target": 5}]).status)
        self.assertEqual("not_completed", self.claim(week_key="2026-W28").status)
        self.assertEqual("inventory_full", self.claim(max_goods_num=4).status)
        self.assertEqual("applied", self.claim().status)
        self.assertEqual("operation_conflict", self.claim(goals=self.goals[:1]).status)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(1, uow.query_one("SELECT COUNT(*) AS n FROM sect_weekly_reward_operations")["n"])

    def test_rechecks_membership_and_sect_existence(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE user_xiuxian SET sect_id=2 WHERE user_id='u'")
        self.assertEqual("sect_changed", self.claim().status)
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE user_xiuxian SET sect_id=1 WHERE user_id='u'")
            uow.execute("DELETE FROM sects WHERE sect_id=1")
        self.assertEqual("sect_missing", self.claim().status)

    def test_operation_insert_failure_rolls_back_both_databases(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TRIGGER fail_weekly_operation BEFORE INSERT ON sect_weekly_reward_operations "
                "BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )
        with self.assertRaises(sqlite3.DatabaseError):
            self.claim()
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual((10, 20, 30), tuple(uow.query_one("SELECT stone,exp,sect_contribution FROM user_xiuxian WHERE user_id='u'").values()))
            self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM back")["n"])
            self.assertEqual('[]', uow.query_one("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key='g1'")["claimed_users"])
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(0, uow.query_one("SELECT integral FROM boss_limit WHERE user_id='u'")["integral"])

    def test_missing_startup_schema_is_rejected_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as directory:
            game, player = Path(directory) / "game.db", Path(directory) / "player.db"
            game.touch()
            player.touch()
            repository = SectWeeklyRewardSqlRepository(game, player)
            self.assertEqual("schema_missing", repository.claim("op", "u", 1, "2026-W29", self.goals, 100).status)
            with sqlite3.connect(game) as conn:
                self.assertIsNone(conn.execute("SELECT 1 FROM sqlite_master WHERE name='sect_weekly_reward_operations'").fetchone())
            with sqlite3.connect(player) as conn:
                self.assertIsNone(conn.execute("SELECT 1 FROM sqlite_master WHERE name='boss_limit'").fetchone())


if __name__ == "__main__":
    unittest.main()
