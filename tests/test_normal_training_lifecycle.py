from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.normal_training_lifecycle_service import NormalTrainingLifecycleService
from tests.test_db_backend import db_backend


class NormalTrainingLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.executemany("INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,%s,%s,%s)", [
                ("cultivator", 100, 50, 1, 2, 3, 4), ("mortal", 0, 20, 0, 0, 0, 0),
            ])
            conn.executemany("INSERT INTO user_cd VALUES(%s,0,0,NULL)", [("cultivator",), ("mortal",)])
        self.service = NormalTrainingLifecycleService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def test_cultivation_lifecycle_is_atomic_and_idempotent(self):
        started = self.service.start(
            "op-c", "cultivator", kind="cultivation", expected_exp=100, expected_stone=50,
            reward=30, exp_cap=120, power_multiplier=2, now=datetime(2026, 7, 13, 12),
        )
        self.assertEqual("started", started.status)
        first = self.service.complete("op-c", task_period="2026-W29")
        second = self.service.complete("op-c", task_period="2026-W29")
        self.assertEqual(("applied", 20), (first.status, first.exp_gain))
        self.assertEqual("duplicate", second.status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual((120, 11, 7, 10, 240), tuple(conn.execute("SELECT exp,hp,mp,atk,power FROM user_xiuxian WHERE user_id='cultivator'").fetchone()))
            self.assertEqual((0, "0"), tuple(conn.execute("SELECT type,create_time FROM user_cd WHERE user_id='cultivator'").fetchone()))
        with db_backend.connection(self.player) as conn:
            self.assertEqual((1, 20), tuple(conn.execute('SELECT "修炼次数","修炼修为" FROM statistics WHERE user_id=\'cultivator\'').fetchone()))
            progress = json.loads(conn.execute("SELECT weekly_progress FROM xiuxian_tasks WHERE user_id='cultivator'").fetchone()[0])
            self.assertEqual(1, progress["weekly_out_closing"])

    def test_mining_and_state_change(self):
        self.service.start("op-m", "mortal", kind="mining", expected_exp=0, expected_stone=20,
                           reward=500, exp_cap=0, power_multiplier=1, now=datetime(2026, 7, 13, 12))
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_cd SET type=0 WHERE user_id='mortal'")
        self.assertEqual("state_changed", self.service.complete("op-m", task_period="2026-W29").status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_cd SET type=5,create_time=%s WHERE user_id='mortal'", ("2026-07-13 12:00:00.000000",))
        self.assertEqual("applied", self.service.complete("op-m", task_period="2026-W29").status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(520, conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='mortal'").fetchone()[0])

    def test_completion_failure_rolls_back_everything(self):
        self.service.start("op-f", "cultivator", kind="cultivation", expected_exp=100, expected_stone=50,
                           reward=10, exp_cap=200, power_multiplier=2, now=datetime(2026, 7, 13, 12))
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_training BEFORE UPDATE OF status ON normal_training_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.service.complete("op-f", task_period="2026-W29")
        with db_backend.connection(self.game) as conn:
            self.assertEqual((100, 5), tuple(conn.execute("SELECT exp,(SELECT type FROM user_cd WHERE user_id='cultivator') FROM user_xiuxian WHERE user_id='cultivator'").fetchone()))
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='statistics'").fetchone())


if __name__ == "__main__":
    unittest.main()
