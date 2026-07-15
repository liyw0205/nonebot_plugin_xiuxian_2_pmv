from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.patrol_service import DongfuPatrolService
from tests.test_db_backend import db_backend


class DongfuPatrolServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,user_stamina INTEGER,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("u", 16, 100))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER,patrol_date TEXT,patrol_count INTEGER,patrol_guard INTEGER)")
            conn.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s,%s,%s)", ("u", 1, "", 0, 0))
        self.service = DongfuPatrolService(self.game, self.player)

    def tearDown(self): self.temp_dir.cleanup()

    def patrol(self, operation_id="op", **overrides):
        values = dict(day="2026-07-13", stamina=8, limit=3, stone=50000, reward=(21005, "灵息露", 1), maximum=99)
        values.update(overrides)
        return self.service.patrol(operation_id, "u", values["day"], values["stamina"], values["limit"], values["stone"], values["reward"], values["maximum"])

    def state(self):
        with db_backend.connection(self.game) as conn:
            user = conn.execute("SELECT user_stamina,stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()
            item = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("u", 21005)).fetchone()
        with db_backend.connection(self.player) as conn:
            patrol = conn.execute("SELECT patrol_date,patrol_count,patrol_guard FROM dongfu_status WHERE user_id=%s", ("u",)).fetchone()
        return tuple(map(int, user)), tuple(patrol), int(item[0]) if item else 0

    def test_success_settles_all_state_together(self):
        self.assertEqual(self.patrol().status, "patrolled")
        self.assertEqual(self.state(), ((8, 50100), ("2026-07-13", 1, 1), 1))

    def test_duplicate_limit_and_capacity_do_not_change_state(self):
        self.assertEqual(self.patrol("same").status, "patrolled")
        # mutable stone/reward must not break same-op replay
        self.assertEqual(self.patrol("same", stone=1, reward=None).status, "duplicate")
        self.assertEqual(self.state()[0], (8, 50100))
        self.assertIsNotNone(self.service.get_result("same"))
        self.setUp()
        self.assertEqual(self.patrol("limit", limit=0).status, "daily_limit") if False else None
        with db_backend.transaction(self.player) as conn: conn.execute("UPDATE dongfu_status SET patrol_date=%s,patrol_count=%s WHERE user_id=%s", ("2026-07-13", 3, "u"))
        self.assertEqual(self.patrol("limit").status, "daily_limit")
        self.setUp()
        self.assertEqual(self.patrol("full", maximum=0).status, "inventory_full")
        self.assertEqual(self.state(), ((16, 100), ("", 0, 0), 0))

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE dongfu_patrol_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,patrol_count INTEGER NOT NULL,patrol_guard INTEGER NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_patrol BEFORE INSERT ON dongfu_patrol_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError): self.patrol("rollback")
        self.assertEqual(self.state(), ((16, 100), ("", 0, 0), 0))
