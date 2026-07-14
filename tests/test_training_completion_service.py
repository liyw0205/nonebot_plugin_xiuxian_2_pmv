import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_training.completion_service import TrainingCompletionService
from tests.test_db_backend import db_backend


class TrainingCompletionServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("u", 1, 10))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE training (user_id TEXT PRIMARY KEY,progress TEXT,last_time TEXT,points TEXT,completed TEXT,max_progress TEXT,last_event TEXT,weekly_purchases TEXT)")
            conn.execute("INSERT INTO training VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u", "11", "2026-07-13 01:00:00", "10", "0", "11", "old", "{}"))
        self.service = TrainingCompletionService(self.game, self.player)
        self.expected = {"progress": 11, "last_time": "2026-07-13 01:00:00", "points": 10, "completed": 0, "max_progress": 11, "last_event": "old", "weekly_purchases": {}}
        self.updated = {"progress": 0, "last_time": "2026-07-13 02:00:00", "points": 1010, "completed": 1, "max_progress": 12, "last_event": "done", "weekly_purchases": {}}

    def tearDown(self): self.temp_dir.cleanup()

    def test_completion_commits_state_assets_and_item(self):
        result = self.service.complete("op", "u", self.expected, self.updated, 3, 4, [{"id": 1, "name": "item", "type": "type", "amount": 1}], 99)
        self.assertEqual(result.status, "applied")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,exp FROM user_xiuxian").fetchone()), (4, 14))
            self.assertEqual(tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone()), (1, 1))
        with db_backend.connection(self.player) as conn:
            self.assertEqual(tuple(conn.execute("SELECT progress,points,completed FROM training").fetchone()), ("0", "1010", "1"))

    def test_inventory_failure_leaves_state_and_assets_unchanged(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u", 1, "item", "type", 99, "", "", 99))
        self.assertEqual(self.service.complete("full", "u", self.expected, self.updated, 3, 4, [{"id": 1, "name": "item", "type": "type", "amount": 1}], 99).status, "inventory_full")
        with db_backend.connection(self.player) as conn:
            self.assertEqual(tuple(conn.execute("SELECT progress,completed FROM training").fetchone()), ("11", "0"))

    def test_compact_weekly_json_matches_semantic_snapshot(self):
        weekly = {"_last_reset": "2026-07-14", "1999": 1}
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE training SET weekly_purchases=%s WHERE user_id=%s",
                ('{"1999":1,"_last_reset":"2026-07-14"}', "u"),
            )
        expected = dict(self.expected, weekly_purchases=weekly)
        updated = dict(self.updated, weekly_purchases=weekly)
        result = self.service.complete("compact", "u", expected, updated, 3, 4, [], 99)
        self.assertEqual(result.status, "applied")
