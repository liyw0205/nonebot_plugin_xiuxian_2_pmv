from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.transaction_service import MapDongfuBuildService
from tests.test_db_backend import db_backend


class MapDongfuBuildTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 150))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)")
            conn.execute("INSERT INTO map_status VALUES (%s,%s,%s,%s)", ("u", "凡界", "一重天", "n1"))
            conn.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER,realm TEXT,heaven TEXT,node_id TEXT,node_name TEXT,node_type TEXT)")
            conn.execute("INSERT INTO dongfu_status (user_id,built) VALUES (%s,%s)", ("u", 0))
        self.service = MapDongfuBuildService(self.game, self.player)
        self.position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        self.dongfu = {"built": 1, "realm": "凡界", "heaven": "一重天", "node_id": "n1", "node_name": "青山", "node_type": "山脉"}

    def tearDown(self):
        self.temp_dir.cleanup()

    def build(self, operation_id="op", **changes):
        values = {"stone": 150, "cost": 100, "position": self.position, "dongfu": self.dongfu}
        values.update(changes)
        return self.service.build(operation_id, "u", values["stone"], values["cost"], values["position"], values["dongfu"])

    def current(self):
        with db_backend.connection(self.game) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            dongfu = conn.execute("SELECT built,node_id,node_name FROM dongfu_status WHERE user_id=%s", ("u",)).fetchone()
        return stone, tuple(dongfu)

    def test_build_spends_stone_and_saves_complete_dongfu(self):
        result = self.build()
        self.assertEqual((result.status, result.stone), ("applied", 50))
        self.assertEqual(self.current(), (50, (1, "n1", "青山")))

    def test_duplicate_is_idempotent_and_position_change_is_rejected(self):
        self.assertEqual(self.build("repeat").status, "applied")
        self.assertEqual(self.build("repeat").status, "duplicate")
        self.assertEqual(self.current()[0], 50)
        self.setUp()
        self.assertEqual(self.build("stale", position=dict(self.position, node_id="n2")).status, "state_changed")
        self.assertEqual(self.current(), (150, (0, None, None)))

    def test_operation_failure_rolls_back_stone_and_dongfu(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE map_dongfu_build_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_build BEFORE INSERT ON map_dongfu_build_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.build("rollback")
        self.assertEqual(self.current(), (150, (0, None, None)))


if __name__ == "__main__":
    unittest.main()
