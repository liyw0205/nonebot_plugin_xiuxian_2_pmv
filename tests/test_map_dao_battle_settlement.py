from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.dao_battle_settlement_service import (
    MapDaoBattleSettlementService,
)
from tests.test_db_backend import db_backend


class MapDaoBattleSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.player, self.game = root / "player.sqlite3", root / "game.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s),(%s)", ("a", "b"))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)")
            conn.execute("INSERT INTO map_status VALUES (%s,%s,%s,%s)", ("a", "凡界", "一重天", "n1"))
            conn.execute("INSERT INTO map_status VALUES (%s,%s,%s,%s)", ("b", "凡界", "一重天", "n1"))
        self.service = MapDaoBattleSettlementService(self.player, self.game)
        self.position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}

    def tearDown(self):
        self.temp_dir.cleanup()

    def settle(self, operation="op", won=True):
        return self.service.settle(operation, "a", "b", self.position, won)

    def records(self):
        with db_backend.connection(self.player) as conn:
            rows = conn.execute("SELECT user_id,total,win,lose FROM dao_record ORDER BY user_id").fetchall()
        return [tuple(row) for row in rows]

    def test_settlement_updates_both_players_symmetrically(self):
        self.assertEqual(self.settle().status, "applied")
        self.assertEqual(self.records(), [("a", 1, 1, 0), ("b", 1, 0, 1)])

    def test_reverse_result_and_duplicate_are_idempotent(self):
        self.assertEqual(self.settle("reverse", False).status, "applied")
        self.assertEqual(self.settle("reverse", False).status, "duplicate")
        self.assertEqual(self.settle("reverse", True).status, "state_changed")
        self.assertEqual(self.records(), [("a", 1, 0, 1), ("b", 1, 1, 0)])

    def test_position_change_rejects_both_records(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE map_status SET node_id=%s WHERE user_id=%s", ("n2", "b"))
        self.assertEqual(self.settle("moved").status, "position_changed")
        with db_backend.connection(self.player) as conn:
            self.assertFalse(conn.table_exists("dao_record"))

    def test_operation_failure_rolls_back_both_players(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE map_dao_battle_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_dao BEFORE INSERT ON map_dao_battle_operations BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")
        with db_backend.connection(self.player) as conn:
            self.assertFalse(conn.table_exists("dao_record"))


if __name__ == "__main__":
    unittest.main()
