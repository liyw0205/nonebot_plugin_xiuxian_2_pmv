from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.movement_settlement_service import (
    MapMovementSettlementService,
)
from tests.test_db_backend import db_backend


class MapMovementSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,user_stamina INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 100))
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT,visited_nodes TEXT)"
            )
            conn.execute(
                "INSERT INTO map_status VALUES (%s,%s,%s,%s,%s)",
                ("u", "凡界", "一重天", "n1", json.dumps(["n1"])),
            )
        self.service = MapMovementSettlementService(self.game, self.player)
        self.old = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        self.new = {"realm": "仙界", "heaven": "九重天", "node_id": "n9"}

    def tearDown(self):
        self.temp_dir.cleanup()

    def move(self, operation="op", **changes):
        values = {"expected": self.old, "target": self.new, "stamina": 100, "cost": 30}
        values.update(changes)
        return self.service.move(
            operation, "u", values["expected"], values["target"], values["stamina"], values["cost"]
        )

    def current(self):
        with db_backend.connection(self.game) as conn:
            stamina = int(conn.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                "SELECT realm,heaven,node_id,visited_nodes FROM map_status WHERE user_id=%s", ("u",)
            ).fetchone()
        return stamina, tuple(row[:3]), json.loads(row[3])

    def test_move_updates_position_visit_and_stamina_atomically(self):
        result = self.move()
        self.assertEqual((result.status, result.stamina), ("applied", 70))
        self.assertEqual(self.current(), (70, ("仙界", "九重天", "n9"), ["n1", "n9"]))

    def test_duplicate_is_idempotent_and_payload_conflict_is_rejected(self):
        self.assertEqual(self.move("repeat").status, "applied")
        self.assertEqual(self.move("repeat").status, "duplicate")
        self.assertEqual(self.move("repeat", cost=31).status, "state_changed")
        self.assertEqual(self.current()[0], 70)

    def test_stale_position_and_insufficient_stamina_are_rejected(self):
        self.assertEqual(self.move("stale", expected=dict(self.old, node_id="n0")).status, "state_changed")
        self.assertEqual(self.move("poor", cost=101).status, "stamina_insufficient")
        self.assertEqual(self.current()[0], 100)

    def test_operation_failure_rolls_back_position_and_stamina(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE map_movement_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stamina INTEGER NOT NULL,created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_move BEFORE INSERT ON map_movement_operations BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.move("rollback")
        self.assertEqual(self.current(), (100, ("凡界", "一重天", "n1"), ["n1"]))


if __name__ == "__main__":
    unittest.main()
