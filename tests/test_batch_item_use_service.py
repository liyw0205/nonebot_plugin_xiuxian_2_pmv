from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.batch_item_use_service import BatchItemUseService
from tests.test_db_backend import db_backend


class BatchItemUseServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER DEFAULT 0, UNIQUE(user_id, goods_id))")
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s)", ("u", 9001, 3, 3))
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TABLE player_pet (user_id TEXT PRIMARY KEY, active_uid TEXT, egg_pity_count INTEGER, egg_pity_no_mythic_count INTEGER, travel TEXT)")
            conn.execute("INSERT INTO player_pet VALUES (%s,%s,0,0,NULL)", ("u", "old"))
            conn.execute("CREATE TABLE player_pet_item (id TEXT PRIMARY KEY, user_id TEXT, uid TEXT, is_active INTEGER, pet_id TEXT, stars INTEGER, exp INTEGER, total_exp INTEGER, skill_id TEXT, created_at INTEGER, updated_at INTEGER, UNIQUE(user_id, uid))")
            conn.execute("INSERT INTO player_pet_item VALUES (%s,%s,%s,1,%s,1,0,0,NULL,0,0)", ("u:old", "u", "old", "base"))
        self.service = BatchItemUseService(self.game_database, self.player_database)
        self.pets = (
            ({"uid": "new-1", "pet_id": "p1", "stars": 1, "skill": {}}, "bag"),
            ({"uid": "new-2", "pet_id": "p2", "stars": 1, "skill": {}}, "bag"),
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def use(self, operation_id="batch-use-1", quantity=2, pets=None):
        return self.service.use_pet_eggs(operation_id, "u", 9001, quantity, "old", ["old"], self.pets if pets is None else pets, bag_limit=10)

    def game_quantity(self):
        with db_backend.connection(self.game_database) as conn:
            return int(conn.execute("SELECT goods_num FROM back").fetchone()[0])

    def pet_uids(self):
        with db_backend.connection(self.player_database) as conn:
            return [str(row[0]) for row in conn.execute("SELECT uid FROM player_pet_item ORDER BY uid").fetchall()]

    def test_batch_consumes_all_eggs_and_grants_all_pets_atomically(self):
        result = self.use()
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.game_quantity(), 1)
        self.assertEqual(self.pet_uids(), ["new-1", "new-2", "old"])

    def test_duplicate_returns_original_batch_without_second_effect(self):
        first = self.use("same-operation")
        rerolled = (({"uid": "other-1", "pet_id": "x", "stars": 1}, "bag"), ({"uid": "other-2", "pet_id": "x", "stars": 1}, "bag"))
        second = self.use("same-operation", pets=rerolled)
        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual([pet[0]["uid"] for pet in second.pets], ["new-1", "new-2"])
        self.assertEqual(self.game_quantity(), 1)

    def test_reusing_operation_for_different_quantity_is_rejected(self):
        self.use("conflicting-operation")
        conflict = self.service.use_pet_eggs("conflicting-operation", "u", 9001, 1, "old", ["old"], (self.pets[0],), bag_limit=10)
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(self.game_quantity(), 1)

    def test_pet_snapshot_change_rejects_whole_batch(self):
        result = self.service.use_pet_eggs("stale-snapshot", "u", 9001, 2, "missing", ["old"], self.pets, bag_limit=10)
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.game_quantity(), 3)
        self.assertEqual(self.pet_uids(), ["old"])

    def test_player_write_failure_rolls_back_egg_consumption(self):
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TRIGGER reject_new_pet BEFORE INSERT ON player_pet_item WHEN NEW.uid <> 'old' BEGIN SELECT RAISE(ABORT, 'reject pet'); END")
        with self.assertRaises(Exception):
            self.use("rollback-operation")
        self.assertEqual(self.game_quantity(), 3)
        self.assertEqual(self.pet_uids(), ["old"])
        with db_backend.connection(self.game_database) as conn:
            if conn.table_exists("batch_pet_egg_use_operations"):
                self.assertIsNone(conn.execute("SELECT 1 FROM batch_pet_egg_use_operations WHERE operation_id=%s", ("rollback-operation",)).fetchone())


if __name__ == "__main__":
    unittest.main()
