from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.transaction_service import PetReleaseService
from tests.test_db_backend import db_backend


class PetReleaseBatchServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,UNIQUE(user_id,goods_id))"
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY,active_uid TEXT,active TEXT)")
            conn.execute("INSERT INTO player_pet VALUES ('u',NULL,NULL)")
            conn.execute("CREATE TABLE player_pet_item(user_id TEXT,uid TEXT,total_exp INTEGER,is_active INTEGER)")
            conn.execute("INSERT INTO player_pet_item VALUES ('u','a',1000,0)")
            conn.execute("INSERT INTO player_pet_item VALUES ('u','b',2000,0)")
        self.service = PetReleaseService(self.game, self.player)
        self.snapshot = [
            {"uid": "a", "total_exp": 1000, "is_active": 0},
            {"uid": "b", "total_exp": 2000, "is_active": 0},
        ]

    def tearDown(self):
        self.temp.cleanup()

    def release(self, operation="batch", **overrides):
        values = {"pets": self.snapshot, "refund": 3, "max_goods": 99}
        values.update(overrides)
        return self.service.release_batch(
            operation, "u", values["pets"], 9, "灵髓", "特殊道具", values["refund"], values["max_goods"]
        )

    def state(self):
        with db_backend.connection(self.player) as conn:
            count = int(conn.execute("SELECT COUNT(*) FROM player_pet_item").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            row = conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=9").fetchone()
        return count, None if row is None else int(row[0])

    def test_success_duplicate_and_payload_conflict(self):
        first = self.release()
        duplicate = self.release()
        conflict = self.release(refund=4)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual((first.released_uids, duplicate.refund, self.state()), (("a", "b"), 3, (0, 3)))

    def test_snapshot_or_active_change_preserves_whole_batch(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE player_pet_item SET total_exp=1001 WHERE uid='a'")
        self.assertEqual(self.release("stale").status, "state_changed")
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE player_pet_item SET total_exp=1000,is_active=1 WHERE uid='a'")
        active = [{**pet, "is_active": 1 if pet["uid"] == "a" else 0} for pet in self.snapshot]
        self.assertEqual(self.release("active", pets=active).status, "active_pet")
        self.assertEqual(self.state(), (2, None))

    def test_inventory_limit_and_exception_roll_back_snapshot(self):
        self.assertEqual(self.release("full", max_goods=2).status, "inventory_full")
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE pet_release_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "refund INTEGER NOT NULL,released_uids TEXT NOT NULL,created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_release BEFORE INSERT ON pet_release_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.release("rollback")
        self.assertEqual(self.state(), (2, None))


if __name__ == "__main__":
    unittest.main()
