from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.transaction_service import PetReleaseService
from tests.test_db_backend import db_backend


class PetReleaseServiceTests(unittest.TestCase):
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
            conn.execute("INSERT INTO player_pet VALUES ('u','active','active')")
            conn.execute("CREATE TABLE player_pet_item(user_id TEXT,uid TEXT,total_exp INTEGER,is_active INTEGER)")
            conn.execute("INSERT INTO player_pet_item VALUES ('u','active',1000,1)")
            conn.execute("INSERT INTO player_pet_item VALUES ('u','bag',500,0)")
        self.service = PetReleaseService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def release(self, operation="release", uid="active", exp=1000, refund=1, max_goods=99, active=True):
        return self.service.release(operation, "u", uid, exp, 9, refund, max_goods, active)

    def state(self):
        with db_backend.connection(self.player) as conn:
            meta = tuple(conn.execute("SELECT active_uid,active FROM player_pet WHERE user_id='u'").fetchone())
            pets = [tuple(row) for row in conn.execute("SELECT uid,is_active FROM player_pet_item ORDER BY uid")]
        with db_backend.connection(self.game) as conn:
            row = conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=9").fetchone()
        return meta, pets, None if row is None else int(row[0])

    def test_active_release_clears_state_and_replays(self):
        first = self.release()
        duplicate = self.release()
        conflict = self.release(refund=2)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(first.released_uids, ("active",))
        self.assertEqual(self.state(), ((None, None), [("bag", 0)], 1))

    def test_bag_release_preserves_active_pet(self):
        result = self.release("bag-release", "bag", 500, active=False)
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.state(), (("active", "active"), [("active", 1)], 1))

    def test_snapshot_and_active_metadata_changes_are_rejected(self):
        self.assertEqual(self.release("stale-exp", exp=999).status, "state_changed")
        self.assertEqual(self.release("stale-active", active=False).status, "state_changed")
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE player_pet SET active_uid='different' WHERE user_id='u'")
        self.assertEqual(self.release("stale-meta").status, "state_changed")
        self.assertEqual(self.state()[1:], ([("active", 1), ("bag", 0)], None))

    def test_inventory_limit_and_operation_failure_roll_back(self):
        self.assertEqual(self.release("full", max_goods=0).status, "inventory_full")
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE pet_release_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "refund INTEGER NOT NULL,released_uids TEXT NOT NULL,created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_release BEFORE INSERT ON pet_release_operations "
                "BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.release("rollback")
        self.assertEqual(self.state(), (("active", "active"), [("active", 1), ("bag", 0)], None))


if __name__ == "__main__":
    unittest.main()
