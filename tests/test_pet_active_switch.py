import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.active_switch_service import PetActiveSwitchService
from tests.test_db_backend import db_backend


class PetActiveSwitchServiceTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY,active_uid TEXT,active TEXT)")
            conn.execute("INSERT INTO player_pet VALUES('u','old','old')")
            conn.execute("CREATE TABLE player_pet_item(user_id TEXT,uid TEXT,is_active INTEGER,updated_at INTEGER)")
            conn.execute("INSERT INTO player_pet_item VALUES('u','old',1,0)")
            conn.execute("INSERT INTO player_pet_item VALUES('u','new',0,0)")
        self.service = PetActiveSwitchService(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_switch_and_replay(self):
        self.assertEqual(self.service.switch("op", "u", "old", "new").status, "applied")
        self.assertEqual(self.service.switch("op", "u", "old", "new").status, "duplicate")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute("SELECT active_uid,active FROM player_pet").fetchone()), ("new", "new"))
            self.assertEqual(
                [tuple(row) for row in conn.execute("SELECT uid,is_active FROM player_pet_item ORDER BY uid").fetchall()],
                [("new", 1), ("old", 0)],
            )

    def test_already_active(self):
        self.assertEqual(self.service.switch("op", "u", "old", "old").status, "already_active")

    def test_missing_and_traveling_pet(self):
        self.assertEqual(self.service.switch("missing", "u", "old", "x").status, "pet_missing")
        self.assertEqual(self.service.switch("travel", "u", "old", "new", "new").status, "pet_traveling")

    def test_state_changed_and_multiple_active_rows(self):
        self.assertEqual(self.service.switch("stale", "u", "different", "new").status, "state_changed")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE player_pet_item SET is_active=1 WHERE uid='new'")
        self.assertEqual(self.service.switch("multiple", "u", "old", "new").status, "state_changed")

    def test_operation_payload_conflict(self):
        self.assertEqual(self.service.switch("op", "u", "old", "new").status, "applied")
        self.assertEqual(self.service.switch("op", "u", "new", "old").status, "operation_conflict")

    def test_operation_failure_rolls_back_switch(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE pet_active_switch_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,active_uid TEXT NOT NULL,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_pet_active_switch BEFORE INSERT ON pet_active_switch_operations BEGIN SELECT RAISE(ABORT,'operation failed'); END")
        with self.assertRaises(Exception):
            self.service.switch("op", "u", "old", "new")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute("SELECT active_uid,active FROM player_pet").fetchone()), ("old", "old"))
            self.assertEqual(
                [tuple(row) for row in conn.execute("SELECT uid,is_active FROM player_pet_item ORDER BY uid").fetchall()],
                [("new", 0), ("old", 1)],
            )
