import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.session_service import DungeonSessionService
from tests.test_db_backend import db_backend


class DungeonSessionServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE player_dungeon_status (user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT)")
            conn.execute("INSERT INTO player_dungeon_status VALUES (%s,%s,%s,%s,%s,%s)", ("u", "d1", "not_started", 2, 5, "2026-07-13"))
        self.service = DungeonSessionService(self.database)
        self.expected = {"dungeon_id": "d1", "dungeon_status": "not_started", "current_layer": 2, "total_layers": 5, "last_reset_date": "2026-07-13"}
        self.dungeon = {"dungeon_id": "d1", "date": "2026-07-13"}

    def tearDown(self):
        self.temp_dir.cleanup()

    def status(self):
        with db_backend.connection(self.database) as conn:
            return str(conn.execute("SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s", ("u",)).fetchone()[0])

    def test_enter_and_exit_preserve_progress(self):
        self.assertEqual(self.service.enter("enter", "u", self.expected, self.dungeon).status, "applied")
        exploring = dict(self.expected, dungeon_status="exploring")
        self.assertEqual(self.service.exit("exit", "u", exploring, self.dungeon).status, "applied")
        self.assertEqual(self.status(), "exited")

    def test_stale_and_duplicate_are_idempotent(self):
        first = self.service.enter("repeat", "u", self.expected, self.dungeon)
        duplicate = self.service.enter("repeat", "u", self.expected, self.dungeon)
        stale = self.service.exit("stale", "u", self.expected, self.dungeon)
        self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))

    def test_operation_failure_rolls_back_transition(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE dungeon_session_operations (operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,dungeon_status TEXT,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_session BEFORE INSERT ON dungeon_session_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.enter("rollback", "u", self.expected, self.dungeon)
        self.assertEqual(self.status(), "not_started")


if __name__ == "__main__":
    unittest.main()
