import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.transaction_service import DungeonSessionService
from tests.test_db_backend import db_backend


class DungeonSessionServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE player_dungeon_status (user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT,reset_generation INTEGER,reset_operation_id TEXT)")
            conn.execute("INSERT INTO player_dungeon_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u", "d1", "not_started", 2, 5, "2026-07-13", 3, "reset-3"))
        self.service = DungeonSessionService(self.database)
        self.expected = {"dungeon_id": "d1", "dungeon_status": "not_started", "current_layer": 2, "total_layers": 5, "last_reset_date": "2026-07-13", "reset_generation": 3, "reset_operation_id": "reset-3"}
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

    def test_reset_generation_prevents_same_shape_aba_transition(self):
        stale = dict(self.expected, reset_generation=2, reset_operation_id="reset-2")
        self.assertEqual(
            self.service.enter("aba", "u", stale, self.dungeon).status,
            "state_changed",
        )
        self.assertEqual(self.status(), "not_started")

    def test_missing_generation_snapshot_cannot_bypass_aba_guard(self):
        legacy_expected = {
            key: value
            for key, value in self.expected.items()
            if key not in {"reset_generation", "reset_operation_id"}
        }

        result = self.service.enter(
            "missing-generation", "u", legacy_expected, self.dungeon
        )

        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.status(), "not_started")

    def test_rejected_exit_replays_without_exiting_a_later_session(self):
        first = self.service.exit("rejected-exit", "u", self.expected, self.dungeon)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_status='exploring' "
                "WHERE user_id='u'"
            )
        exploring = dict(self.expected, dungeon_status="exploring")
        replay = self.service.exit("rejected-exit", "u", exploring, self.dungeon)

        self.assertEqual((first.status, replay.status), ("not_exploring", "not_exploring"))
        self.assertEqual(self.status(), "exploring")

    def test_state_changed_result_is_persisted_for_same_request(self):
        stale = dict(self.expected, reset_generation=2, reset_operation_id="reset-2")
        first = self.service.enter("stale-enter", "u", stale, self.dungeon)
        replay = self.service.enter("stale-enter", "u", self.expected, self.dungeon)

        self.assertEqual((first.status, replay.status), ("state_changed", "state_changed"))
        self.assertEqual(self.status(), "not_started")

    def test_exit_operation_replays_before_a_later_reset_snapshot(self):
        exploring = dict(self.expected, dungeon_status="exploring")
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_status=%s WHERE user_id=%s",
                ("exploring", "u"),
            )
        first = self.service.exit("cross-reset", "u", exploring, self.dungeon)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_id=%s,dungeon_status=%s,"
                "current_layer=%s,last_reset_date=%s,reset_generation=%s,"
                "reset_operation_id=%s WHERE user_id=%s",
                ("d2", "not_started", 0, "2026-07-14", 4, "reset-4", "u"),
            )

        replay = self.service.operation_result("cross-reset", "u", "exit")

        self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
        with db_backend.connection(self.database) as conn:
            current = conn.execute(
                "SELECT dungeon_id,dungeon_status,reset_generation,reset_operation_id "
                "FROM player_dungeon_status WHERE user_id=%s",
                ("u",),
            ).fetchone()
        self.assertEqual(tuple(current), ("d2", "not_started", 4, "reset-4"))

    def test_exit_handler_replays_before_reading_mutable_dungeon_state(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[
            source.index("async def handle_dungeon_exit") : source.index(
                "async def handle_explore_dungeon"
            )
        ]
        self.assertLess(
            handler.index("dungeon_session_service.operation_result"),
            handler.index("dungeon_manager.get_player_status"),
        )


if __name__ == "__main__":
    unittest.main()
