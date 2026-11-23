from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import DungeonApplication
from ..migrations import apply_dungeon_explore_player_schema, apply_dungeon_session


class DungeonExitWriteTests(unittest.TestCase):
    def prepare(self, directory):
        player, game = Path(directory) / "player.db", Path(directory) / "game.db"
        with DatabaseUnitOfWork(player) as uow:
            apply_dungeon_explore_player_schema(uow)
            apply_dungeon_session(uow)
            uow.execute("INSERT INTO player_dungeon_status VALUES('actor','trial','Trial','exploring',2,5,'2026-11-20',1,'generation-1')")
            uow.execute("INSERT INTO player_dungeon_status VALUES('other','trial','Trial','not_started',0,5,'2026-11-20',1,'generation-1')")
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('actor',100)")
        app = DungeonApplication(game, player)
        expected = {"dungeon_id": "trial", "dungeon_status": "exploring", "current_layer": 2, "total_layers": 5,
                    "last_reset_date": "2026-11-20", "reset_generation": 1, "reset_operation_id": "generation-1"}
        return app, expected, {"dungeon_id": "trial", "date": "2026-11-20"}

    def state(self, app):
        with DatabaseUnitOfWork(app.player_database, read_only=True) as uow:
            players = uow.query_all("SELECT * FROM player_dungeon_status ORDER BY user_id")
            receipts = uow.query_all("SELECT * FROM dungeon_session_operations ORDER BY operation_id")
        with DatabaseUnitOfWork(app.game_database, read_only=True) as uow:
            assets = uow.query_all("SELECT * FROM user_xiuxian ORDER BY user_id")
        return players, receipts, assets

    def test_known_write_failures_roll_back_entire_exit_and_recover_same_operation(self):
        faults = (
            ("BEFORE UPDATE ON player_dungeon_status", "NEW.user_id='actor'", "SELECT RAISE(IGNORE)", "state_write_rejected"),
            ("AFTER UPDATE ON player_dungeon_status", "NEW.user_id='actor'", "UPDATE player_dungeon_status SET dungeon_name='changed' WHERE user_id='actor'", "state_projection_mismatch"),
            ("AFTER UPDATE ON player_dungeon_status", "NEW.user_id='actor'", "DELETE FROM player_dungeon_status WHERE user_id='actor'", "state_projection_mismatch"),
            ("BEFORE INSERT ON dungeon_session_operations", "1", "SELECT RAISE(IGNORE)", "receipt_write_rejected"),
            ("AFTER INSERT ON dungeon_session_operations", "1", "UPDATE dungeon_session_operations SET payload='{}' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
            ("AFTER INSERT ON dungeon_session_operations", "1", "UPDATE dungeon_session_operations SET result_status='completed' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
            ("AFTER INSERT ON dungeon_session_operations", "1", "UPDATE dungeon_session_operations SET dungeon_status='not_started' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
            ("AFTER INSERT ON dungeon_session_operations", "1", "UPDATE player_dungeon_status SET current_layer=4 WHERE user_id='actor'", "state_projection_mismatch"),
            ("AFTER UPDATE ON player_dungeon_status", "NEW.user_id='actor'", "UPDATE player_dungeon_status SET reset_generation=99 WHERE user_id='actor'", "state_projection_mismatch"),
        )
        for timing, condition, body, reason in faults:
            with self.subTest(reason=reason, body=body), tempfile.TemporaryDirectory() as directory:
                app, expected, dungeon = self.prepare(directory)
                before = self.state(app)
                with DatabaseUnitOfWork(app.player_database) as uow:
                    uow.execute(f"CREATE TRIGGER exit_fault {timing} WHEN {condition} BEGIN "
                                f"UPDATE player_dungeon_status SET current_layer=99 WHERE user_id='other'; {body}; END")
                failed = app.session_transition(operation_id="exit", user_id="actor", expected=expected, dungeon=dungeon, action="exit")
                self.assertEqual((failed["status"], failed.get("reason"), failed.get("subject")),
                                 ("failed", "dungeon_exit_" + reason, "actor"))
                self.assertEqual(self.state(app), before)
                self.assertIsNone(app.session_operation(operation_id="exit", user_id="actor", action="exit"))
                with DatabaseUnitOfWork(app.player_database) as uow:
                    uow.execute("DROP TRIGGER exit_fault")
                applied = app.session_transition(operation_id="exit", user_id="actor", expected=expected, dungeon=dungeon, action="exit")
                self.assertEqual(applied["status"], "applied")
                committed = self.state(app)
                self.assertEqual({key: value for key, value in committed[0][0].items() if key != "dungeon_status"},
                                 {key: value for key, value in before[0][0].items() if key != "dungeon_status"})
                self.assertEqual(committed[0][0]["dungeon_status"], "exited")
                self.assertEqual(committed[0][1], before[0][1])
                self.assertEqual(committed[2], before[2])
                self.assertEqual(app.session_operation(operation_id="exit", user_id="actor", action="exit")["status"], "duplicate")
                self.assertEqual(app.session_transition(operation_id="exit", user_id="actor", expected={}, dungeon=dungeon, action="exit")["status"], "duplicate")
                self.assertEqual(self.state(app), committed)

    def test_abort_keeps_original_exception_and_rolls_back_trigger_side_effects(self):
        for timing, condition in (("BEFORE UPDATE ON player_dungeon_status", "NEW.user_id='actor'"),
                                  ("BEFORE INSERT ON dungeon_session_operations", "1")):
            with self.subTest(timing=timing), tempfile.TemporaryDirectory() as directory:
                app, expected, dungeon = self.prepare(directory)
                before = self.state(app)
                with DatabaseUnitOfWork(app.player_database) as uow:
                    uow.execute(f"CREATE TRIGGER exit_abort {timing} WHEN {condition} BEGIN "
                                "UPDATE player_dungeon_status SET current_layer=99 WHERE user_id='other'; "
                                "SELECT RAISE(ABORT,'isolated departure transaction abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated departure transaction abort"):
                    app.session_transition(operation_id="exit", user_id="actor", expected=expected, dungeon=dungeon, action="exit")
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.player_database) as uow:
                    uow.execute("DROP TRIGGER exit_abort")
                self.assertEqual(app.session_transition(operation_id="exit", user_id="actor", expected=expected, dungeon=dungeon, action="exit")["status"], "applied")

    def test_rejection_keeps_reason_subject_and_checked_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            app, expected, dungeon = self.prepare(directory)
            stale = {**expected, "reset_generation": 0}
            result = app.session_transition(operation_id="stale", user_id="actor", expected=stale, dungeon=dungeon, action="exit")
            self.assertEqual((result["status"], result["reason"], result.get("subject")), ("state_changed", "dungeon_reset_changed", "actor"))
            self.assertEqual(app.session_operation(operation_id="stale", user_id="actor", action="exit"), result)
            self.assertEqual(app.session_transition(operation_id="stale", user_id="actor", expected=expected, dungeon=dungeon, action="exit"), result)
            before = self.state(app)
            with DatabaseUnitOfWork(app.player_database) as uow:
                uow.execute("CREATE TRIGGER exit_rejection_fault BEFORE INSERT ON dungeon_session_operations BEGIN SELECT RAISE(IGNORE); END")
            failed = app.session_transition(operation_id="missing", user_id="missing", expected=expected, dungeon=dungeon, action="exit")
            self.assertEqual((failed["status"], failed["reason"], failed["subject"]), ("failed", "dungeon_exit_receipt_write_rejected", "missing"))
            self.assertEqual(self.state(app), before)

    def test_concurrent_same_operation_applies_once_and_legacy_receipts_stay_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            app, expected, dungeon = self.prepare(directory)

            def exit_once(_):
                return app.session_transition(operation_id="exit", user_id="actor", expected=expected, dungeon=dungeon, action="exit")["status"]

            with ThreadPoolExecutor(max_workers=2) as executor:
                self.assertEqual(sorted(executor.map(exit_once, range(2))), ["applied", "duplicate"])
            with DatabaseUnitOfWork(app.player_database) as uow:
                payload = json.dumps({"user_id": "actor", "action": "exit", "dungeon": dungeon}, sort_keys=True)
                uow.execute("INSERT INTO dungeon_session_operations(operation_id,payload,result_status,dungeon_status) VALUES('legacy',?,'state_changed','exploring')", (payload,))
            before = self.state(app)
            legacy = app.session_operation(operation_id="legacy", user_id="actor", action="exit")
            self.assertEqual(legacy, {"status": "state_changed", "reason": "legacy_rejection", "dungeon_status": "exploring"})
            conflict = app.session_transition(operation_id="exit", user_id="other", expected={}, dungeon=dungeon, action="exit")
            self.assertEqual(conflict["reason"], "operation_conflict")
            self.assertEqual(self.state(app), before)


if __name__ == "__main__":
    unittest.main()
