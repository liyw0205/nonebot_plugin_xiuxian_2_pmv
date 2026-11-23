from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_dungeon_team_members_index, apply_dungeon_team_schema
from ..team_application import DungeonTeamApplication


class DungeonTeamCreateWriteTests(unittest.TestCase):
    def prepare(self, directory):
        game, player = Path(directory) / "game.db", Path(directory) / "player.db"
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            uow.execute("INSERT INTO user_xiuxian VALUES('leader')")
        with DatabaseUnitOfWork(player) as uow:
            apply_dungeon_team_schema(uow)
            apply_dungeon_team_members_index(uow)
            uow.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
            uow.execute("INSERT INTO player_dungeon_status VALUES('leader','not_started')")
        return DungeonTeamApplication(player, game_database=game)

    @staticmethod
    def create(application, *, name="Trial"):
        return application.create("create", "team", name, "leader", "g", "now", 100)

    @staticmethod
    def state(application):
        with DatabaseUnitOfWork(application.repository.database, read_only=True) as uow:
            return {
                table: uow.query_all(f"SELECT * FROM {table} ORDER BY rowid")
                for table in ("teams", "dungeon_team_members", "dungeon_team_operations", "team_cd")
            }

    def test_each_write_failure_retains_exact_reason_rolls_back_and_recovers(self):
        faults = (
            ("BEFORE INSERT ON teams", "SELECT RAISE(IGNORE)", "state_write_rejected"),
            ("AFTER INSERT ON teams", "UPDATE teams SET team_name='changed' WHERE user_id=NEW.user_id", "state_projection_mismatch"),
            ("BEFORE INSERT ON dungeon_team_members", "SELECT RAISE(IGNORE)", "members_write_rejected"),
            ("AFTER INSERT ON dungeon_team_members", "UPDATE dungeon_team_members SET version=9 WHERE team_id=NEW.team_id", "members_projection_mismatch"),
            ("BEFORE INSERT ON dungeon_team_operations", "SELECT RAISE(IGNORE)", "receipt_write_rejected"),
            ("AFTER INSERT ON dungeon_team_operations", "UPDATE dungeon_team_operations SET action='invite' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
            ("AFTER INSERT ON dungeon_team_operations", "UPDATE teams SET leader='other' WHERE user_id=NEW.team_id", "state_projection_mismatch"),
            ("AFTER INSERT ON dungeon_team_operations", "DELETE FROM dungeon_team_members WHERE team_id=NEW.team_id", "members_projection_mismatch"),
        )
        for timing, body, reason in faults:
            with self.subTest(timing=timing, reason=reason), tempfile.TemporaryDirectory() as directory:
                application = self.prepare(directory)
                before = self.state(application)
                with DatabaseUnitOfWork(application.repository.database) as uow:
                    uow.execute(
                        f"CREATE TRIGGER fail_create {timing} BEGIN "
                        f"INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END"
                    )
                failed = self.create(application)
                self.assertEqual((failed.status, failed.reason, failed.subject), ("failed", "dungeon_team_create_" + reason, "team"))
                self.assertEqual(self.state(application), before)
                self.assertIsNone(application.operation_result("create", "create"))
                with DatabaseUnitOfWork(application.repository.database) as uow:
                    uow.execute("DROP TRIGGER fail_create")
                self.assertEqual(self.create(application).status, "applied")
                applied = self.state(application)
                self.assertEqual(application.team_id_for_user("leader"), "team")
                self.assertEqual(application.snapshot("team").members, ("leader",))
                stored = applied["dungeon_team_operations"][0]
                self.assertEqual((stored["action"], stored["result_status"]), ("create", "applied"))
                self.assertEqual(json.loads(stored["result_json"])["subject"], "team")
                self.assertEqual(self.create(application).status, "applied")
                self.assertEqual(application.operation_result("create", "create").status, "duplicate")
                conflict = self.create(application, name="Different")
                self.assertEqual((conflict.status, conflict.reason), ("state_changed", "operation_conflict"))
                self.assertEqual(self.state(application), applied)
                with DatabaseUnitOfWork(application.repository.database, read_only=True) as uow:
                    self.assertEqual(uow.query_one("PRAGMA integrity_check"), {"integrity_check": "ok"})

    def test_abort_keeps_original_exception_and_same_request_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            application = self.prepare(directory)
            before = self.state(application)
            with DatabaseUnitOfWork(application.repository.database) as uow:
                uow.execute(
                    "CREATE TRIGGER abort_create BEFORE INSERT ON dungeon_team_operations BEGIN "
                    "INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','aborted'); "
                    "SELECT RAISE(ABORT,'isolated team create abort'); END"
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated team create abort"):
                self.create(application)
            self.assertEqual(self.state(application), before)
            with DatabaseUnitOfWork(application.repository.database) as uow:
                uow.execute("DROP TRIGGER abort_create")
            self.assertEqual(self.create(application).status, "applied")


if __name__ == "__main__":
    unittest.main()
