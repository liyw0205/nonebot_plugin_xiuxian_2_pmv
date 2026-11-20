from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest

from ....infrastructure.database import DatabaseUnitOfWork
from ..team_repository import TeamStateSnapshot
from . import test_team_leave_writes as fixtures


class DungeonTeamKickWriteTests(unittest.TestCase):
    prepare = fixtures.DungeonTeamLeaveWriteTests.prepare
    state = staticmethod(fixtures.DungeonTeamLeaveWriteTests.state)

    def test_write_failures_roll_back_all_stages_and_recover(self):
        faults = (
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(IGNORE)", "state_write_rejected", "team"),
            ("AFTER UPDATE ON teams", "1", "UPDATE teams SET description='changed' WHERE user_id=NEW.user_id", "state_projection_mismatch", "team"),
            ("BEFORE DELETE ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "members_write_rejected", "team"),
            ("BEFORE INSERT ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "members_write_rejected", "team"),
            ("AFTER INSERT ON dungeon_team_members", "1", "UPDATE dungeon_team_members SET version=99 WHERE team_id=NEW.team_id", "members_projection_mismatch", "team"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='member'", "SELECT RAISE(IGNORE)", "cooldown_write_rejected", "member"),
            ("AFTER UPDATE ON team_cd", "NEW.user_id='member'", "UPDATE team_cd SET had_first_join=0 WHERE user_id=NEW.user_id", "cooldown_projection_mismatch", "member"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.invite_id='pending-invite'", "SELECT RAISE(IGNORE)", "invite_write_rejected", "pending-invite"),
            ("AFTER UPDATE ON dungeon_team_invites", "NEW.invite_id='pending-invite'", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "invite_projection_mismatch", "pending-invite"),
            ("BEFORE INSERT ON dungeon_team_exit_operations", "1", "SELECT RAISE(IGNORE)", "receipt_write_rejected", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE dungeon_team_exit_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE teams SET leader='changed' WHERE user_id='team'", "state_projection_mismatch", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "DELETE FROM dungeon_team_members WHERE team_id='team'", "members_projection_mismatch", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE team_cd SET join_cd_until='changed' WHERE user_id='member'", "cooldown_projection_mismatch", "member"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "DELETE FROM dungeon_team_invites WHERE invite_id='pending-invite'", "invite_projection_mismatch", "pending-invite"),
            ("AFTER UPDATE ON teams", "1", "UPDATE team_cd SET had_first_join=0 WHERE user_id='member'", "cooldown_projection_mismatch", "member"),
        )
        for timing, condition, body, reason, subject in faults:
            with self.subTest(reason=reason, body=body), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                snapshot, before = app.snapshot("team"), self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER kick_fault {timing} WHEN {condition} BEGIN "
                                f"INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                failed = app.kick("kick", "leader", "member", snapshot, "later")
                self.assertEqual((failed.status, failed.reason, failed.subject),
                                 ("failed", "dungeon_team_kick_" + reason, subject))
                self.assertEqual(self.state(app), before)
                self.assertIsNone(app.exit_operation_result("kick", "kick", "leader", "member"))
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER kick_fault")
                recovered = app.kick("kick", "leader", "member", snapshot, "later")
                self.assertEqual((recovered.status, recovered.cooldown_members), ("applied", ("member",)))
                committed = self.state(app)
                self.assertEqual((committed["teams"][0]["members"], committed["teams"][0]["version"]), ('["leader"]', 2))
                self.assertEqual(committed["dungeon_team_members"], [{"team_id": "team", "member_id": "leader", "version": 2}])
                self.assertEqual(committed["team_cd"], [{"user_id": "member", "join_cd_until": "later", "had_first_join": 1}])
                pending = next(row for row in committed["dungeon_team_invites"] if row["invite_id"] == "pending-invite")
                self.assertEqual((pending["status"], pending["resolved_operation_id"], pending["bot_id"]), ("team_changed", "kick", "bot"))
                self.assertIn(next(row for row in before["dungeon_team_invites"] if row["invite_id"] == "joined-invite"), committed["dungeon_team_invites"])
                self.assertEqual(json.loads(committed["dungeon_team_exit_operations"][0]["result_json"])["status"], "applied")
                self.assertEqual(app.kick("kick", "leader", "member", app.snapshot("team"), "different").status, "duplicate")
                self.assertEqual(self.state(app), committed)
                for database in (app.repository.database, app.repository.game_database):
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        self.assertEqual(uow.query_one("PRAGMA integrity_check"), {"integrity_check": "ok"})

    def test_abort_preserves_exception_and_entire_state(self):
        for timing, condition in (
            ("BEFORE UPDATE ON teams", "1"),
            ("BEFORE DELETE ON dungeon_team_members", "1"),
            ("BEFORE INSERT ON dungeon_team_members", "1"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='member'"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.invite_id='pending-invite'"),
            ("BEFORE INSERT ON dungeon_team_exit_operations", "1"),
        ):
            with self.subTest(timing=timing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                snapshot, before = app.snapshot("team"), self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER kick_abort {timing} WHEN {condition} BEGIN "
                                "INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','aborted'); "
                                "SELECT RAISE(ABORT,'isolated removal abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated removal abort"):
                    app.kick("kick", "leader", "member", snapshot, "later")
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER kick_abort")
                self.assertEqual(app.kick("kick", "leader", "member", snapshot, "later").status, "applied")

    def test_legacy_receipt_and_normal_rejection_receipt_failure(self):
        for routing in (False, True):
            with self.subTest(routing=routing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory, routing=routing)
                self.assertEqual(app.kick("kick", "leader", "member", app.snapshot("team"), "original").status, "applied")
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    data = json.loads(uow.query_one("SELECT result_json FROM dungeon_team_exit_operations WHERE operation_id='kick'")["result_json"])
                    data.pop("subject")
                    uow.execute("UPDATE dungeon_team_exit_operations SET result_json=? WHERE operation_id='kick'", (json.dumps(data),))
                before = self.state(app)
                replay = app.kick("kick", "leader", "member", app.snapshot("team"), "different")
                self.assertEqual((replay.status, replay.cooldown_until, replay.subject), ("duplicate", "original", ""))
                self.assertEqual(app.exit_operation_result("kick", "kick", "leader", "member"), replay)
                conflict = app.kick("kick", "leader", "other", app.snapshot("team"), "different")
                self.assertEqual((conflict.status, conflict.reason), ("state_changed", "operation_conflict"))
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("CREATE TRIGGER missing_fault BEFORE INSERT ON dungeon_team_exit_operations BEGIN SELECT RAISE(IGNORE); END")
                failed = app.kick("missing", "leader", "member", TeamStateSnapshot("missing", "", "", ()), "later")
                self.assertEqual((failed.status, failed.reason), ("failed", "dungeon_team_kick_receipt_write_rejected"))
                self.assertEqual(self.state(app), before)

    def test_later_pending_invite_failure_rolls_back_earlier_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            with DatabaseUnitOfWork(app.repository.game_database) as uow:
                uow.execute("INSERT INTO user_xiuxian VALUES('last')")
            self.assertEqual(app.invite("send-last", "second-pending", "team", "leader", "last", "g", 180, 110).status, "applied")
            snapshot, before = app.snapshot("team"), self.state(app)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("CREATE TRIGGER later_invite_fault BEFORE UPDATE ON dungeon_team_invites "
                            "WHEN OLD.invite_id='second-pending' BEGIN SELECT RAISE(IGNORE); END")
            failed = app.kick("kick", "leader", "member", snapshot, "later")
            self.assertEqual((failed.status, failed.reason, failed.subject),
                             ("failed", "dungeon_team_kick_invite_write_rejected", "second-pending"))
            self.assertEqual(self.state(app), before)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("DROP TRIGGER later_invite_fault")
            self.assertEqual(app.kick("kick", "leader", "member", snapshot, "later").status, "applied")
            with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
                self.assertEqual(uow.query_all("SELECT invite_id,status,resolved_operation_id FROM dungeon_team_invites "
                                               "WHERE invite_id IN ('pending-invite','second-pending') ORDER BY invite_id"), [
                    {"invite_id": invite, "status": "team_changed", "resolved_operation_id": "kick"}
                    for invite in ("pending-invite", "second-pending")
                ])


if __name__ == "__main__":
    unittest.main()
