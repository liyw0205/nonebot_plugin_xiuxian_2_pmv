from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace

from ....infrastructure.database import DatabaseUnitOfWork
from ..team_repository import TeamStateSnapshot
from . import test_team_leave_writes as fixtures


class DungeonTeamDisbandWriteTests(unittest.TestCase):
    state = staticmethod(fixtures.DungeonTeamLeaveWriteTests.state)

    def prepare(self, directory, *, routing=True):
        app = fixtures.DungeonTeamLeaveWriteTests.prepare(self, directory, routing=routing)
        with DatabaseUnitOfWork(app.repository.database) as uow:
            uow.execute("INSERT INTO team_cd(user_id,join_cd_until,had_first_join) VALUES('leader','original',1)")
        return app

    def test_write_failures_roll_back_all_members_and_recover(self):
        faults = (
            ("BEFORE DELETE ON teams", "1", "SELECT RAISE(IGNORE)", "state_write_rejected", "team"),
            ("AFTER DELETE ON teams", "1", "INSERT INTO teams(user_id) VALUES(OLD.user_id)", "state_projection_mismatch", "team"),
            ("BEFORE DELETE ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "members_write_rejected", "team"),
            ("AFTER DELETE ON dungeon_team_members", "1", "INSERT INTO dungeon_team_members VALUES(OLD.team_id,OLD.member_id,99)", "members_projection_mismatch", "team"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='member'", "SELECT RAISE(IGNORE)", "cooldown_write_rejected", "member"),
            ("AFTER UPDATE ON team_cd", "NEW.user_id='member'", "UPDATE team_cd SET had_first_join=0 WHERE user_id=NEW.user_id", "cooldown_projection_mismatch", "member"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.invite_id='pending-invite'", "SELECT RAISE(IGNORE)", "invite_write_rejected", "pending-invite"),
            ("AFTER UPDATE ON dungeon_team_invites", "NEW.invite_id='pending-invite'", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "invite_projection_mismatch", "pending-invite"),
            ("BEFORE INSERT ON dungeon_team_exit_operations", "1", "SELECT RAISE(IGNORE)", "receipt_write_rejected", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE dungeon_team_exit_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "INSERT INTO teams(user_id) VALUES('team')", "state_projection_mismatch", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "INSERT INTO dungeon_team_members VALUES('team','leader',99)", "members_projection_mismatch", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE team_cd SET join_cd_until='changed' WHERE user_id='member'", "cooldown_projection_mismatch", "member"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "DELETE FROM dungeon_team_invites WHERE invite_id='pending-invite'", "invite_projection_mismatch", "pending-invite"),
            ("AFTER DELETE ON teams", "1", "UPDATE team_cd SET had_first_join=0 WHERE user_id='member'", "cooldown_projection_mismatch", "member"),
            ("AFTER UPDATE ON team_cd", "NEW.user_id='leader'", "UPDATE team_cd SET had_first_join=0 WHERE user_id='member'", "cooldown_projection_mismatch", "member"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE team_cd SET join_cd_until='changed' WHERE user_id='leader'", "cooldown_projection_mismatch", "leader"),
        )
        for timing, condition, body, reason, subject in faults:
            with self.subTest(reason=reason, body=body), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                snapshot, before = app.snapshot("team"), self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER disband_fault {timing} WHEN {condition} BEGIN "
                                f"INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                failed = app.disband("disband", "leader", snapshot, "later")
                self.assertEqual((failed.status, failed.reason, failed.subject),
                                 ("failed", "dungeon_team_disband_" + reason, subject))
                self.assertEqual(self.state(app), before)
                self.assertIsNone(app.exit_operation_result("disband", "disband", "leader"))
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER disband_fault")
                applied = app.disband("disband", "leader", snapshot, "later")
                self.assertEqual((applied.status, applied.disbanded, applied.cooldown_members), ("applied", True, ("leader", "member")))
                committed = self.state(app)
                self.assertEqual(committed["teams"], [])
                self.assertEqual(committed["dungeon_team_members"], [])
                self.assertEqual(committed["team_cd"], [
                    {"user_id": "member", "join_cd_until": "later", "had_first_join": 1},
                    {"user_id": "leader", "join_cd_until": "later", "had_first_join": 1},
                ])
                pending = next(row for row in committed["dungeon_team_invites"] if row["invite_id"] == "pending-invite")
                self.assertEqual((pending["status"], pending["resolved_operation_id"], pending["bot_id"]), ("team_changed", "disband", "bot"))
                self.assertIn(next(row for row in before["dungeon_team_invites"] if row["invite_id"] == "joined-invite"), committed["dungeon_team_invites"])
                self.assertEqual(json.loads(committed["dungeon_team_exit_operations"][0]["result_json"])["status"], "applied")
                self.assertEqual(app.disband("disband", "leader", snapshot, "different").status, "duplicate")
                self.assertEqual(self.state(app), committed)
                for database in (app.repository.database, app.repository.game_database):
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        self.assertEqual(uow.query_one("PRAGMA integrity_check"), {"integrity_check": "ok"})

    def test_abort_preserves_original_exception_and_entire_state(self):
        for timing, condition in (
            ("BEFORE DELETE ON teams", "1"),
            ("BEFORE DELETE ON dungeon_team_members", "1"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='leader'"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='member'"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.invite_id='pending-invite'"),
            ("BEFORE INSERT ON dungeon_team_exit_operations", "1"),
        ):
            with self.subTest(timing=timing, condition=condition), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                snapshot, before = app.snapshot("team"), self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER disband_abort {timing} WHEN {condition} BEGIN "
                                "INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','aborted'); "
                                "SELECT RAISE(ABORT,'isolated dissolution abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated dissolution abort"):
                    app.disband("disband", "leader", snapshot, "later")
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER disband_abort")
                self.assertEqual(app.disband("disband", "leader", snapshot, "later").status, "applied")

    def test_rejection_receipts_legacy_routing_and_legacy_result(self):
        for routing in (False, True):
            with self.subTest(routing=routing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory, routing=routing)
                snapshot = app.snapshot("team")
                stale = app.disband("stale", "leader", replace(snapshot, version=99), "later")
                self.assertEqual((stale.status, stale.reason), ("state_changed", "team_version_changed"))
                self.assertEqual(app.disband("outsider", "other", snapshot, "later").status, "actor_not_member")
                self.assertEqual(app.disband("member", "member", snapshot, "later").status, "actor_not_leader")
                self.assertEqual(app.disband("disband", "leader", snapshot, "original").status, "applied")
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    data = json.loads(uow.query_one("SELECT result_json FROM dungeon_team_exit_operations WHERE operation_id='disband'")["result_json"])
                    data.pop("subject")
                    uow.execute("UPDATE dungeon_team_exit_operations SET result_json=? WHERE operation_id='disband'", (json.dumps(data),))
                before = self.state(app)
                replay = app.disband("disband", "leader", snapshot, "different")
                self.assertEqual((replay.status, replay.cooldown_until, replay.subject), ("duplicate", "original", ""))
                self.assertEqual(app.exit_operation_result("disband", "disband", "leader"), replay)
                conflict = app.disband("disband", "member", snapshot, "different")
                self.assertEqual((conflict.status, conflict.reason), ("state_changed", "operation_conflict"))
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("CREATE TRIGGER missing_fault BEFORE INSERT ON dungeon_team_exit_operations BEGIN SELECT RAISE(IGNORE); END")
                missing = TeamStateSnapshot("missing", "", "", ("leader",))
                failed = app.disband("missing", "leader", missing, "later")
                self.assertEqual((failed.status, failed.reason), ("failed", "dungeon_team_disband_receipt_write_rejected"))
                self.assertEqual(self.state(app), before)

    def test_second_pending_invite_failure_rolls_back_first_and_all_cooldowns(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            with DatabaseUnitOfWork(app.repository.game_database) as uow:
                uow.execute("INSERT INTO user_xiuxian VALUES('last')")
            self.assertEqual(app.invite("send-last", "second-pending", "team", "leader", "last", "g", 180, 110).status, "applied")
            snapshot, before = app.snapshot("team"), self.state(app)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("CREATE TRIGGER later_invite_fault BEFORE UPDATE ON dungeon_team_invites "
                            "WHEN OLD.invite_id='second-pending' BEGIN SELECT RAISE(IGNORE); END")
            failed = app.disband("disband", "leader", snapshot, "later")
            self.assertEqual((failed.status, failed.reason, failed.subject),
                             ("failed", "dungeon_team_disband_invite_write_rejected", "second-pending"))
            self.assertEqual(self.state(app), before)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("DROP TRIGGER later_invite_fault")
            self.assertEqual(app.disband("disband", "leader", snapshot, "later").status, "applied")
            with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
                self.assertEqual(uow.query_all("SELECT invite_id,status,resolved_operation_id FROM dungeon_team_invites "
                                               "WHERE invite_id IN ('pending-invite','second-pending') ORDER BY invite_id"), [
                    {"invite_id": invite, "status": "team_changed", "resolved_operation_id": "disband"}
                    for invite in ("pending-invite", "second-pending")
                ])


if __name__ == "__main__":
    unittest.main()
