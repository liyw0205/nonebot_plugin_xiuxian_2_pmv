from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from .. import migrations
from ..team_application import DungeonTeamApplication
from ..team_repository import TeamStateSnapshot


class DungeonTeamLeaveWriteTests(unittest.TestCase):
    def prepare(self, directory, *, joined=True, routing=True):
        game, player = Path(directory) / "game.db", Path(directory) / "player.db"
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            uow.executemany("INSERT INTO user_xiuxian VALUES(?)", [("leader",), ("member",), ("other",)])
        with DatabaseUnitOfWork(player) as uow:
            migrations.apply_dungeon_team_schema(uow)
            migrations.apply_dungeon_team_members_index(uow)
            if routing:
                migrations.apply_dungeon_team_invite_expiry(uow)
            uow.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
        app = DungeonTeamApplication(player, game_database=game)
        self.assertEqual(app.create("create", "team", "Trial", "leader", "g", "now", 1).status, "applied")
        route = dict(bot_id="bot", source_message_id="message", notification_scene="group") if routing else {}
        if joined:
            self.assertEqual(app.invite("send-member", "joined-invite", "team", "leader", "member", "g", 160, 100, **route).status, "applied")
            self.assertEqual(app.join("join", "joined-invite", "team", "leader", "member", "g", 101).status, "applied")
        self.assertEqual(app.invite("send-other", "pending-invite", "team", "leader", "other", "g", 180, 110, **route).status, "applied")
        return app

    @staticmethod
    def state(app):
        with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
            return {table: uow.query_all(f"SELECT * FROM {table} ORDER BY rowid")
                    for table in ("teams", "dungeon_team_members", "team_cd", "dungeon_team_invites",
                                  "dungeon_team_operations", "dungeon_team_exit_operations")}

    def test_write_failures_roll_back_every_stage_and_recover(self):
        faults = (
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(IGNORE)", "state_write_rejected", "team"),
            ("AFTER UPDATE ON teams", "1", "UPDATE teams SET description='changed' WHERE user_id=NEW.user_id", "state_projection_mismatch", "team"),
            ("BEFORE DELETE ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "members_write_rejected", "team"),
            ("BEFORE INSERT ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "members_write_rejected", "team"),
            ("AFTER INSERT ON dungeon_team_members", "1", "UPDATE dungeon_team_members SET version=99 WHERE team_id=NEW.team_id", "members_projection_mismatch", "team"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='member'", "SELECT RAISE(IGNORE)", "cooldown_write_rejected", "member"),
            ("AFTER UPDATE ON team_cd", "NEW.user_id='member'", "UPDATE team_cd SET had_first_join=0 WHERE user_id=NEW.user_id", "cooldown_projection_mismatch", "member"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.invite_id='pending-invite'", "SELECT RAISE(IGNORE)", "invite_write_rejected", "pending-invite"),
            ("AFTER UPDATE ON dungeon_team_invites", "NEW.invite_id='pending-invite'", "UPDATE dungeon_team_invites SET group_id='changed' WHERE invite_id=NEW.invite_id", "invite_projection_mismatch", "pending-invite"),
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
                snapshot = app.snapshot("team")
                before = self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER fail_leave {timing} WHEN {condition} BEGIN "
                                f"INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                failed = app.leave("leave", "member", snapshot, "later")
                self.assertEqual((failed.status, failed.reason, getattr(failed, "subject", "")),
                                 ("failed", "dungeon_team_leave_" + reason, subject))
                self.assertEqual(self.state(app), before)
                self.assertIsNone(app.exit_operation_result("leave", "leave", "member"))
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER fail_leave")
                recovered = app.leave("leave", "member", snapshot, "later")
                self.assertEqual((recovered.status, recovered.cooldown_members), ("applied", ("member",)))
                committed = self.state(app)
                self.assertEqual((committed["teams"][0]["members"], committed["teams"][0]["version"]), ('["leader"]', 2))
                self.assertEqual(committed["dungeon_team_members"], [{"team_id": "team", "member_id": "leader", "version": 2}])
                self.assertEqual(committed["team_cd"], [{"user_id": "member", "join_cd_until": "later", "had_first_join": 1}])
                pending = next(row for row in committed["dungeon_team_invites"] if row["invite_id"] == "pending-invite")
                self.assertEqual((pending["status"], pending["resolved_operation_id"], pending["bot_id"]), ("team_changed", "leave", "bot"))
                self.assertIsNotNone(pending["consumed_at"])
                original = next(row for row in before["dungeon_team_invites"] if row["invite_id"] == "joined-invite")
                self.assertIn(original, committed["dungeon_team_invites"])
                self.assertEqual(json.loads(committed["dungeon_team_exit_operations"][0]["result_json"])["status"], "applied")
                self.assertEqual(app.leave("leave", "member", app.snapshot("team"), "different").status, "duplicate")
                self.assertEqual(self.state(app), committed)
                for database in (app.repository.database, app.repository.game_database):
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        self.assertEqual(uow.query_one("PRAGMA integrity_check"), {"integrity_check": "ok"})

    def test_abort_at_each_stage_preserves_exception_and_entire_state(self):
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
                    uow.execute(f"CREATE TRIGGER abort_leave {timing} WHEN {condition} BEGIN "
                                "INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','aborted'); "
                                "SELECT RAISE(ABORT,'isolated departure abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated departure abort"):
                    app.leave("leave", "member", snapshot, "later")
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER abort_leave")
                self.assertEqual(app.leave("leave", "member", snapshot, "later").status, "applied")

    def test_solo_disband_delete_faults_roll_back_and_replay_without_team(self):
        for timing, body, reason in (
            ("BEFORE DELETE ON teams", "SELECT RAISE(IGNORE)", "state_write_rejected"),
            ("BEFORE DELETE ON dungeon_team_members", "SELECT RAISE(IGNORE)", "members_write_rejected"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "INSERT INTO teams(user_id) VALUES('team')", "state_projection_mismatch"),
        ):
            with self.subTest(timing=timing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory, joined=False)
                snapshot, before = app.snapshot("team"), self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER solo_fault {timing} BEGIN {body}; END")
                failed = app.leave("leave", "leader", snapshot, "later")
                self.assertEqual((failed.status, failed.reason), ("failed", "dungeon_team_leave_" + reason))
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER solo_fault")
                applied = app.leave("leave", "leader", snapshot, "later")
                self.assertTrue(applied.disbanded)
                self.assertEqual(applied.cooldown_members, ())
                self.assertIsNone(app.snapshot("team"))
                self.assertEqual(self.state(app)["team_cd"], [])
                self.assertEqual(app.leave("leave", "leader", snapshot, "different").status, "duplicate")

    def test_normal_rejections_missing_team_and_legacy_routing_contracts(self):
        for routing in (False, True):
            with self.subTest(routing=routing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory, routing=routing)
                snapshot = app.snapshot("team")
                stale = app.leave("stale", "member", replace(snapshot, version=99), "later")
                self.assertEqual((stale.status, stale.reason), ("state_changed", "team_version_changed"))
                self.assertEqual(app.leave("outsider", "other", snapshot, "later").status, "actor_not_member")
                self.assertEqual(app.leave("missing", "other", TeamStateSnapshot("missing", "", "", ()), "later").status, "team_missing")
                before = self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("CREATE TRIGGER missing_fault BEFORE INSERT ON dungeon_team_exit_operations BEGIN SELECT RAISE(IGNORE); END")
                failed = app.leave("missing-fault", "other", TeamStateSnapshot("missing", "", "", ()), "later")
                self.assertEqual((failed.status, failed.reason), ("failed", "dungeon_team_leave_receipt_write_rejected"))
                self.assertEqual(self.state(app), before)

    def test_later_invite_failure_rolls_back_earlier_invite_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            with DatabaseUnitOfWork(app.repository.game_database) as uow:
                uow.execute("INSERT INTO user_xiuxian VALUES('last')")
            self.assertEqual(app.invite("send-last", "second-pending", "team", "leader", "last", "g", 180, 110).status, "applied")
            snapshot, before = app.snapshot("team"), self.state(app)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("CREATE TRIGGER later_invite_fault BEFORE UPDATE ON dungeon_team_invites "
                            "WHEN OLD.invite_id='second-pending' BEGIN SELECT RAISE(IGNORE); END")
            failed = app.leave("leave", "member", snapshot, "later")
            self.assertEqual((failed.status, failed.reason, failed.subject),
                             ("failed", "dungeon_team_leave_invite_write_rejected", "second-pending"))
            self.assertEqual(self.state(app), before)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("DROP TRIGGER later_invite_fault")
            self.assertEqual(app.leave("leave", "member", snapshot, "later").status, "applied")
            with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
                self.assertEqual(uow.query_all("SELECT invite_id,status,resolved_operation_id FROM dungeon_team_invites "
                                               "WHERE invite_id IN ('pending-invite','second-pending') ORDER BY invite_id"), [
                    {"invite_id": invite, "status": "team_changed", "resolved_operation_id": "leave"}
                    for invite in ("pending-invite", "second-pending")
                ])

    def test_historical_receipt_without_subject_replays_and_conflicts_without_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            self.assertEqual(app.leave("leave", "member", app.snapshot("team"), "original").status, "applied")
            with DatabaseUnitOfWork(app.repository.database) as uow:
                receipt = uow.query_one("SELECT result_json FROM dungeon_team_exit_operations WHERE operation_id='leave'")
                data = json.loads(receipt["result_json"])
                data.pop("subject")
                uow.execute("UPDATE dungeon_team_exit_operations SET result_json=? WHERE operation_id='leave'", (json.dumps(data),))
            before = self.state(app)
            replay = app.leave("leave", "member", app.snapshot("team"), "different")
            self.assertEqual((replay.status, replay.subject, replay.cooldown_until), ("duplicate", "", "original"))
            self.assertEqual(app.exit_operation_result("leave", "leave", "member"), replay)
            conflict = app.leave("leave", "leader", app.snapshot("team"), "different")
            self.assertEqual((conflict.status, conflict.reason), ("state_changed", "operation_conflict"))
            self.assertEqual(self.state(app), before)


if __name__ == "__main__":
    unittest.main()
