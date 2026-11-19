from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from .. import migrations
from ..team_application import DungeonTeamApplication


class DungeonTeamInviteWriteTests(unittest.TestCase):
    def prepare(self, directory):
        game, player = Path(directory) / "game.db", Path(directory) / "player.db"
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            uow.executemany("INSERT INTO user_xiuxian VALUES(?)", [("leader",), ("member",)])
        with DatabaseUnitOfWork(player) as uow:
            migrations.apply_dungeon_team_schema(uow)
            migrations.apply_dungeon_team_members_index(uow)
            migrations.apply_dungeon_team_invite_expiry(uow)
            uow.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
        app = DungeonTeamApplication(player, game_database=game)
        self.assertEqual(app.create("create", "team", "Trial", "leader", "g", "now", 1).status, "applied")
        with DatabaseUnitOfWork(player) as uow:
            uow.executemany(
                "INSERT INTO dungeon_team_invites(invite_id,team_id,inviter_id,invitee_id,group_id,created_at,expires_at,bot_id,source_message_id,notification_scene) VALUES(?,?,?,?,?,?,?,?,?,?)",
                [("old-1", "old-team", "old-leader", "member", "old-g", 40, 100, "old-bot", "old-message", "group"),
                 ("old-2", "old-team", "old-leader", "member", "old-g", 20, 80, "old-bot", "old-message-2", "group")],
            )
        return app

    @staticmethod
    def invite(app, *, operation="invite", target="member", now=100):
        return app.invite(operation, "new-invite", "team", "leader", target, "g", now + 60, now,
                          bot_id="bot", source_message_id="message", notification_scene="group")

    @staticmethod
    def state(app):
        with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
            return {table: uow.query_all(f"SELECT * FROM {table} ORDER BY rowid")
                    for table in ("teams", "dungeon_team_members", "dungeon_team_invites", "dungeon_team_operations", "team_cd")}

    def test_known_write_failures_roll_back_all_steps_and_recover(self):
        faults = (
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.invite_id='old-2'", "SELECT RAISE(IGNORE)", "expiry_write_rejected", "old-2"),
            ("AFTER UPDATE ON dungeon_team_invites", "OLD.invite_id='old-1'", "UPDATE dungeon_team_invites SET group_id='changed' WHERE invite_id=NEW.invite_id", "expiry_projection_mismatch", "old-1"),
            ("BEFORE INSERT ON dungeon_team_invites", "1", "SELECT RAISE(IGNORE)", "state_write_rejected", "member"),
            ("AFTER INSERT ON dungeon_team_invites", "1", "UPDATE dungeon_team_invites SET expires_at=999 WHERE invite_id=NEW.invite_id", "state_projection_mismatch", "member"),
            ("AFTER INSERT ON dungeon_team_invites", "1", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "state_projection_mismatch", "member"),
            ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='invite'", "SELECT RAISE(IGNORE)", "receipt_write_rejected", "member"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='invite'", "UPDATE dungeon_team_operations SET action='create' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch", "member"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='invite'", "UPDATE dungeon_team_invites SET status='joined' WHERE invite_id='new-invite'", "state_projection_mismatch", "member"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='invite'", "UPDATE dungeon_team_invites SET status='pending' WHERE invite_id='old-1'", "expiry_projection_mismatch", "old-1"),
            ("AFTER INSERT ON dungeon_team_invites", "1", "DELETE FROM dungeon_team_invites WHERE invite_id='old-2'", "expiry_projection_mismatch", "old-2"),
        )
        for timing, condition, body, reason, subject in faults:
            with self.subTest(reason=reason, body=body), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                before = self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER fail_invite {timing} WHEN {condition} BEGIN "
                                f"INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                failed = self.invite(app)
                self.assertEqual((failed.status, failed.reason, failed.subject), ("failed", "dungeon_team_invite_" + reason, subject))
                self.assertEqual(self.state(app), before)
                self.assertIsNone(app.operation_result("invite", "invite"))
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER fail_invite")
                result = self.invite(app)
                self.assertEqual(result.status, "applied")
                committed = self.state(app)
                self.assertEqual([(row["invite_id"], row["status"]) for row in committed["dungeon_team_invites"]],
                                 [("old-1", "expired"), ("old-2", "expired"), ("new-invite", "pending")])
                self.assertEqual(app.invite_by_id("new-invite").expires_at, 160)
                receipt = committed["dungeon_team_operations"][-1]
                self.assertEqual((receipt["action"], receipt["result_status"]), ("invite", "applied"))
                self.assertEqual(json.loads(receipt["result_json"])["target_id"], "member")
                self.assertEqual(self.invite(app, now=999).status, "applied")
                self.assertEqual(app.operation_result("invite", "invite").status, "duplicate")
                conflict = self.invite(app, target="leader")
                self.assertEqual((conflict.status, conflict.reason), ("state_changed", "operation_conflict"))
                self.assertEqual(self.state(app), committed)
                with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
                    self.assertEqual(uow.query_one("PRAGMA integrity_check"), {"integrity_check": "ok"})

    def test_abort_at_each_step_preserves_exception_and_allows_same_request_recovery(self):
        for timing, condition in (("BEFORE UPDATE ON dungeon_team_invites", "OLD.invite_id='old-2'"),
                                  ("BEFORE INSERT ON dungeon_team_invites", "1"),
                                  ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='invite'")):
            with self.subTest(timing=timing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                before = self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER abort_invite {timing} WHEN {condition} BEGIN "
                                "INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','aborted'); "
                                "SELECT RAISE(ABORT,'isolated invitation abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated invitation abort"):
                    self.invite(app)
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER abort_invite")
                self.assertEqual(self.invite(app).status, "applied")

    def test_deadline_replacement_and_original_replay_do_not_extend_or_reroute(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            early = self.invite(app, operation="early", now=99.999)
            self.assertEqual(early.status, "invite_pending")
            self.assertEqual(app.invite_by_id("old-1").status, "pending")
            self.assertEqual(app.invite_by_id("old-2").status, "expired")
            self.assertEqual(self.invite(app).status, "applied")
            self.assertIsNotNone(app.pending_invite("member", 159.999))
            self.assertIsNone(app.pending_invite("member", 160))
            committed = self.state(app)
            replay = app.invite("invite", "new-invite", "team", "leader", "member", "g", 999, 500,
                                bot_id="other", source_message_id="other", notification_scene="channel_group")
            self.assertEqual((replay.status, replay.expires_at), ("applied", 160))
            route = app.invite_by_id("new-invite")
            self.assertEqual((route.bot_id, route.source_message_id, route.notification_scene), ("bot", "message", "group"))
            self.assertEqual(self.state(app), committed)

    def test_rejected_command_receipt_failure_is_not_cached_as_normal_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            before = self.state(app)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("CREATE TRIGGER ignore_receipt BEFORE INSERT ON dungeon_team_operations WHEN NEW.action='invite' BEGIN SELECT RAISE(IGNORE); END")
            result = self.invite(app, target="")
            self.assertEqual((result.status, result.reason), ("failed", "dungeon_team_invite_receipt_write_rejected"))
            self.assertEqual(self.state(app), before)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("DROP TRIGGER ignore_receipt")
            self.assertEqual(self.invite(app, target="").status, "target_missing")


if __name__ == "__main__":
    unittest.main()
