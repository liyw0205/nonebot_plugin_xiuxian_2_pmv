from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from .. import migrations
from ..team_application import DungeonTeamApplication


class DungeonTeamJoinWriteTests(unittest.TestCase):
    def prepare(self, directory, *, existing_cooldown=False):
        game, player = Path(directory) / "game.db", Path(directory) / "player.db"
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            uow.executemany("INSERT INTO user_xiuxian VALUES(?)", [("leader",), ("member",)])
        with DatabaseUnitOfWork(player) as uow:
            migrations.apply_dungeon_team_schema(uow)
            migrations.apply_dungeon_team_members_index(uow)
            migrations.apply_dungeon_team_invite_expiry(uow)
            uow.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
            if existing_cooldown:
                uow.execute("INSERT INTO team_cd VALUES('member','original-cooldown',0)")
        app = DungeonTeamApplication(player, game_database=game)
        self.assertEqual(app.create("create", "team", "Trial", "leader", "g", "now", 1).status, "applied")
        self.assertEqual(app.invite("send", "invite", "team", "leader", "member", "g", 160, 100,
                                    bot_id="bot", source_message_id="message", notification_scene="group").status, "applied")
        return app

    @staticmethod
    def join(app, *, operation="join", invite="invite", group="g", now=101):
        return app.join(operation, invite, "team", "leader", "member", group, now)

    @staticmethod
    def state(app):
        with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
            return {table: uow.query_all(f"SELECT * FROM {table} ORDER BY rowid")
                    for table in ("teams", "dungeon_team_members", "dungeon_team_invites", "team_cd", "dungeon_team_operations")}

    def test_known_failures_roll_back_all_steps_and_recover_same_request(self):
        faults = (
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(IGNORE)", "state_write_rejected", "team", False),
            ("AFTER UPDATE ON teams", "1", "UPDATE teams SET leader='changed' WHERE user_id=NEW.user_id", "state_projection_mismatch", "team", False),
            ("BEFORE DELETE ON dungeon_team_members", "OLD.member_id='leader'", "SELECT RAISE(IGNORE)", "members_write_rejected", "team", False),
            ("AFTER DELETE ON dungeon_team_members", "OLD.member_id='leader'", "INSERT INTO dungeon_team_members VALUES('team','ghost',9)", "members_projection_mismatch", "team", False),
            ("BEFORE INSERT ON dungeon_team_members", "NEW.member_id='member'", "SELECT RAISE(IGNORE)", "members_write_rejected", "team", False),
            ("AFTER INSERT ON dungeon_team_members", "NEW.member_id='member'", "UPDATE dungeon_team_members SET version=99 WHERE team_id=NEW.team_id", "members_projection_mismatch", "team", False),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='member'", "SELECT RAISE(IGNORE)", "cooldown_write_rejected", "member", False),
            ("AFTER INSERT ON team_cd", "NEW.user_id='member'", "UPDATE team_cd SET join_cd_until='changed' WHERE user_id=NEW.user_id", "cooldown_projection_mismatch", "member", False),
            ("BEFORE UPDATE ON team_cd", "NEW.user_id='member'", "SELECT RAISE(IGNORE)", "cooldown_write_rejected", "member", True),
            ("AFTER UPDATE ON teams", "1", "UPDATE team_cd SET join_cd_until='changed' WHERE user_id='member'", "cooldown_projection_mismatch", "member", True),
            ("BEFORE UPDATE ON dungeon_team_invites", "1", "SELECT RAISE(IGNORE)", "invite_write_rejected", "invite", False),
            ("AFTER UPDATE ON dungeon_team_invites", "1", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "invite_projection_mismatch", "invite", False),
            ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='join'", "SELECT RAISE(IGNORE)", "receipt_write_rejected", "team", False),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='join'", "UPDATE dungeon_team_operations SET action='invite' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch", "team", False),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='join'", "UPDATE teams SET team_name='changed' WHERE user_id=NEW.team_id", "state_projection_mismatch", "team", False),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='join'", "DELETE FROM dungeon_team_members WHERE member_id='member'", "members_projection_mismatch", "team", False),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='join'", "UPDATE team_cd SET had_first_join=0 WHERE user_id='member'", "cooldown_projection_mismatch", "member", False),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='join'", "UPDATE dungeon_team_invites SET status='pending' WHERE invite_id='invite'", "invite_projection_mismatch", "invite", False),
        )
        for timing, condition, body, reason, subject, existing in faults:
            with self.subTest(reason=reason, body=body), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory, existing_cooldown=existing)
                before = self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER fail_join {timing} WHEN {condition} BEGIN "
                                f"INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                failed = self.join(app)
                self.assertEqual((failed.status, failed.reason, failed.subject), ("failed", "dungeon_team_join_" + reason, subject))
                self.assertEqual(self.state(app), before)
                self.assertIsNone(app.operation_result("join", "join"))
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER fail_join")
                applied = self.join(app)
                self.assertEqual((applied.status, applied.member_count, applied.version), ("applied", 2, 1))
                committed = self.state(app)
                self.assertEqual(app.snapshot("team").members, ("leader", "member"))
                self.assertEqual(app.team_id_for_user("member"), "team")
                self.assertEqual(committed["dungeon_team_members"], [
                    {"team_id": "team", "member_id": "leader", "version": 1},
                    {"team_id": "team", "member_id": "member", "version": 1},
                ])
                self.assertEqual(committed["team_cd"], [{"user_id": "member", "join_cd_until": "original-cooldown" if existing else "", "had_first_join": 1}])
                invite = committed["dungeon_team_invites"][0]
                self.assertEqual((invite["status"], invite["resolved_operation_id"], invite["bot_id"], invite["expires_at"]), ("joined", "join", "bot", 160))
                self.assertIsNotNone(invite["consumed_at"])
                receipt = committed["dungeon_team_operations"][-1]
                self.assertEqual((receipt["action"], receipt["result_status"]), ("join", "applied"))
                self.assertEqual(json.loads(receipt["result_json"])["member_count"], 2)
                self.assertEqual(self.join(app, now=999).status, "applied")
                self.assertEqual(app.operation_result("join", "join").status, "duplicate")
                conflict = self.join(app, invite="other-invite")
                self.assertEqual((conflict.status, conflict.reason), ("state_changed", "operation_conflict"))
                self.assertEqual(self.state(app), committed)
                for database in (app.repository.database, app.repository.game_database):
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        self.assertEqual(uow.query_one("PRAGMA integrity_check"), {"integrity_check": "ok"})

    def test_abort_at_each_step_retains_exception_rolls_back_and_recovers(self):
        faults = (("BEFORE UPDATE ON teams", "1"),
                  ("BEFORE INSERT ON dungeon_team_members", "NEW.member_id='member'"),
                  ("BEFORE INSERT ON team_cd", "NEW.user_id='member'"),
                  ("BEFORE UPDATE ON dungeon_team_invites", "1"),
                  ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='join'"))
        for timing, condition in faults:
            with self.subTest(timing=timing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                before = self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER abort_join {timing} WHEN {condition} BEGIN "
                                "INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','aborted'); "
                                "SELECT RAISE(ABORT,'isolated acceptance abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated acceptance abort"):
                    self.join(app)
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER abort_join")
                self.assertEqual(self.join(app).status, "applied")

    def test_exact_deadline_and_subject_failures_leave_invite_and_members_untouched(self):
        for kwargs, reason in (({"now": 160}, "team_invite_expired"), ({"invite": "missing"}, "team_invite_missing"),
                               ({"group": "wrong"}, "invite_subject_mismatch")):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                before = self.state(app)
                rejected = self.join(app, **kwargs)
                self.assertEqual((rejected.status, rejected.reason), ("invite_invalid", reason))
                after = self.state(app)
                for table in before.keys() - {"dungeon_team_operations"}:
                    self.assertEqual(after[table], before[table])
                self.assertEqual(json.loads(after["dungeon_team_operations"][-1]["result_json"])["reason"], reason)
                self.assertEqual(self.join(app, **kwargs), rejected)
                self.assertEqual(self.join(app, operation="corrected", now=159.999).status, "applied")

    def test_rejection_receipt_failure_rolls_back_and_can_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            before = self.state(app)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON dungeon_team_operations WHEN NEW.action='join' BEGIN SELECT RAISE(IGNORE); END")
            failed = self.join(app, now=160)
            self.assertEqual((failed.status, failed.reason, failed.subject), ("failed", "dungeon_team_join_receipt_write_rejected", "team"))
            self.assertEqual(self.state(app), before)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("DROP TRIGGER reject_receipt")
            rejected = self.join(app, now=160)
            self.assertEqual((rejected.status, rejected.reason), ("invite_invalid", "team_invite_expired"))


if __name__ == "__main__":
    unittest.main()
