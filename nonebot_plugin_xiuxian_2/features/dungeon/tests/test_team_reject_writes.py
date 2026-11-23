from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from .. import migrations
from ..team_application import DungeonTeamApplication


class DungeonTeamRejectWriteTests(unittest.TestCase):
    def prepare(self, directory, *, routing=True):
        game, player = Path(directory) / "game.db", Path(directory) / "player.db"
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            uow.executemany("INSERT INTO user_xiuxian VALUES(?)", [("leader",), ("member",)])
        with DatabaseUnitOfWork(player) as uow:
            migrations.apply_dungeon_team_schema(uow)
            migrations.apply_dungeon_team_members_index(uow)
            if routing:
                migrations.apply_dungeon_team_invite_expiry(uow)
            uow.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
        app = DungeonTeamApplication(player, game_database=game)
        self.assertEqual(app.create("create", "team", "Trial", "leader", "g", "now", 1).status, "applied")
        route = dict(bot_id="bot", source_message_id="message", notification_scene="group") if routing else {}
        self.assertEqual(app.invite("send", "invite", "team", "leader", "member", "g", 160, 100, **route).status, "applied")
        return app

    @staticmethod
    def reject(app, *, operation="reject", invite="invite", user="member", group="g", now=101):
        return app.reject(operation, invite, user, group, now)

    @staticmethod
    def state(app):
        with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
            return {table: uow.query_all(f"SELECT * FROM {table} ORDER BY rowid")
                    for table in ("teams", "dungeon_team_members", "dungeon_team_invites", "team_cd", "dungeon_team_operations")}

    def test_write_failures_keep_reason_roll_back_everything_and_recover(self):
        faults = (
            ("BEFORE UPDATE ON dungeon_team_invites", "1", "SELECT RAISE(IGNORE)", "state_write_rejected"),
            ("AFTER UPDATE ON dungeon_team_invites", "1", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "state_projection_mismatch"),
            ("AFTER UPDATE ON dungeon_team_invites", "1", "UPDATE dungeon_team_invites SET resolved_operation_id='changed' WHERE invite_id=NEW.invite_id", "state_projection_mismatch"),
            ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='reject'", "SELECT RAISE(IGNORE)", "receipt_write_rejected"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='reject'", "UPDATE dungeon_team_operations SET action='invite' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='reject'", "UPDATE dungeon_team_invites SET status='pending' WHERE invite_id='invite'", "state_projection_mismatch"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='reject'", "DELETE FROM dungeon_team_invites WHERE invite_id='invite'", "state_projection_mismatch"),
        )
        for timing, condition, body, reason in faults:
            with self.subTest(reason=reason, body=body), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                before = self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER fail_reject {timing} WHEN {condition} BEGIN "
                                f"INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                failed = self.reject(app)
                self.assertEqual((failed.status, failed.reason, failed.subject), ("failed", "dungeon_team_reject_" + reason, "invite"))
                self.assertEqual(self.state(app), before)
                self.assertIsNone(app.operation_result("reject", "reject"))
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER fail_reject")
                self.assertEqual(self.reject(app).status, "applied")
                committed = self.state(app)
                invite = committed["dungeon_team_invites"][0]
                self.assertEqual((invite["status"], invite["resolved_operation_id"], invite["bot_id"], invite["expires_at"]), ("rejected", "reject", "bot", 160))
                self.assertIsNotNone(invite["consumed_at"])
                for table in ("teams", "dungeon_team_members", "team_cd"):
                    self.assertEqual(committed[table], before[table])
                receipt = committed["dungeon_team_operations"][-1]
                self.assertEqual((receipt["action"], receipt["result_status"]), ("reject", "applied"))
                self.assertEqual(json.loads(receipt["result_json"])["invite_id"], "invite")
                self.assertEqual(self.reject(app, now=999).status, "applied")
                self.assertEqual(app.operation_result("reject", "reject").status, "duplicate")
                conflict = self.reject(app, invite="other")
                self.assertEqual((conflict.status, conflict.reason), ("state_changed", "operation_conflict"))
                self.assertEqual(self.state(app), committed)
                for database in (app.repository.database, app.repository.game_database):
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        self.assertEqual(uow.query_one("PRAGMA integrity_check"), {"integrity_check": "ok"})

    def test_abort_at_either_write_keeps_original_exception_and_recovers(self):
        for timing, condition in (("BEFORE UPDATE ON dungeon_team_invites", "1"),
                                  ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='reject'")):
            with self.subTest(timing=timing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                before = self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER abort_reject {timing} WHEN {condition} BEGIN "
                                "INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','aborted'); "
                                "SELECT RAISE(ABORT,'isolated refusal abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated refusal abort"):
                    self.reject(app)
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER abort_reject")
                self.assertEqual(self.reject(app).status, "applied")

    def test_known_subject_failures_retain_reason_in_receipt_and_replay(self):
        for kwargs, reason in (({"invite": "missing"}, "team_invite_missing"), ({"user": "other"}, "invite_subject_mismatch")):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                before = self.state(app)
                rejected = self.reject(app, **kwargs)
                self.assertEqual((rejected.status, rejected.reason, rejected.subject), ("invite_invalid", reason, kwargs.get("invite", "invite")))
                after = self.state(app)
                for table in before.keys() - {"dungeon_team_operations"}:
                    self.assertEqual(before[table], after[table])
                self.assertEqual(json.loads(after["dungeon_team_operations"][-1]["result_json"])["reason"], reason)
                self.assertEqual(self.reject(app, **kwargs), rejected)

    def test_original_late_pending_and_legacy_schema_replay_contracts_remain(self):
        for routing in (False, True):
            with self.subTest(routing=routing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory, routing=routing)
                wrong_group = self.reject(app, operation="wrong", group="other")
                self.assertEqual((wrong_group.status, wrong_group.group_id), ("wrong_group", "g"))
                self.assertEqual(self.reject(app, now=999).status, "applied")
                resolved = self.reject(app, operation="resolved")
                self.assertEqual((resolved.status, resolved.reason), ("invite_invalid", "team_invite_resolved"))
                committed = self.state(app)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TABLE dungeon_team_invites")
                replay = self.reject(app)
                self.assertEqual(replay.status, "applied")
                with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
                    self.assertEqual(uow.query_all("SELECT * FROM dungeon_team_operations ORDER BY rowid"), committed["dungeon_team_operations"])

    def test_normal_rejection_receipt_failure_is_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            before = self.state(app)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("CREATE TRIGGER ignore_receipt BEFORE INSERT ON dungeon_team_operations WHEN NEW.action='reject' BEGIN SELECT RAISE(IGNORE); END")
            failed = self.reject(app, group="wrong")
            self.assertEqual((failed.status, failed.reason), ("failed", "dungeon_team_reject_receipt_write_rejected"))
            self.assertEqual(self.state(app), before)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("DROP TRIGGER ignore_receipt")
            self.assertEqual(self.reject(app, group="wrong").status, "wrong_group")

    def test_failed_group_check_cannot_commit_receipt_side_effect_on_invite(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            before = self.state(app)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("CREATE TRIGGER change_invite AFTER INSERT ON dungeon_team_operations WHEN NEW.action='reject' BEGIN UPDATE dungeon_team_invites SET status='rejected' WHERE invite_id='invite'; END")
            failed = self.reject(app, group="wrong")
            self.assertEqual((failed.status, failed.reason, failed.subject), ("failed", "dungeon_team_reject_state_projection_mismatch", "invite"))
            self.assertEqual(self.state(app), before)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("DROP TRIGGER change_invite")
            self.assertEqual(self.reject(app, group="wrong").status, "wrong_group")


if __name__ == "__main__":
    unittest.main()
