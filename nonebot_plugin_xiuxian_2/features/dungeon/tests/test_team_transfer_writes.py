from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from .. import migrations
from ..team_application import DungeonTeamApplication
from ..team_repository import TeamStateSnapshot


class DungeonTeamTransferWriteTests(unittest.TestCase):
    @staticmethod
    def prepare(directory):
        game, player = Path(directory) / "game.db", Path(directory) / "player.db"
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            uow.executemany("INSERT INTO user_xiuxian VALUES(?)", [("leader",), ("member",), ("other",)])
        with DatabaseUnitOfWork(player) as uow:
            migrations.apply_dungeon_team_schema(uow)
            migrations.apply_dungeon_team_members_index(uow)
            uow.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
        app = DungeonTeamApplication(player, game_database=game)
        app.create("create", "team", "Trial", "leader", "g", "now", 1)
        app.invite("invite-op", "invite", "team", "leader", "member", "g", 160, 100)
        joined = app.join("join", "invite", "team", "leader", "member", "g", 101)
        if joined.status != "applied":
            raise AssertionError(joined)
        return app

    @staticmethod
    def state(app):
        with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
            return {table: uow.query_all(f"SELECT * FROM {table} ORDER BY rowid")
                    for table in ("teams", "dungeon_team_members", "team_cd", "dungeon_team_operations")}

    def test_each_transfer_write_failure_rolls_back_with_reason_and_recovers(self):
        faults = (
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(IGNORE)", "state_write_rejected"),
            ("AFTER UPDATE ON teams", "1", "UPDATE teams SET leader='changed' WHERE user_id=NEW.user_id", "state_projection_mismatch"),
            ("BEFORE UPDATE ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "members_write_rejected"),
            ("AFTER UPDATE ON dungeon_team_members", "1", "UPDATE dungeon_team_members SET version=99 WHERE team_id=NEW.team_id", "members_projection_mismatch"),
            ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='transfer'", "SELECT RAISE(IGNORE)", "receipt_write_rejected"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='transfer'", "UPDATE dungeon_team_operations SET action='create' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='transfer'", "UPDATE dungeon_team_members SET version=99 WHERE team_id='team'", "members_projection_mismatch"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='transfer'", "UPDATE teams SET members='[]' WHERE user_id='team'", "state_projection_mismatch"),
        )
        for index, (timing, condition, body, reason) in enumerate(faults):
            with self.subTest(reason=reason, body=body), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                snapshot = app.snapshot("team")
                before = self.state(app)
                operation = f"transfer-{index}"
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER fail_transfer {timing} WHEN {condition} BEGIN "
                                f"INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                failed = app.transfer(operation, "leader", "member", snapshot)
                self.assertEqual((failed.status, failed.reason, failed.subject),
                                 ("failed", "dungeon_team_transfer_" + reason, "team"))
                self.assertEqual(self.state(app), before)
                self.assertIsNone(app.operation_result(operation, "transfer"))
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER fail_transfer")
                recovered = app.transfer(operation, "leader", "member", snapshot)
                self.assertEqual(recovered.status, "applied")
                committed = self.state(app)
                self.assertEqual(committed["teams"][0]["leader"], "member")
                self.assertEqual([row["version"] for row in committed["dungeon_team_members"]], [2, 2])
                receipt = committed["dungeon_team_operations"][-1]
                self.assertEqual((receipt["action"], receipt["result_status"]), ("transfer", "applied"))
                self.assertEqual(json.loads(receipt["result_json"])["leader_id"], "member")
                replay = app.transfer(operation, "leader", "member", snapshot)
                self.assertEqual(replay.status, "applied")
                self.assertEqual(self.state(app), committed)
                for database in (app.repository.database, app.repository.game_database):
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        self.assertEqual(uow.query_one("PRAGMA integrity_check"), {"integrity_check": "ok"})

    def test_abort_at_each_transfer_write_preserves_exception_and_retry(self):
        for index, (timing, condition) in enumerate((
            ("BEFORE UPDATE ON teams", "1"),
            ("BEFORE UPDATE ON dungeon_team_members", "1"),
            ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='transfer'"),
        )):
            with self.subTest(timing=timing), tempfile.TemporaryDirectory() as directory:
                app = self.prepare(directory)
                snapshot = app.snapshot("team")
                before = self.state(app)
                operation = f"abort-{index}"
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute(f"CREATE TRIGGER abort_transfer {timing} WHEN {condition} BEGIN "
                                "INSERT INTO team_cd(user_id,join_cd_until) VALUES('aborted','touched'); "
                                "SELECT RAISE(ABORT,'isolated transfer abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated transfer abort"):
                    app.transfer(operation, "leader", "member", snapshot)
                self.assertEqual(self.state(app), before)
                with DatabaseUnitOfWork(app.repository.database) as uow:
                    uow.execute("DROP TRIGGER abort_transfer")
                self.assertEqual(app.transfer(operation, "leader", "member", snapshot).status, "applied")

    def test_normal_rejections_and_request_conflict_remain_specific(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            snapshot = app.snapshot("team")
            self.assertEqual(app.transfer("self", "leader", "leader", snapshot).status, "self_target")
            self.assertEqual(app.transfer("outsider", "leader", "other", snapshot).status, "target_not_member")
            self.assertEqual(app.transfer("nonleader", "member", "leader", snapshot).status, "actor_not_leader")
            applied = app.transfer("transfer", "leader", "member", snapshot)
            self.assertEqual(applied.status, "applied")
            changed = app.transfer("transfer", "leader", "leader", snapshot)
            self.assertEqual((changed.status, changed.reason), ("state_changed", "operation_conflict"))

    def test_missing_team_receipt_failure_is_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self.prepare(directory)
            before = self.state(app)
            missing = TeamStateSnapshot("missing", "", "", ())
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("CREATE TRIGGER ignore_transfer_receipt BEFORE INSERT ON dungeon_team_operations WHEN NEW.action='transfer' BEGIN SELECT RAISE(IGNORE); END")
            failed = app.transfer("missing-op", "other", "member", missing)
            self.assertEqual((failed.status, failed.reason, failed.subject),
                             ("failed", "dungeon_team_transfer_receipt_write_rejected", "missing"))
            self.assertEqual(self.state(app), before)
            with DatabaseUnitOfWork(app.repository.database) as uow:
                uow.execute("DROP TRIGGER ignore_transfer_receipt")
            self.assertEqual(app.transfer("missing-op", "other", "member", missing).status, "team_missing")


if __name__ == "__main__":
    unittest.main()
