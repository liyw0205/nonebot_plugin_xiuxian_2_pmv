from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_dungeon_team_members_index, apply_dungeon_team_schema
from ..repository import DungeonSessionSqlRepository
from ..team_repository import DungeonTeamRepository


class DungeonTeamMembersProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "player.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_dungeon_team_schema(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            uow.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
            for user in ("leader", "member", "other", "last"):
                uow.execute("INSERT INTO user_xiuxian VALUES(?)", (user,))
                uow.execute("INSERT INTO player_dungeon_status VALUES(?,'not_started')", (user,))
            apply_dungeon_team_members_index(uow)
        self.repo = DungeonTeamRepository(self.database)

    def projection(self):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_all("SELECT team_id,member_id,version FROM dungeon_team_members ORDER BY team_id,member_id")

    def create(self):
        self.assertEqual(self.repo.create("create", "team", "Trial", "leader", "g", "now", 100).status, "applied")

    def join(self, user):
        self.assertEqual(self.repo.invite("invite-" + user, "i-" + user, "team", "leader", user, "g", 200, 100).status, "applied")
        self.assertEqual(self.repo.join("join-" + user, "i-" + user, "team", "leader", user, "g", 101).status, "applied")

    def assert_members(self, members, version):
        self.assertEqual(self.projection(), [
            {"team_id": "team", "member_id": member, "version": version}
            for member in sorted(set(members))
        ])
        for member in members:
            self.assertEqual(self.repo.team_id_for_user(member), "team")

    def test_create_join_transfer_leave_kick_disband_sync_versions(self):
        self.create()
        self.assert_members(["leader"], 0)
        self.join("member")
        self.join("other")
        self.assert_members(["leader", "member", "other"], 2)
        result = self.repo.transfer("transfer", "leader", "member", self.repo.snapshot("team"))
        self.assertEqual(result.status, "applied")
        self.assert_members(["leader", "member", "other"], 3)
        self.assertEqual(self.repo.leave("leave", "leader", self.repo.snapshot("team"), "later").status, "applied")
        self.assert_members(["member", "other"], 4)
        self.assertIsNone(self.repo.team_id_for_user("leader"))
        self.assertEqual(self.repo.kick("kick", "member", "other", self.repo.snapshot("team"), "later").status, "applied")
        self.assert_members(["member"], 5)
        self.assertEqual(self.repo.disband("disband", "member", self.repo.snapshot("team"), "later").status, "applied")
        self.assertEqual(self.projection(), [])
        self.assertEqual(self.repo.exit_operation_result("disband", "disband", "member").status, "duplicate")

    def test_late_receipt_failure_rolls_back_join_and_projection(self):
        self.create()
        self.repo.invite("invite", "i", "team", "leader", "member", "g", 200, 100)
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TRIGGER fail_join BEFORE INSERT ON dungeon_team_operations WHEN NEW.operation_id='join' BEGIN SELECT RAISE(ABORT,'receipt failed'); END")
        before = self.repo.snapshot("team"), self.projection()
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.join("join", "i", "team", "leader", "member", "g", 101)
        self.assertEqual((self.repo.snapshot("team"), self.projection()), before)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(uow.query_one("SELECT status FROM dungeon_team_invites WHERE invite_id='i'")["status"], "pending")
            self.assertIsNone(uow.query_one("SELECT 1 FROM team_cd WHERE user_id='member'"))
            self.assertIsNone(uow.query_one("SELECT 1 FROM dungeon_team_operations WHERE operation_id='join'"))

    def test_backfill_fallback_and_settlement_share_typed_members_and_order(self):
        mixed = ["leader", "leader", 42, None, True, False, 1.5, {"bad": True}, [], ""]
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TABLE dungeon_team_members")
            for team_id in ("team-z", "team-a"):
                uow.execute("INSERT INTO teams(user_id,leader,members,version) VALUES(?,?,?,?)", (team_id, "leader", json.dumps(mixed), 7))
        self.assertEqual(self.repo.team_id_for_user("leader"), "team-a")
        for value in ("True", "1", "1.5", '{"bad":true}'):
            self.assertIsNone(self.repo.team_id_for_user(value))
        with DatabaseUnitOfWork(self.database) as uow:
            apply_dungeon_team_members_index(uow)
            uow.execute("INSERT INTO dungeon_team_members VALUES('stale','ghost',99)")
            apply_dungeon_team_members_index(uow)
        self.assertEqual(len(self.projection()), 4)
        self.assertEqual(self.repo.team_id_for_user("42"), "team-a")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            uow.attach_database(self.database, "player_data")
            current = DungeonSessionSqlRepository._current_team(uow, "leader")
            self.assertEqual(current["team_id"], "team-a")
            self.assertEqual(current["members"], ["leader", "leader", "42"])
            plan = uow.query_all("EXPLAIN QUERY PLAN SELECT team_id FROM dungeon_team_members WHERE member_id=? ORDER BY team_id LIMIT 1", ("leader",))
        self.assertTrue(any("USING COVERING INDEX dungeon_team_members_member_idx" in row["detail"] for row in plan))

    def test_duplicate_historical_members_do_not_break_sync(self):
        self.create()
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE teams SET members=? WHERE user_id='team'", (json.dumps(["leader", "member", "member"]),))
            apply_dungeon_team_members_index(uow)
        self.join("other")
        self.assert_members(["leader", "member", "other"], 1)

    def test_pure_reads_are_read_only_and_never_materialize_all_teams(self):
        self.create()
        calls = []
        original = DatabaseUnitOfWork.query_all

        def bounded_query_all(uow, sql, params=()):
            self.assertNotIn("FROM teams", sql)
            return original(uow, sql, params)

        enter = DatabaseUnitOfWork.__enter__

        def read_only_enter(uow):
            calls.append(uow.read_only)
            return enter(uow)

        with patch.object(DatabaseUnitOfWork, "query_all", bounded_query_all), patch.object(DatabaseUnitOfWork, "__enter__", read_only_enter):
            self.repo.team_id_for_user("leader")
            self.repo.team_info("team")
            self.repo.snapshot("team")
            self.repo.operation_result("create")
            self.repo.pending_invite("member", 100)
            self.repo.invite_by_id("missing")
            self.repo.exit_operation_result("missing", "leave", "leader")
        self.assertEqual(calls, [True] * 7)

    def test_member_migration_routes_to_player_only(self):
        from ....plugin import build_migrations, migrations_for_database

        migrations = build_migrations()
        legacy = next(m for m in migrations if m.version == "dungeon.006")
        self.assertEqual(legacy.checksum, "bc3723edecfa226975b0062c09b566ecc51cdffc64a82f1952aef8b98758281d")
        self.assertIn("dungeon.009", {m.version for m in migrations_for_database(migrations, "player_db")})
        for key in ("game_db", "trade_db", "impart_db"):
            self.assertNotIn("dungeon.009", {m.version for m in migrations_for_database(migrations, key)})

    def test_separate_game_database_owns_users_without_player_copy(self):
        from ..team_application import DungeonTeamApplication

        game_database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TABLE user_xiuxian")
        with DatabaseUnitOfWork(game_database) as game:
            game.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            game.executemany("INSERT INTO user_xiuxian VALUES(?)", [("leader",), ("member",)])
        application = DungeonTeamApplication(self.database, game_database=game_database)
        self.assertEqual(application.create("create", "team", "Trial", "leader", "g", "now", 100).status, "applied")
        self.assertEqual(application.invite("invite", "i", "team", "leader", "member", "g", 200, 100).status, "applied")
        self.assertEqual(application.join("join", "i", "team", "leader", "member", "g", 101).status, "applied")
        self.assert_members(["leader", "member"], 1)
        with DatabaseUnitOfWork(game_database) as game:
            game.execute("DROP TABLE user_xiuxian")
        self.assertEqual(application.operation_result("create").status, "duplicate")
        self.assertEqual(application.create("create", "team", "Trial", "leader", "g", "now", 100).status, "applied")
        self.assertEqual(application.invite("missing-owner", "i2", "team", "leader", "other", "g", 200, 100).status, "schema_missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertIsNone(uow.query_one("SELECT 1 FROM sqlite_master WHERE name='user_xiuxian'"))

    def test_missing_game_owner_fails_closed_without_creating_database(self):
        game_database = Path(self.temp.name) / "missing-game.db"
        repo = DungeonTeamRepository(self.database, game_database=game_database)
        self.assertEqual(repo.create("create", "team", "Trial", "leader", "g", "now", 100).status, "schema_missing")
        self.assertFalse(game_database.exists())
        self.assertEqual(self.projection(), [])

    def test_missing_or_wrong_member_index_rejects_mutation(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP INDEX dungeon_team_members_member_idx")
            uow.execute("CREATE INDEX dungeon_team_members_member_idx ON dungeon_team_members(version)")
        self.assertEqual(self.repo.create("create", "team", "Trial", "leader", "g", "now", 100).status, "schema_missing")
