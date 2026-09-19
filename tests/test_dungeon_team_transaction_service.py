from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.transaction_service import DungeonTeamTransactionService
from nonebot_plugin_xiuxian_2.features.dungeon.team_application import DungeonTeamApplication
from nonebot_plugin_xiuxian_2.features.dungeon.migrations import apply_dungeon_team
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class DungeonTeamTransactionServiceTests(unittest.TestCase):
    def test_dungeon_facade_defers_team_transaction_service_construction(self):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as dungeon_plugin

        source = Path(
            "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("dungeon_team_application", source)
        self.assertNotIn("_dungeon_team_transaction_service().create(", source)
        self.assertNotIn("_dungeon_team_transaction_service().invite(", source)
        source = Path(
            "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/dungeon_manager.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().get_fields(", source)
        self.assertIn("_player_data_manager()._ensure_table_exists(", source)

    def test_feature_team_application_owns_create_and_invite(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.sqlite3"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
                conn.executemany("INSERT INTO user_xiuxian VALUES (%s)", (("leader",), ("member",)))
                conn.execute("CREATE TABLE player_dungeon_status (user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
                conn.executemany("INSERT INTO player_dungeon_status VALUES (%s,%s)", (("leader", "not_started"), ("member", "not_started")))
            with DatabaseUnitOfWork(database) as uow:
                apply_dungeon_team(uow)
            application = DungeonTeamApplication(database)
            created = application.create("create-feature", "team-1", "试炼队", "leader", "100", "now", 100)
            invited = application.invite("invite-feature", "invite-1", "team-1", "leader", "member", "100", 160, 100)
            self.assertEqual((created.status, invited.status), ("applied", "applied"))

    def test_feature_team_application_owns_join(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.sqlite3"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
                conn.executemany("INSERT INTO user_xiuxian VALUES (%s)", (("leader",), ("member",)))
                conn.execute("CREATE TABLE player_dungeon_status (user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
                conn.executemany("INSERT INTO player_dungeon_status VALUES (%s,%s)", (("leader", "not_started"), ("member", "not_started")))
            with DatabaseUnitOfWork(database) as uow:
                apply_dungeon_team(uow)
            application = DungeonTeamApplication(database)
            application.create("create-feature", "team-1", "试炼队", "leader", "100", "now", 100)
            application.invite("invite-feature", "invite-1", "team-1", "leader", "member", "100", 160, 100)
            joined = application.join("join-feature", "invite-1", "team-1", "leader", "member", "100", 101)
            self.assertEqual(joined.status, "applied")

    def test_feature_team_application_owns_reject_and_expire(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.sqlite3"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
                conn.executemany("INSERT INTO user_xiuxian VALUES (%s)", (("leader",), ("member",)))
                conn.execute("CREATE TABLE player_dungeon_status (user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
                conn.executemany("INSERT INTO player_dungeon_status VALUES (%s,%s)", (("leader", "not_started"), ("member", "not_started")))
            with DatabaseUnitOfWork(database) as uow:
                apply_dungeon_team(uow)
            application = DungeonTeamApplication(database)
            application.create("create-feature", "team-1", "试炼队", "leader", "100", "now", 100)
            application.invite("invite-reject", "invite-reject", "team-1", "leader", "member", "100", 160, 100)
            rejected = application.reject("reject-feature", "invite-reject", "member", "100", 101)
            application.invite("invite-expire", "invite-expire", "team-1", "leader", "member", "100", 150, 100)
            expired = application.expire("expire-feature", "invite-expire", 160)
            self.assertEqual((rejected.status, expired.status), ("applied", "applied"))

    def test_feature_team_application_owns_snapshot_transfer(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.sqlite3"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
                conn.executemany("INSERT INTO user_xiuxian VALUES (%s)", (("leader",), ("member",)))
                conn.execute("CREATE TABLE player_dungeon_status (user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
                conn.executemany("INSERT INTO player_dungeon_status VALUES (%s,%s)", (("leader", "not_started"), ("member", "not_started")))
            with DatabaseUnitOfWork(database) as uow:
                apply_dungeon_team(uow)
            application = DungeonTeamApplication(database)
            application.create("create-feature", "team-1", "试炼队", "leader", "100", "now", 100)
            application.invite("invite-feature", "invite-1", "team-1", "leader", "member", "100", 160, 100)
            application.join("join-feature", "invite-1", "team-1", "leader", "member", "100", 101)
            snapshot = application.snapshot("team-1")
            transferred = application.transfer("transfer-feature", "leader", "member", snapshot)
            self.assertEqual(transferred.status, "applied")

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s)", ("leader",))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s)", ("member",))
            conn.execute("CREATE TABLE player_dungeon_status (user_id TEXT PRIMARY KEY,dungeon_status TEXT)")
            conn.execute("INSERT INTO player_dungeon_status VALUES (%s,%s)", ("leader", "not_started"))
            conn.execute("INSERT INTO player_dungeon_status VALUES (%s,%s)", ("member", "not_started"))
        self.service = DungeonTeamTransactionService(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def members(self, team_id="team-1"):
        with db_backend.connection(self.database) as conn:
            row = conn.execute("SELECT members FROM teams WHERE user_id=%s", (team_id,)).fetchone()
            return json.loads(row[0]) if row else []

    def create_team(self):
        return self.service.create("create-1", "team-1", "试炼队", "leader", "100", "2026-07-13 12:00:00")

    def test_create_is_atomic_idempotent_and_rechecks_state(self):
        self.assertEqual(self.create_team().status, "applied")
        self.assertEqual(self.create_team().status, "duplicate")
        self.assertEqual(self.members(), ["leader"])
        self.assertEqual(self.service.create("create-2", "team-2", "另一队", "leader", "100", "now").status, "user_has_team")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE player_dungeon_status SET dungeon_status=%s WHERE user_id=%s", ("exploring", "member"))
        self.assertEqual(self.service.create("create-3", "team-3", "会话队", "member", "100", "now").status, "session_active")

    def test_join_rechecks_invite_membership_and_session(self):
        self.create_team()
        self.service.record_invite("invite-1", "team-1", "leader", "member", "100", 200)
        applied = self.service.join("join-1", "invite-1", "team-1", "leader", "member", "100", 100)
        self.assertEqual((applied.status, applied.member_count), ("applied", 2))
        self.assertEqual(self.service.join("join-1", "invite-1", "team-1", "leader", "member", "100", 100).status, "duplicate")
        self.assertEqual(self.members(), ["leader", "member"])
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO user_xiuxian VALUES (%s)", ("late",))
            conn.execute("INSERT INTO player_dungeon_status VALUES (%s,%s)", ("late", "not_started"))
        self.service.record_invite("invite-2", "team-1", "leader", "late", "100", 200)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_status=%s WHERE user_id=%s",
                ("exploring", "late"),
            )
        self.assertEqual(self.service.join("join-2", "invite-2", "team-1", "leader", "late", "100", 100).status, "session_active")
        self.assertEqual(self.members(), ["leader", "member"])

    def test_join_failure_rolls_back_all_changes(self):
        self.create_team()
        self.service.record_invite("invite-rb", "team-1", "leader", "member", "100", 200)
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TRIGGER fail_team_operation BEFORE INSERT ON dungeon_team_operations WHEN NEW.operation_id='join-rb' BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.join("join-rb", "invite-rb", "team-1", "leader", "member", "100", 100)
        self.assertEqual(self.members(), ["leader"])
        with db_backend.connection(self.database) as conn:
            invite = conn.execute("SELECT consumed_at FROM dungeon_team_invites WHERE invite_id=%s", ("invite-rb",)).fetchone()
            operation = conn.execute("SELECT 1 FROM dungeon_team_operations WHERE operation_id=%s", ("join-rb",)).fetchone()
        self.assertIsNone(invite[0])
        self.assertIsNone(operation)


if __name__ == "__main__":
    unittest.main()
