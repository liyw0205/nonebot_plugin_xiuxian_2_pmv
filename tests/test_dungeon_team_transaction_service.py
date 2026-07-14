from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.team_transaction_service import DungeonTeamTransactionService
from tests.test_db_backend import db_backend


class DungeonTeamTransactionServiceTests(unittest.TestCase):
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
