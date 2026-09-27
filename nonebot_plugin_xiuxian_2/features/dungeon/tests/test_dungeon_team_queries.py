from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..team_application import DungeonTeamApplication
from tests.test_db_backend import db_backend


class DungeonTeamQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE teams ("
                "user_id TEXT PRIMARY KEY, team_id TEXT, team_name TEXT, leader TEXT, "
                "members TEXT, create_time TEXT, max_members TEXT, version TEXT)"
            )
            conn.execute(
                "INSERT INTO teams VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    "team-2",
                    "public-team-id",
                    "trial",
                    '{"invalid": true}',
                    '[1001, "1002", null, {"bad": true}]',
                    "2026-09-28 12:00:00",
                    "invalid",
                    "invalid",
                ),
            )
        self.application = DungeonTeamApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_team_queries_preserve_legacy_record_normalization(self) -> None:
        self.assertEqual(self.application.team_id_for_user("1001"), "team-2")
        self.assertIsNone(self.application.team_id_for_user("missing"))

        team = self.application.team_info("team-2")
        self.assertIsNotNone(team)
        self.assertEqual(team["team_id"], "public-team-id")
        self.assertEqual(team["team_name"], "trial")
        self.assertEqual(team["members"], ["1001", "1002"])
        self.assertEqual(team["leader"], "1001")
        self.assertEqual(team["max_members"], 4)
        self.assertEqual(team["version"], 0)
        self.assertIsNone(self.application.team_info("missing"))
        with db_backend.connection(self.database) as conn:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("dungeon_team_operations", tables)
        self.assertNotIn("dungeon_team_invites", tables)
        self.assertNotIn("team_cd", tables)

    def test_invite_by_id_returns_persisted_snapshot(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE dungeon_team_invites ("
                "invite_id TEXT PRIMARY KEY, team_id TEXT, inviter_id TEXT, "
                "invitee_id TEXT, group_id TEXT, expires_at REAL, consumed_at TEXT, "
                "status TEXT, created_at REAL, resolved_operation_id TEXT)"
            )
            conn.execute(
                "INSERT INTO dungeon_team_invites VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s, NULL)",
                ("invite-1", "team-2", "leader", "1001", "100", 160, "pending", 100),
            )

        invite = self.application.invite_by_id("invite-1")

        self.assertIsNotNone(invite)
        self.assertEqual(
            (
                invite.invite_id,
                invite.team_id,
                invite.inviter_id,
                invite.invitee_id,
                invite.group_id,
                invite.created_at,
                invite.expires_at,
                invite.status,
            ),
            ("invite-1", "team-2", "leader", "1001", "100", 100, 160, "pending"),
        )
        self.assertIsNone(self.application.invite_by_id("missing"))


if __name__ == "__main__":
    unittest.main()
