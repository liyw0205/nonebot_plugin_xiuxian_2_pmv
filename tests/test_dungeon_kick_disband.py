from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.team_exit_service import (
    DungeonTeamExitService,
)
from tests.test_db_backend import db_backend


class DungeonKickDisbandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE teams ("
                "user_id TEXT PRIMARY KEY, team_id TEXT, team_name TEXT, leader TEXT, "
                "members TEXT)"
            )
            conn.execute(
                "INSERT INTO teams VALUES (%s, %s, %s, %s, %s)",
                (
                    "team-1",
                    "team-1",
                    "trial",
                    "leader",
                    json.dumps(["leader", "member", "newcomer"]),
                ),
            )
            conn.execute(
                "CREATE TABLE team_cd ("
                "user_id TEXT PRIMARY KEY, join_cd_until TEXT, had_first_join INTEGER)"
            )
            conn.executemany(
                "INSERT INTO team_cd VALUES (%s, %s, %s)",
                [("leader", "", 1), ("member", "", 1), ("newcomer", "", 0)],
            )
            conn.execute(
                "CREATE TABLE player_dungeon_status ("
                "user_id TEXT PRIMARY KEY, dungeon_status TEXT)"
            )
            conn.executemany(
                "INSERT INTO player_dungeon_status VALUES (%s, %s)",
                [("leader", "not_started"), ("member", "not_started"), ("newcomer", "not_started")],
            )
            conn.execute(
                "CREATE TABLE dungeon_team_invites ("
                "invite_id TEXT PRIMARY KEY, team_id TEXT, inviter_id TEXT, "
                "invitee_id TEXT, group_id TEXT, expires_at REAL, consumed_at TIMESTAMP)"
            )
            conn.executemany(
                "INSERT INTO dungeon_team_invites VALUES (%s, %s, %s, %s, %s, %s, NULL)",
                [
                    ("invite-team", "team-1", "leader", "waiting", "100", 9999999999),
                    ("invite-member", "other", "other", "member", "100", 9999999999),
                ],
            )
        self.service = DungeonTeamExitService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self, sql: str, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()

    def snapshot(self):
        snapshot = self.service.snapshot("team-1")
        self.assertIsNotNone(snapshot)
        return snapshot

    def test_kick_rechecks_leader_and_updates_member_cooldown_session_and_invites(self) -> None:
        snapshot = self.snapshot()
        denied = self.service.kick("kick-denied", "member", "newcomer", snapshot, "2026-07-14 15:00:00")
        result = self.service.kick("kick-1", "leader", "member", snapshot, "2026-07-14 15:00:00")

        self.assertEqual((denied.status, result.status, result.cooldown_members), ("actor_not_leader", "applied", ("member",)))
        team = self.row("SELECT leader, members FROM teams WHERE user_id=%s", ("team-1",))
        self.assertEqual((team[0], json.loads(team[1])), ("leader", ["leader", "newcomer"]))
        self.assertEqual(
            self.row("SELECT join_cd_until FROM team_cd WHERE user_id=%s", ("member",))[0],
            "2026-07-14 15:00:00",
        )
        self.assertEqual(
            self.row(
                "SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s", ("member",)
            )[0],
            "not_started",
        )
        self.assertIsNotNone(
            self.row("SELECT consumed_at FROM dungeon_team_invites WHERE invite_id=%s", ("invite-team",))[0]
        )
        self.assertIsNotNone(
            self.row("SELECT consumed_at FROM dungeon_team_invites WHERE invite_id=%s", ("invite-member",))[0]
        )

    def test_disband_clears_all_sessions_and_applies_only_eligible_cooldowns(self) -> None:
        snapshot = self.snapshot()
        result = self.service.disband("disband-1", "leader", snapshot, "2026-07-14 15:00:00")
        duplicate = self.service.disband("disband-1", "leader", snapshot, "2026-07-14 15:00:00")

        self.assertEqual(
            (result.status, result.disbanded, result.cooldown_members, duplicate.status),
            ("applied", True, ("leader", "member"), "duplicate"),
        )
        self.assertEqual(self.row("SELECT COUNT(*) FROM teams WHERE user_id=%s", ("team-1",))[0], 0)
        for user_id, expected_cd in (("leader", "2026-07-14 15:00:00"), ("member", "2026-07-14 15:00:00"), ("newcomer", "")):
            self.assertEqual(
                self.row("SELECT join_cd_until FROM team_cd WHERE user_id=%s", (user_id,))[0],
                expected_cd,
            )
            self.assertEqual(
                self.row(
                    "SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s", (user_id,)
                )[0],
                "not_started",
            )
        self.assertIsNotNone(
            self.row("SELECT consumed_at FROM dungeon_team_invites WHERE invite_id=%s", ("invite-team",))[0]
        )
        self.assertEqual(self.row("SELECT COUNT(*) FROM dungeon_team_exit_operations")[0], 1)

    def test_kick_and_disband_are_blocked_during_active_session(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_status=%s WHERE user_id=%s",
                ("exploring", "newcomer"),
            )
        snapshot = self.snapshot()

        kick = self.service.kick(
            "kick-active", "leader", "member", snapshot, "2026-07-14 15:00:00"
        )
        disband = self.service.disband(
            "disband-active", "leader", snapshot, "2026-07-14 15:00:00"
        )

        self.assertEqual((kick.status, disband.status), ("session_active", "session_active"))
        self.assertEqual(
            json.loads(self.row("SELECT members FROM teams WHERE user_id=%s", ("team-1",))[0]),
            ["leader", "member", "newcomer"],
        )

    def test_kick_operation_failure_rolls_back_team_and_member_state(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_kick_operation BEFORE INSERT ON "
                "dungeon_team_exit_operations BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.kick("kick-fail", "leader", "member", self.snapshot(), "2026-07-14 15:00:00")

        team = self.row("SELECT members FROM teams WHERE user_id=%s", ("team-1",))
        self.assertEqual(json.loads(team[0]), ["leader", "member", "newcomer"])
        self.assertEqual(
            self.row("SELECT join_cd_until FROM team_cd WHERE user_id=%s", ("member",))[0], ""
        )
        self.assertEqual(
            self.row(
                "SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s", ("member",)
            )[0],
            "not_started",
        )

    def test_disband_operation_failure_rolls_back_team_cooldowns_and_sessions(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_disband_operation BEFORE INSERT ON "
                "dungeon_team_exit_operations BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.disband("disband-fail", "leader", self.snapshot(), "2026-07-14 15:00:00")

        self.assertEqual(self.row("SELECT COUNT(*) FROM teams WHERE user_id=%s", ("team-1",))[0], 1)
        for user_id in ("leader", "member", "newcomer"):
            self.assertEqual(
                self.row("SELECT join_cd_until FROM team_cd WHERE user_id=%s", (user_id,))[0], ""
            )
            self.assertEqual(
                self.row(
                    "SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s", (user_id,)
                )[0],
                "not_started",
            )

    def test_production_kick_and_disband_handlers_use_transaction_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/__init__.py"
        ).read_text(encoding="utf-8")
        kick_start = source.index("async def kick_team_handler")
        disband_start = source.index("async def disband_team_handler")
        view_start = source.index("@view_team_cmd.handle", disband_start)
        kick_handler = source[kick_start:disband_start]
        disband_handler = source[disband_start:view_start]

        self.assertIn("dungeon_team_exit_service.kick(", kick_handler)
        self.assertIn("dungeon_team_exit_service.disband(", disband_handler)
        self.assertNotIn("remove_member_from_team(", kick_handler)
        self.assertNotIn("disband_team(", disband_handler)
        self.assertNotIn("set_team_cd(", kick_handler)
        self.assertNotIn("set_team_cd(", disband_handler)


if __name__ == "__main__":
    unittest.main()
