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


class DungeonMemberLeaveTests(unittest.TestCase):
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
                ("team-1", "team-1", "trial", "leader", json.dumps(["leader", "member"])),
            )
            conn.execute(
                "CREATE TABLE team_cd ("
                "user_id TEXT PRIMARY KEY, join_cd_until TEXT, had_first_join INTEGER)"
            )
            conn.executemany(
                "INSERT INTO team_cd VALUES (%s, %s, %s)",
                [("leader", "", 1), ("member", "", 1)],
            )
            conn.execute(
                "CREATE TABLE player_dungeon_status ("
                "user_id TEXT PRIMARY KEY, dungeon_status TEXT)"
            )
            conn.executemany(
                "INSERT INTO player_dungeon_status VALUES (%s, %s)",
                [("leader", "not_started"), ("member", "not_started")],
            )
            conn.execute(
                "CREATE TABLE dungeon_team_invites ("
                "invite_id TEXT PRIMARY KEY, team_id TEXT, inviter_id TEXT, "
                "invitee_id TEXT, group_id TEXT, expires_at REAL, consumed_at TIMESTAMP)"
            )
            conn.execute(
                "INSERT INTO dungeon_team_invites VALUES (%s, %s, %s, %s, %s, %s, NULL)",
                ("invite-1", "team-1", "leader", "member", "100", 9999999999),
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

    def test_leader_leave_transfers_leadership_and_applies_cooldown_atomically(self) -> None:
        result = self.service.leave(
            "leave-1", "leader", self.snapshot(), "2026-07-14 15:00:00"
        )

        self.assertEqual(
            (result.status, result.new_leader_id, result.disbanded, result.cooldown_members),
            ("applied", "member", False, ("leader",)),
        )
        team = self.row("SELECT leader, members FROM teams WHERE user_id=%s", ("team-1",))
        self.assertEqual((team[0], json.loads(team[1])), ("member", ["member"]))
        self.assertEqual(
            self.row("SELECT join_cd_until FROM team_cd WHERE user_id=%s", ("leader",))[0],
            "2026-07-14 15:00:00",
        )
        self.assertEqual(
            self.row(
                "SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s", ("leader",)
            )[0],
            "not_started",
        )
        self.assertIsNotNone(
            self.row("SELECT consumed_at FROM dungeon_team_invites WHERE invite_id=%s", ("invite-1",))[0]
        )
        self.assertEqual(
            self.row("SELECT COUNT(*) FROM dungeon_team_exit_operations")[0], 1
        )

    def test_leave_is_idempotent_and_rejects_stale_snapshots(self) -> None:
        snapshot = self.snapshot()
        first = self.service.leave("leave-repeat", "leader", snapshot, "2026-07-14 15:00:00")
        duplicate = self.service.leave("leave-repeat", "leader", snapshot, "2026-07-14 15:00:00")
        stale = self.service.leave("leave-stale", "member", snapshot, "2026-07-14 15:00:00")

        self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(self.row("SELECT COUNT(*) FROM dungeon_team_exit_operations")[0], 2)

    def test_leave_is_blocked_while_any_team_member_is_exploring(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_status=%s WHERE user_id=%s",
                ("exploring", "member"),
            )

        result = self.service.leave(
            "leave-active", "leader", self.snapshot(), "2026-07-14 15:00:00"
        )

        self.assertEqual(result.status, "session_active")
        self.assertEqual(
            json.loads(self.row("SELECT members FROM teams WHERE user_id=%s", ("team-1",))[0]),
            ["leader", "member"],
        )

    def test_last_member_leave_disbands_the_team(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM teams WHERE user_id=%s", ("team-1",))
            conn.execute(
                "INSERT INTO teams VALUES (%s, %s, %s, %s, %s)",
                ("team-1", "team-1", "trial", "leader", json.dumps(["leader"])),
            )

        result = self.service.leave(
            "leave-last", "leader", self.snapshot(), "2026-07-14 15:00:00"
        )

        self.assertEqual((result.status, result.disbanded, result.cooldown_members), ("applied", True, ("leader",)))
        self.assertEqual(self.row("SELECT COUNT(*) FROM teams WHERE user_id=%s", ("team-1",))[0], 0)

    def test_last_member_leave_disbands_and_rolls_back_on_operation_failure(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM teams WHERE user_id=%s", ("team-1",))
            conn.execute(
                "INSERT INTO teams VALUES (%s, %s, %s, %s, %s)",
                ("team-1", "team-1", "trial", "leader", json.dumps(["leader"])),
            )
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_leave_operation BEFORE INSERT ON "
                "dungeon_team_exit_operations BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.leave("leave-fail", "leader", self.snapshot(), "2026-07-14 15:00:00")

        self.assertEqual(self.row("SELECT COUNT(*) FROM teams WHERE user_id=%s", ("team-1",))[0], 1)
        self.assertEqual(
            self.row("SELECT join_cd_until FROM team_cd WHERE user_id=%s", ("leader",))[0], ""
        )
        self.assertEqual(
            self.row(
                "SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s", ("leader",)
            )[0],
            "not_started",
        )

    def test_production_leave_handler_uses_transaction_service_without_split_writes(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def leave_team_handler")
        end = source.index("@kick_team_cmd.handle", start)
        handler = source[start:end]

        self.assertIn("dungeon_team_exit_service.leave(", handler)
        self.assertNotIn("remove_member_from_team(", handler)
        self.assertNotIn("set_team_cd(", handler)


if __name__ == "__main__":
    unittest.main()
