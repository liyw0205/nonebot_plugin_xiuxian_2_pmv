from __future__ import annotations

import json
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.team_manager import (
    PersistentTeamInviteMapping,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.team_transaction_service import (
    DungeonTeamTransactionService,
)
from tests.test_db_backend import db_backend


class DungeonTeamLifecycleServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES (%s)",
                ((user_id,) for user_id in ("leader", "member", "other", "waiting")),
            )
            conn.execute(
                "CREATE TABLE player_dungeon_status ("
                "user_id TEXT PRIMARY KEY,dungeon_status TEXT)"
            )
            conn.executemany(
                "INSERT INTO player_dungeon_status VALUES (%s,%s)",
                (
                    (user_id, "not_started")
                    for user_id in ("leader", "member", "other", "waiting")
                ),
            )
        self.service = DungeonTeamTransactionService(self.database)
        result = self.service.create(
            "create-1", "team-1", "trial", "leader", "100", "first-time"
        )
        self.assertEqual(result.status, "applied")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self, sql: str, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()

    def members_and_version(self) -> tuple[list[str], int]:
        row = self.row(
            "SELECT members,version FROM teams WHERE user_id=%s", ("team-1",)
        )
        return json.loads(row[0]), int(row[1])

    def invite(
        self,
        operation_id: str,
        invite_id: str,
        user_id: str,
        *,
        expires_at: float = 200,
        now: float = 100,
    ):
        return self.service.invite(
            operation_id,
            invite_id,
            "team-1",
            "leader",
            user_id,
            "100",
            expires_at,
            now,
        )

    def test_invite_is_db_authoritative_and_replays_original_expiry(self) -> None:
        first = self.invite("invite-op", "invite-1", "member")
        restarted = DungeonTeamTransactionService(self.database)

        pending = restarted.pending_invite("member", 101)
        replay = restarted.invite(
            "invite-op", "invite-1", "team-1", "leader", "member", "100", 999, 500
        )

        self.assertIsNotNone(pending)
        self.assertEqual((pending.invite_id, pending.expires_at), ("invite-1", 200))
        self.assertEqual((first.status, replay.status, replay.expires_at), ("applied", "duplicate", 200))
        self.assertEqual(
            tuple(self.row(
                "SELECT expires_at,status FROM dungeon_team_invites WHERE invite_id=%s",
                ("invite-1",),
            )),
            (200.0, "pending"),
        )

    def test_cooldown_rejection_replays_original_remaining_seconds(self) -> None:
        until = datetime.fromtimestamp(200).strftime("%Y-%m-%d %H:%M:%S")
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO team_cd(user_id,join_cd_until,had_first_join) "
                "VALUES (%s,%s,1)",
                ("member", until),
            )

        first = self.invite("invite-cooldown", "cooldown-1", "member", now=100)
        replay = self.service.invite(
            "invite-cooldown",
            "cooldown-1",
            "team-1",
            "leader",
            "member",
            "100",
            999,
            150,
        )

        self.assertEqual(first.status, "cooldown_active")
        self.assertEqual(replay.status, "cooldown_active")
        self.assertEqual((first.cooldown_seconds, replay.cooldown_seconds), (100, 100))

    def test_persistent_mapping_recovers_invite_after_restart_and_delete_consumes_it(self) -> None:
        now = time.time()
        self.invite(
            "mapping-invite", "mapping-1", "member", expires_at=now + 60, now=now
        )

        mapping = PersistentTeamInviteMapping(
            service=DungeonTeamTransactionService(self.database)
        )
        recovered = mapping["member"]
        del mapping["member"]

        self.assertEqual(recovered["invite_id"], "mapping-1")
        self.assertNotIn("member", mapping)
        self.assertEqual(
            self.row(
                "SELECT status FROM dungeon_team_invites WHERE invite_id=%s", ("mapping-1",)
            )[0],
            "rejected",
        )

    def test_reject_and_expire_have_stable_operations(self) -> None:
        self.invite("invite-reject", "reject-1", "member")
        rejected = self.service.reject("reject-op", "reject-1", "member", "100", 101)
        rejected_again = self.service.reject(
            "reject-op", "reject-1", "member", "100", 999
        )
        self.invite("invite-expire", "expire-1", "other", expires_at=150, now=100)
        too_early = self.service.expire("expire-early", "expire-1", 120)
        same_operation_later = self.service.expire("expire-early", "expire-1", 160)
        expired = self.service.expire("expire-final", "expire-1", 160)

        self.assertEqual((rejected.status, rejected_again.status), ("applied", "duplicate"))
        self.assertEqual(
            (too_early.status, same_operation_later.status, expired.status),
            ("not_expired", "not_expired", "applied"),
        )
        self.assertEqual(
            self.row(
                "SELECT status FROM dungeon_team_invites WHERE invite_id=%s", ("expire-1",)
            )[0],
            "expired",
        )

    def test_join_sets_first_join_and_version_in_the_same_transaction(self) -> None:
        self.invite("invite-member", "join-1", "member")
        joined = self.service.join(
            "join-op", "join-1", "team-1", "leader", "member", "100", 101
        )
        replay = self.service.join(
            "join-op", "join-1", "team-1", "leader", "member", "100", 999
        )

        self.assertEqual((joined.status, joined.version, replay.status), ("applied", 1, "duplicate"))
        self.assertEqual(self.members_and_version(), (["leader", "member"], 1))
        self.assertEqual(
            self.row(
                "SELECT had_first_join FROM team_cd WHERE user_id=%s", ("member",)
            )[0],
            1,
        )
        self.assertEqual(
            self.row(
                "SELECT status FROM dungeon_team_invites WHERE invite_id=%s", ("join-1",)
            )[0],
            "joined",
        )

    def test_join_rejection_is_replayed_after_session_state_changes(self) -> None:
        self.invite("invite-active", "active-1", "member")
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_status='exploring' WHERE user_id='leader'"
            )
        expected_failure = self.service.join(
            "join-active", "active-1", "team-1", "leader", "member", "100", 101
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_status='not_started' WHERE user_id='leader'"
            )

        replay = self.service.join(
            "join-active", "active-1", "team-1", "leader", "member", "100", 101
        )
        retry = self.service.join(
            "join-after-idle", "active-1", "team-1", "leader", "member", "100", 101
        )

        self.assertEqual(
            (expected_failure.status, replay.status, retry.status),
            ("session_active", "session_active", "applied"),
        )

    def test_transfer_uses_cas_blocks_active_session_and_replays(self) -> None:
        self.invite("invite-transfer", "transfer-invite", "member")
        self.service.join(
            "join-transfer", "transfer-invite", "team-1", "leader", "member", "100", 101
        )
        expected = self.service.snapshot("team-1")
        self.assertIsNotNone(expected)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_status='exploring' WHERE user_id='member'"
            )
        blocked = self.service.transfer("transfer-blocked", "leader", "member", expected)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_status='not_started' WHERE user_id='member'"
            )
        blocked_replay = self.service.transfer(
            "transfer-blocked", "leader", "member", self.service.snapshot("team-1")
        )
        applied = self.service.transfer("transfer-applied", "leader", "member", expected)
        replay = self.service.transfer(
            "transfer-applied", "leader", "member", self.service.snapshot("team-1")
        )

        self.assertEqual(
            (blocked.status, blocked_replay.status, applied.status, replay.status),
            ("session_active", "session_active", "applied", "duplicate"),
        )
        self.assertEqual(
            tuple(self.row("SELECT leader,version FROM teams WHERE user_id=%s", ("team-1",))),
            ("member", 2),
        )

    def test_exit_replay_ignores_new_snapshot_and_cooldown_deadline(self) -> None:
        self.invite("invite-exit", "exit-invite", "member")
        self.service.join(
            "join-exit", "exit-invite", "team-1", "leader", "member", "100", 101
        )
        expected = self.service.snapshot("team-1")
        first = self.service.kick(
            "kick-op", "leader", "member", expected, "2026-07-15 10:00:00"
        )
        current = self.service.snapshot("team-1")
        replay = self.service.kick(
            "kick-op", "leader", "member", current, "2099-01-01 00:00:00"
        )
        early_replay = self.service.exit_operation_result(
            "kick-op", "kick", "leader", "member"
        )

        self.assertEqual(
            (first.status, replay.status, early_replay.status),
            ("applied", "duplicate", "duplicate"),
        )
        self.assertEqual(replay.cooldown_until, "2026-07-15 10:00:00")
        self.assertEqual(
            self.row(
                "SELECT join_cd_until FROM team_cd WHERE user_id=%s", ("member",)
            )[0],
            "2026-07-15 10:00:00",
        )

    def test_operation_insert_failures_roll_back_invite_join_transfer_and_reject(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_selected_team_operations BEFORE INSERT ON "
                "dungeon_team_operations WHEN NEW.operation_id IN "
                "('invite-fail','join-fail','transfer-fail','reject-fail') "
                "BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.invite("invite-fail", "invite-rb", "member")
        self.assertIsNone(
            self.row(
                "SELECT 1 FROM dungeon_team_invites WHERE invite_id=%s", ("invite-rb",)
            )
        )

        self.invite("invite-ok", "join-rb", "member")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.join(
                "join-fail", "join-rb", "team-1", "leader", "member", "100", 101
            )
        self.assertEqual(self.members_and_version(), (["leader"], 0))
        self.assertIsNone(
            self.row("SELECT 1 FROM team_cd WHERE user_id=%s", ("member",))
        )
        self.assertEqual(
            self.row(
                "SELECT status FROM dungeon_team_invites WHERE invite_id=%s", ("join-rb",)
            )[0],
            "pending",
        )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.reject("reject-fail", "join-rb", "member", "100", 101)
        self.assertEqual(
            self.row(
                "SELECT status FROM dungeon_team_invites WHERE invite_id=%s", ("join-rb",)
            )[0],
            "pending",
        )

        joined = self.service.join(
            "join-ok", "join-rb", "team-1", "leader", "member", "100", 101
        )
        self.assertEqual(joined.status, "applied")
        expected = self.service.snapshot("team-1")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.transfer("transfer-fail", "leader", "member", expected)
        self.assertEqual(
            tuple(self.row("SELECT leader,version FROM teams WHERE user_id=%s", ("team-1",))),
            ("leader", 1),
        )


class DungeonTeamLegacySchemaMigrationTests(unittest.TestCase):
    def test_old_tables_gain_version_invite_state_and_operation_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "legacy.sqlite3"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE teams (user_id TEXT PRIMARY KEY,team_name TEXT,"
                    "leader TEXT,members TEXT)"
                )
                conn.execute(
                    "INSERT INTO teams VALUES (%s,%s,%s,%s)",
                    ("legacy", "old", "leader", json.dumps(["leader"])),
                )
                conn.execute(
                    "CREATE TABLE dungeon_team_invites ("
                    "invite_id TEXT PRIMARY KEY,team_id TEXT,inviter_id TEXT,invitee_id TEXT,"
                    "group_id TEXT,expires_at REAL,consumed_at TIMESTAMP)"
                )
                conn.executemany(
                    "INSERT INTO dungeon_team_invites VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    [
                        ("pending", "legacy", "leader", "member", "100", 200, None),
                        ("used", "legacy", "leader", "other", "100", 200, "done"),
                    ],
                )
                conn.execute(
                    "CREATE TABLE dungeon_team_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,"
                    "team_id TEXT,created_at TIMESTAMP)"
                )
                conn.execute(
                    "INSERT INTO dungeon_team_operations VALUES (%s,%s,%s,%s,%s)",
                    (
                        "legacy-create",
                        json.dumps(
                            {
                                "action": "create",
                                "team_id": "legacy-created",
                                "team_name": "old operation",
                                "leader_id": "leader",
                                "group_id": "100",
                            },
                            sort_keys=True,
                        ),
                        "applied",
                        "legacy-created",
                        "old-time",
                    ),
                )

            service = DungeonTeamTransactionService(database)
            snapshot = service.snapshot("legacy")
            pending = service.pending_invite("member", 100)
            replay = service.create(
                "legacy-create",
                "legacy-created",
                "old operation",
                "leader",
                "100",
                "new-time",
            )

            self.assertEqual(snapshot.version, 0)
            self.assertEqual((pending.invite_id, pending.created_at), ("pending", 140.0))
            self.assertEqual((replay.status, replay.team_id), ("duplicate", "legacy-created"))
            with db_backend.connection(database) as conn:
                self.assertIn("version", conn.column_names("teams"))
                self.assertIn("result_json", conn.column_names("dungeon_team_operations"))
                self.assertEqual(
                    conn.execute(
                        "SELECT status FROM dungeon_team_invites WHERE invite_id='used'"
                    ).fetchone()[0],
                    "consumed",
                )


if __name__ == "__main__":
    unittest.main()
