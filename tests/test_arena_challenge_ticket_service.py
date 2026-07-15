from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena.transaction_service import (
    ArenaChallengeTicketService,
)
from tests.test_db_backend import db_backend


class ArenaChallengeTicketServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER DEFAULT 0,UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "INSERT INTO back VALUES (%s,%s,%s,%s)",
                ("user", 20024, 5, 3),
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE arena (user_id TEXT PRIMARY KEY,daily_challenges_used INTEGER,"
                "daily_extra_challenges INTEGER)"
            )
            conn.execute("INSERT INTO arena VALUES (%s,%s,%s)", ("user", 4, 2))
        self.service = ArenaChallengeTicketService(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def use(self, operation_id="ticket", **overrides):
        values = {
            "requested": 3,
            "items": 5,
            "used": 4,
            "extra": 2,
            "cap": 12,
        }
        values.update(overrides)
        return self.service.use(
            operation_id,
            "user",
            20024,
            values["requested"],
            values["items"],
            values["used"],
            values["extra"],
            values["cap"],
        )

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = conn.execute(
                "SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 20024),
            ).fetchone()
            table_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='arena_challenge_ticket_operations'"
            ).fetchone()
            operations = 0
            if table_exists is not None:
                operations = conn.execute(
                    "SELECT COUNT(*) FROM arena_challenge_ticket_operations"
                ).fetchone()[0]
        with db_backend.connection(self.player) as conn:
            arena = conn.execute(
                "SELECT daily_challenges_used,daily_extra_challenges FROM arena WHERE user_id=%s",
                ("user",),
            ).fetchone()
        return tuple(map(int, item)), tuple(map(int, arena)), int(operations)

    def test_success_atomically_consumes_tickets_and_restores_attempts(self) -> None:
        result = self.use()
        self.assertEqual(
            (
                result.status,
                result.used_tickets,
                result.item_remaining,
                result.challenges_used,
                result.challenges_remaining,
                result.challenge_cap,
            ),
            ("applied", 3, 2, 1, 11, 12),
        )
        self.assertEqual(self.state(), ((2, 0), (1, 2), 1))

    def test_use_count_is_limited_by_spent_attempts_and_inventory(self) -> None:
        result = self.use(requested=10)
        self.assertEqual((result.used_tickets, result.item_remaining, result.challenges_used), (4, 1, 0))
        self.assertEqual(self.state(), ((1, 1), (0, 2), 1))

    def test_stale_inventory_or_arena_snapshot_changes_nothing(self) -> None:
        self.assertEqual(self.use("stale-item", items=4).status, "state_changed")
        self.assertEqual(self.use("stale-used", used=3).status, "state_changed")
        self.assertEqual(self.use("stale-cap", extra=1, cap=11).status, "state_changed")
        with db_backend.connection(self.game) as conn:
            table_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='arena_challenge_ticket_operations'"
            ).fetchone()
        self.assertEqual(self.state()[:2], ((5, 3), (4, 2)))
        self.assertIsNone(table_exists)

    def test_duplicate_reuses_result_and_payload_conflict_is_rejected(self) -> None:
        first = self.use("same")
        duplicate = self.use("same")
        conflict = self.use("same", requested=2)
        self.assertEqual((first.status, duplicate.status, conflict.status), (
            "applied", "duplicate", "operation_conflict"
        ))
        self.assertEqual(self.state(), ((2, 0), (1, 2), 1))

    def test_operation_write_failure_rolls_back_inventory_and_attempts(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE arena_challenge_ticket_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,used_tickets INTEGER NOT NULL,"
                "item_remaining INTEGER NOT NULL,challenges_used INTEGER NOT NULL,"
                "challenges_remaining INTEGER NOT NULL,challenge_cap INTEGER NOT NULL,"
                "created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_ticket BEFORE INSERT ON arena_challenge_ticket_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.use("rollback")
        self.assertEqual(self.state(), ((5, 3), (4, 2), 0))

    def test_real_entry_uses_service_without_legacy_split_writes(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_arena/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source.split("async def use_arena_challenge_ticket", 1)[1]
        self.assertIn("arena_challenge_ticket_service.use(", handler)
        self.assertNotIn("arena_limit.add_challenge_count", handler)
        self.assertNotIn("sql_message.update_back_j", handler)


if __name__ == "__main__":
    unittest.main()
