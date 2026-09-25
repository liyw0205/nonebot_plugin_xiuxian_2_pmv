from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.buff.migrations import (
    apply_partner_token_operations,
    apply_partner_token_usage,
)
from nonebot_plugin_xiuxian_2.features.buff.partner_token_application import (
    PartnerTokenUseApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class PartnerTokenUseApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER DEFAULT 0,UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES('u',9001,5,2)")
        with DatabaseUnitOfWork(self.game) as uow:
            apply_partner_token_operations(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_partner_token_usage(uow)
            uow.execute("INSERT INTO partner_two_exp_usage(user_id,used_count) VALUES('u',3)")
        self.application = PartnerTokenUseApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self) -> tuple[tuple[int, int], int]:
        with db_backend.connection(self.game) as conn:
            item = conn.execute(
                "SELECT goods_num,bind_num FROM back WHERE user_id='u'"
            ).fetchone()
        with db_backend.connection(self.player) as conn:
            usage = conn.execute(
                "SELECT used_count FROM partner_two_exp_usage WHERE user_id='u'"
            ).fetchone()[0]
        return tuple(item), int(usage)

    def apply(
        self,
        operation_id: str = "op",
        *,
        requested: int = 2,
        items: int = 5,
        used: int = 3,
    ):
        return self.application.apply(
            operation_id,
            "u",
            9001,
            requested_count=requested,
            expected_item_count=items,
            expected_used_count=used,
        )

    def test_consumes_tokens_and_replays_conflict_without_duplicate_effects(self) -> None:
        first = self.apply()
        duplicate = self.apply()
        conflict = self.apply(requested=1)
        self.assertEqual(("applied", 2, 1, 3), (
            first.status, first.used_tokens, first.used_count, first.item_remaining
        ))
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual(((3, 0), 1), self.state())

    def test_snapshot_mismatch_and_limit_full_do_not_consume(self) -> None:
        self.assertEqual("state_changed", self.apply("stale-item", items=4).status)
        self.assertEqual("state_changed", self.apply("stale-count", used=2).status)
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE partner_two_exp_usage SET used_count=0 WHERE user_id='u'")
        self.assertEqual("limit_full", self.apply("full", used=0).status)
        self.assertEqual(((5, 2), 0), self.state())

    def test_usage_write_failure_rolls_back_inventory_update(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER fail_partner_token BEFORE UPDATE ON partner_two_exp_usage "
                "BEGIN SELECT RAISE(ABORT,'forced'); END"
            )
        with self.assertRaises(sqlite3.DatabaseError):
            self.apply("rollback")
        self.assertEqual(((5, 2), 3), self.state())

    def test_missing_migration_is_rejected_without_request_time_ddl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER)")
            with db_backend.transaction(player):
                pass
            application = PartnerTokenUseApplication(game, player)
            with self.assertRaisesRegex(RuntimeError, "partner_token_operations"):
                application.apply(
                    "missing", "u", 9001, requested_count=1,
                    expected_item_count=1, expected_used_count=1,
                )
            with db_backend.connection(game) as conn:
                self.assertIsNone(conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='partner_token_operations'"
                ).fetchone())

    def test_migrations_route_operation_and_usage_tables_to_owning_databases(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("buff.002", routed["game_db"])
        self.assertNotIn("buff.002", routed["player_db"])
        self.assertIn("buff.003", routed["player_db"])
        self.assertNotIn("buff.003", routed["game_db"])
        for key in ("trade_db", "impart_db", "message_db"):
            self.assertNotIn("buff.002", routed[key])
            self.assertNotIn("buff.003", routed[key])


if __name__ == "__main__":
    unittest.main()
