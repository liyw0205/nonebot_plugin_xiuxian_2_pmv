from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import (
    apply_dufang_player_receipts,
    apply_dufang_share_player,
    apply_dufang_sharing_preferences,
)
from ..player_stats_repository import DufangPlayerStatsSqlRepository
from ..sharing_preferences_repository import DufangSharingPreferencesSqlRepository


class DufangSharingPreferencesRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        self.repository = DufangSharingPreferencesSqlRepository(self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def migrate(self) -> None:
        with DatabaseUnitOfWork(self.player) as uow:
            apply_dufang_sharing_preferences(uow)

    def test_migration_backfills_legacy_global_json_users(self) -> None:
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                'CREATE TABLE "global"(user_id TEXT PRIMARY KEY,unseal_sharing TEXT)'
            )
            uow.execute(
                'INSERT INTO "global"(user_id,unseal_sharing) VALUES(?,?)',
                ("global", json.dumps(["u1", "u2", "u1"], ensure_ascii=False)),
            )

        self.migrate()

        self.assertIs(self.repository.enabled("u1"), True)
        self.assertIs(self.repository.enabled("u2"), True)
        self.assertIs(self.repository.enabled("not-enabled"), False)
        self.assertEqual(set(self.repository.enabled_users()), {"u1", "u2"})

        # Rerunning the startup migration must not duplicate or disable rows.
        self.migrate()
        self.assertEqual(set(self.repository.enabled_users()), {"u1", "u2"})

    def test_set_enabled_is_idempotent_and_enabled_users_excludes_self(self) -> None:
        self.migrate()
        stamp = "2026-10-07T12:00:00+08:00"

        self.assertIs(self.repository.set_enabled("self", True, stamp), True)
        self.assertIs(self.repository.set_enabled("peer-a", True, stamp), True)
        self.assertIs(self.repository.set_enabled("peer-b", True, stamp), True)
        self.assertIs(self.repository.set_enabled("peer-a", True, "later"), False)
        self.assertIs(self.repository.set_enabled("peer-b", False, "later"), True)
        self.assertIs(self.repository.set_enabled("peer-b", False, "again"), False)

        self.assertIs(self.repository.enabled("peer-a"), True)
        self.assertIs(self.repository.enabled("peer-b"), False)
        self.assertEqual(self.repository.enabled_users(excluding_user_id="self"), ("peer-a",))
        self.assertEqual(self.repository.enabled_users(excluding_user_id="peer-a"), ("self",))

    def test_import_enabled_users_counts_unique_new_users(self) -> None:
        self.migrate()

        inserted = self.repository.import_enabled_users(
            ["u1", "u2", "u1", "", "u2"], "2026-10-07T12:30:00+08:00"
        )

        self.assertEqual(inserted, 2)
        self.assertEqual(set(self.repository.enabled_users()), {"u1", "u2"})
        self.assertEqual(
            self.repository.import_enabled_users(["u1", "u2"], "later"),
            0,
        )

    def test_missing_schema_reads_and_writes_fail_closed_without_ddl(self) -> None:
        # Create an empty database file, then verify each API leaves its schema
        # untouched when startup migrations have not run.
        with DatabaseUnitOfWork(self.player):
            pass

        self.assertIsNone(self.repository.enabled("u"))
        self.assertIsNone(self.repository.enabled_users())
        self.assertIsNone(self.repository.set_enabled("u", True, "now"))
        self.assertIsNone(self.repository.import_enabled_users(["u"], "now"))

        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            tables = uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        self.assertEqual(tables, [])

    def test_import_legacy_snapshot_inserts_once_and_preserves_active_statistics(self) -> None:
        with DatabaseUnitOfWork(self.game):
            pass
        with DatabaseUnitOfWork(self.player) as uow:
            apply_dufang_share_player(uow)
            apply_dufang_player_receipts(uow)

        repository = DufangPlayerStatsSqlRepository(self.game, self.player)
        legacy = {
            "unseal_info": {"count": 2, "total_cost": 30, "profit": 8, "loss": 4},
            "sharing_info": {
                "shared_profit": 5,
                "shared_loss": 6,
                "received_profit": 7,
                "received_loss": 8,
            },
            "last_update": "legacy-update",
        }
        imported_at = "2026-10-07T13:00:00+08:00"

        self.assertIs(repository.import_legacy_snapshot("u", legacy, imported_at), True)
        imported = repository.snapshot("u")
        self.assertEqual(
            imported.legacy_data(),
            {
                "unseal_info": {"count": 2, "total_cost": 30, "profit": 8, "loss": 4},
                "sharing_info": {
                    "shared_profit": 5,
                    "shared_loss": 6,
                    "received_profit": 7,
                    "received_loss": 8,
                },
                "last_update": imported_at,
            },
        )

        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "UPDATE unseal_data SET count=?,total_cost=?,profit=?,loss=?,"
                "shared_profit=?,shared_loss=?,received_profit=?,received_loss=?,last_update=? "
                "WHERE user_id=?",
                (9, 90, 18, 14, 15, 16, 17, 18, "active-update", "u"),
            )

        newer_legacy = {
            "unseal_info": {"count": 200, "total_cost": 3000, "profit": 800, "loss": 400},
            "sharing_info": {
                "shared_profit": 500,
                "shared_loss": 600,
                "received_profit": 700,
                "received_loss": 800,
            },
            "last_update": "stale-legacy-update",
        }
        self.assertIs(repository.import_legacy_snapshot("u", newer_legacy, "later"), False)
        self.assertEqual(
            repository.snapshot("u").legacy_data(),
            {
                "unseal_info": {"count": 9, "total_cost": 90, "profit": 18, "loss": 14},
                "sharing_info": {
                    "shared_profit": 15,
                    "shared_loss": 16,
                    "received_profit": 17,
                    "received_loss": 18,
                },
                "last_update": "active-update",
            },
        )

    def test_import_legacy_snapshot_returns_none_without_player_schema(self) -> None:
        with DatabaseUnitOfWork(self.game):
            pass
        with DatabaseUnitOfWork(self.player):
            pass

        repository = DufangPlayerStatsSqlRepository(self.game, self.player)
        legacy = {
            "unseal_info": {"count": 1, "total_cost": 2, "profit": 3, "loss": 4},
            "sharing_info": {},
            "last_update": "legacy-update",
        }

        self.assertIsNone(repository.import_legacy_snapshot("u", legacy, "now"))
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(
                uow.query_all("SELECT name FROM sqlite_master WHERE type='table'"),
                [],
            )


if __name__ == "__main__":
    unittest.main()
