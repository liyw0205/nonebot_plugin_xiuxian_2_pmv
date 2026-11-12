from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.bootstrap import LifecyclePhase, build_runtime_context
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank, apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.paths import configure_paths, get_paths
from nonebot_plugin_xiuxian_2.plugin import build_migrations, shutdown, startup
from tests.bootstrap import copy_static_data


_BANK_003_ORIGINAL_CHECKSUM = "2f47b238159b04b6305e542e90b7f718b7e6bbcd131cd5ad7ccead41edf8d3b3"


class MigrationChecksumCompatibilityTests(unittest.TestCase):
    def test_startup_preserves_original_bank_003_history_and_applies_bank_004(self) -> None:
        async def verify() -> None:
            previous_data = get_paths().data
            try:
                with tempfile.TemporaryDirectory() as directory:
                    data_dir = copy_static_data(
                        Path(__file__).resolve().parents[1] / "data" / "xiuxian",
                        Path(directory) / "data" / "xiuxian",
                    )
                    context = build_runtime_context(data_dir=data_dir, legacy_startup=False)
                    game_database = context.database.path("game_db")
                    player_database = context.database.path("player_db")
                    catalog = {migration.version: migration for migration in build_migrations()}
                    self.assertEqual(catalog["bank.003"].checksum, _BANK_003_ORIGINAL_CHECKSUM)
                    self.assertEqual(catalog["bank.004"].name, "bank_legacy_account_backfill_exact")

                    with DatabaseUnitOfWork(game_database) as uow:
                        uow.execute(
                            "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, name TEXT NOT NULL, "
                            "checksum TEXT NOT NULL, applied_at TEXT NOT NULL, duration_ms INTEGER NOT NULL)"
                        )
                        apply_bank(uow)
                        apply_bank_accounts(uow)
                        uow.execute(
                            "INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) "
                            "VALUES('legacy-user',9000000000000000,'3','newer-game-state')"
                        )
                        uow.execute(
                            "INSERT INTO bank_account_operations VALUES(?,?,?,?,?,?,?,?)",
                            ("bank-op", "legacy-user", "{}", 0, 0, 0, 9000000000000000, "receipt"),
                        )
                        for version in ("bank.001", "bank.002", "bank.003"):
                            migration = catalog[version]
                            checksum = (
                                _BANK_003_ORIGINAL_CHECKSUM if version == "bank.003" else migration.checksum
                            )
                            uow.execute(
                                "INSERT INTO schema_migrations(version,name,checksum,applied_at,duration_ms) "
                                "VALUES(?,?,?,?,?)",
                                (version, migration.name, checksum, "2026-11-07T22:01:03+00:00", 1),
                            )

                    with DatabaseUnitOfWork(player_database) as uow:
                        uow.execute(
                            "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY,savestone INTEGER,savetime TEXT,banklevel TEXT)"
                        )
                        uow.execute(
                            "INSERT INTO bankinfo VALUES('legacy-user',100,'old-snapshot','1')"
                        )

                    state, _readiness, _context, lifecycle = await startup(context)
                    try:
                        self.assertIs(state.phase, LifecyclePhase.READY, state.error)
                        with DatabaseUnitOfWork(game_database, read_only=True) as uow:
                            account = uow.query_one(
                                "SELECT saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id=?",
                                ("legacy-user",),
                            )
                            applied = {
                                row["version"]: row["checksum"]
                                for row in uow.query_all(
                                    "SELECT version,checksum FROM schema_migrations WHERE version LIKE 'bank.%'"
                                )
                            }
                        self.assertEqual(
                            tuple(account.values()),
                            (9000000000000000, "3", "newer-game-state"),
                        )
                        self.assertEqual(applied["bank.003"], _BANK_003_ORIGINAL_CHECKSUM)
                        self.assertEqual(applied["bank.004"], catalog["bank.004"].checksum)
                    finally:
                        await shutdown(lifecycle)
            finally:
                configure_paths(previous_data)

        asyncio.run(verify())


if __name__ == "__main__":
    unittest.main()
