from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, BrokenBarrierError, Lock
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.bootstrap import LifecyclePhase, build_runtime_context
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, Migration, MigrationRunner
from nonebot_plugin_xiuxian_2.paths import configure_paths, get_paths
from nonebot_plugin_xiuxian_2.plugin import build_lifecycle


class StartupMigrationConcurrencyTests(unittest.TestCase):
    def _assert_concurrent_startup_migrations(self, *, existing_history: bool) -> None:
        previous_data = get_paths().data
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            calls = 0
            calls_lock = Lock()
            both_callbacks = Barrier(2)

            def apply_platform(_uow: DatabaseUnitOfWork) -> None:
                return None

            def apply_concurrent(uow: DatabaseUnitOfWork) -> None:
                nonlocal calls
                with calls_lock:
                    calls += 1
                try:
                    both_callbacks.wait(timeout=0.25)
                except BrokenBarrierError:
                    pass
                uow.execute(
                    "CREATE TABLE startup_migration_concurrency (value TEXT NOT NULL)"
                )
                uow.execute(
                    "INSERT INTO startup_migration_concurrency(value) VALUES('once')"
                )

            migrations = (
                Migration("platform.001", "platform_schema", apply_platform),
                Migration(
                    "zzzz_startup_concurrency.001",
                    "startup_concurrency_probe",
                    apply_concurrent,
                ),
            )
            contexts = [
                build_runtime_context(data_dir=data_dir, settings={}, legacy_startup=False)
                for _ in range(2)
            ]
            lifecycles = []
            try:
                with patch("nonebot_plugin_xiuxian_2.plugin.build_migrations", return_value=migrations):
                    for context in contexts:
                        lifecycle, _readiness, _context = build_lifecycle(context)
                        lifecycle._callbacks[LifecyclePhase.FILESYSTEM]()
                        lifecycle._callbacks[LifecyclePhase.DATABASE]()
                        lifecycles.append(lifecycle)

                platform = migrations[0]
                if existing_history:
                    for spec in contexts[0].database.specs():
                        with DatabaseUnitOfWork(spec.path, immediate=True) as uow:
                            runner = MigrationRunner((platform,))
                            runner.ensure_schema(uow)
                            uow.execute(
                                "INSERT INTO schema_migrations"
                                "(version,name,checksum,applied_at,duration_ms) VALUES(?,?,?,?,?)",
                                (
                                    platform.version,
                                    platform.name,
                                    platform.checksum,
                                    "2026-11-15T00:00:00+00:00",
                                    1,
                                ),
                            )
                            if spec.key == "game_db":
                                uow.execute(
                                    "CREATE TABLE startup_existing_state (value TEXT NOT NULL)"
                                )
                                uow.execute(
                                    "INSERT INTO startup_existing_state(value) VALUES('preserved')"
                                )

                def apply_phase(lifecycle) -> None:
                    lifecycle._callbacks[LifecyclePhase.MIGRATIONS]()

                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(apply_phase, lifecycle) for lifecycle in lifecycles]
                    for future in futures:
                        future.result()

                game_database = contexts[0].database.path("game_db")
                with DatabaseUnitOfWork(game_database, read_only=True) as uow:
                    applied = uow.query_all(
                        "SELECT version,name,checksum FROM schema_migrations "
                        "WHERE version='zzzz_startup_concurrency.001'"
                    )
                    rows = uow.query_all("SELECT value FROM startup_migration_concurrency")
                    existing = (
                        uow.query_all("SELECT value FROM startup_existing_state")
                        if existing_history
                        else []
                    )

                with DatabaseUnitOfWork(game_database, read_only=True) as uow:
                    uow.attach_database(contexts[0].database.path("player_db"), "player_data", read_only=True)
                    attached = uow.query_all(
                        "SELECT version FROM player_data.attached_schema_migrations ORDER BY version"
                    )

                self.assertEqual(calls, 1)
                self.assertEqual(
                    applied,
                    [{
                        "version": "zzzz_startup_concurrency.001",
                        "name": "startup_concurrency_probe",
                        "checksum": migrations[1].checksum,
                    }],
                )
                self.assertEqual(rows, [{"value": "once"}])
                self.assertEqual(
                    existing,
                    [{"value": "preserved"}] if existing_history else [],
                )
                self.assertEqual(len(attached), 3)
            finally:
                configure_paths(previous_data)

    def test_fresh_database_startups_apply_new_migration_once(self) -> None:
        self._assert_concurrent_startup_migrations(existing_history=False)

    def test_existing_migration_history_is_serialized_before_read(self) -> None:
        self._assert_concurrent_startup_migrations(existing_history=True)

    def test_migration_error_keeps_its_sqlite_context_in_lifecycle_state(self) -> None:
        previous_data = get_paths().data
        with tempfile.TemporaryDirectory() as directory:
            context = build_runtime_context(
                data_dir=Path(directory) / "data",
                settings={},
                legacy_startup=False,
            )
            original = sqlite3.OperationalError("database is locked")

            def apply_platform(_uow: DatabaseUnitOfWork) -> None:
                raise original

            migrations = (Migration("platform.001", "platform_schema", apply_platform),)
            try:
                with patch(
                    "nonebot_plugin_xiuxian_2.plugin.build_migrations",
                    return_value=migrations,
                ):
                    lifecycle, _readiness, _context = build_lifecycle(context)
                    state = asyncio.run(lifecycle.start())

                self.assertIs(state.phase, LifecyclePhase.NOT_READY)
                self.assertIs(state.failed_phase, LifecyclePhase.MIGRATIONS)
                self.assertEqual(state.error_type, "OperationalError")
                self.assertEqual(state.error_message, "database is locked")
                self.assertIn("ensure_migrations", state.error_traceback or "")
                self.assertEqual(state.started, ("filesystem", "database"))
                with self.assertRaises(sqlite3.OperationalError) as raised:
                    lifecycle.raise_startup_error()
                self.assertIs(raised.exception, original)
            finally:
                configure_paths(previous_data)


if __name__ == "__main__":
    unittest.main()
