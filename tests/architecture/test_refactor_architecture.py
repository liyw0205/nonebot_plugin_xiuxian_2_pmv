from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.bootstrap import (
    CommandSpec,
    FeatureManifest,
    FeatureRegistry,
    Lifecycle,
    LifecyclePhase,
    RouteSpec,
    build_runtime_context,
)
from nonebot_plugin_xiuxian_2.core.result import OperationOutcome, utc_now
from nonebot_plugin_xiuxian_2.features.daily_fortune.application import DailyFortuneApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import (
    DatabaseUnitOfWork,
    Migration,
    MigrationRunner,
    OperationLedger,
    ReconcileService,
)
from nonebot_plugin_xiuxian_2.infrastructure.observability import current_context, trace_context
from nonebot_plugin_xiuxian_2.adapters.nonebot import context_from_event
from nonebot_plugin_xiuxian_2.plugin import build_lifecycle, build_migrations, build_registry
from nonebot_plugin_xiuxian_2.cli import main as cli_main


class RefactorArchitectureTests(unittest.TestCase):
    def test_operation_outcome_uses_injected_clock(self) -> None:
        class FixedClock:
            def now(self):
                return datetime(2026, 9, 12, 1, 2, 3, tzinfo=timezone.utc)

        clock = FixedClock()
        self.assertEqual(utc_now(clock), "2026-09-12T01:02:03+00:00")
        outcome = OperationOutcome.applied("op", "action", clock=clock)
        self.assertEqual(outcome.occurred_at, "2026-09-12T01:02:03+00:00")
        self.assertEqual(OperationOutcome.applied("op-2", "action").occurred_at, "")

    def test_command_context_normalizes_qq_and_onebot_events(self) -> None:
        class Event:
            __module__ = "nonebot.adapters.qq.event"
            group_openid = "g"
            message_id = "m"
            author = type("Author", (), {"id": "u"})()

        context = context_from_event(Event())
        self.assertEqual((context.platform, context.scene, context.group_id, context.user_id), ("qq", "group", "g", "u"))

    def test_manifest_rejects_duplicate_commands_and_requires_permissions(self) -> None:
        registry = FeatureRegistry()
        registry.register(
            FeatureManifest(
                key="one", title="one", owner="test",
                commands=(CommandSpec("same", permission="user"),),
                routes=(RouteSpec("/one", permission="user"),),
                test_tag="one",
            )
        )
        with self.assertRaises(ValueError):
            registry.register(
                FeatureManifest(
                    key="two", title="two", owner="test",
                    commands=(CommandSpec("same", permission="user"),),
                    test_tag="two",
                )
            )

    def test_registry_accepts_immutable_disabled_collection(self) -> None:
        registry = build_registry(disabled=frozenset({"daily_fortune"}))
        self.assertNotIn("daily_fortune", {feature.key for feature in registry.features})

    def test_lifecycle_is_idempotent_and_orders_callbacks(self) -> None:
        calls: list[str] = []
        lifecycle = Lifecycle()
        for phase in Lifecycle.ORDER:
            lifecycle.register(phase, lambda phase=phase: calls.append(phase.value))
        asyncio.run(lifecycle.start())
        asyncio.run(lifecycle.start())
        self.assertEqual(calls, [phase.value for phase in Lifecycle.ORDER])
        self.assertEqual(lifecycle.state.phase, LifecyclePhase.READY)

    def test_readiness_forced_state_does_not_evaluate_live_check(self) -> None:
        from nonebot_plugin_xiuxian_2.bootstrap import Readiness

        readiness = Readiness()
        readiness.register("database", lambda: (_ for _ in ()).throw(RuntimeError("closed")))
        readiness.set("database", True)
        report = readiness.report()
        self.assertTrue(report.ready)
        self.assertEqual(report.checks, {"database": True})

    def test_concurrent_lifecycle_start_runs_each_phase_once(self) -> None:
        calls: list[str] = []
        lifecycle = Lifecycle()
        for phase in Lifecycle.ORDER:
            lifecycle.register(phase, lambda phase=phase: calls.append(phase.value))

        async def run():
            await asyncio.gather(lifecycle.start(), lifecycle.start())

        asyncio.run(run())
        self.assertEqual(calls, [phase.value for phase in Lifecycle.ORDER])

    def test_failed_phase_runs_its_shutdown_callback(self) -> None:
        calls: list[str] = []
        lifecycle = Lifecycle()

        def ensure_filesystem() -> None:
            calls.append("ensure")
            raise RuntimeError("half-started")

        def shutdown_filesystem() -> None:
            calls.append("shutdown")

        lifecycle.register(
            LifecyclePhase.FILESYSTEM,
            ensure_filesystem,
            shutdown=shutdown_filesystem,
        )
        state = asyncio.run(lifecycle.start())
        self.assertEqual(state.phase, LifecyclePhase.NOT_READY)
        self.assertEqual(calls, ["ensure", "shutdown"])

    def test_daily_fortune_ledger_and_reconcile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "game.db")
            clock = lambda: datetime(2026, 9, 12, tzinfo=timezone.utc)
            random_source = type("Random", (), {"randint": lambda self, start, end: 90})()
            app = DailyFortuneApplication(database, clock=clock, random_source=random_source)
            first = app.claim(user_id="u-1", operation_id="op-1")
            replay = app.claim(user_id="u-1", operation_id="op-1")
            self.assertTrue(first.ok)
            self.assertEqual(replay.status, "replayed")
            with DatabaseUnitOfWork(database) as uow:
                report = ReconcileService().inspect(uow)
            self.assertTrue(report.clean)

    def test_migration_checksum_and_order_are_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "game.db")
            runner = MigrationRunner(
                [Migration("001", "one", lambda uow: uow.execute("CREATE TABLE sample (id INTEGER)"))]
            )
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(runner.apply(uow), ["001"])
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(runner.apply(uow), [])
                row = uow.query_one("SELECT version, checksum FROM schema_migrations")
                self.assertEqual(row["version"], "001")

    def test_migration_preview_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "game.db")
            runner = MigrationRunner(
                [Migration("001", "one", lambda uow: uow.execute("CREATE TABLE sample (id INTEGER)"))]
            )
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(runner.preview(uow), ["001"])
            with DatabaseUnitOfWork(database) as uow:
                table = uow.query_one(
                    "SELECT 1 AS present FROM sqlite_master "
                    "WHERE type = 'table' AND name = 'schema_migrations'"
                )
                self.assertIsNone(table)

    def test_cli_migrate_dry_run_does_not_record_schema_versions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(cli_main(["migrate", "--data-dir", directory, "--dry-run"]), 0)
            database = Path(directory) / "xiuxian.db"
            with sqlite3.connect(database) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
                    ).fetchone(),
                    None,
                )

    def test_trace_context_redacts_scope_and_restores_values(self) -> None:
        self.assertEqual(current_context()["operation_id"], "")
        with trace_context(request_id="req", operation_id="op", user_scope="user-1234"):
            context = current_context()
            self.assertEqual(context["request_id"], "req")
            self.assertEqual(context["operation_id"], "op")
            self.assertEqual(context["user_scope"], "us***34")
        self.assertEqual(current_context()["operation_id"], "")

    def test_migration_catalog_is_complete_and_reused_by_runtime(self) -> None:
        migrations = build_migrations()
        self.assertGreaterEqual(len(migrations), 50)
        versions = [migration.version for migration in migrations]
        self.assertEqual(versions, sorted(set(versions)))
        with tempfile.TemporaryDirectory() as directory:
            _lifecycle, _readiness, context = build_lifecycle(
                build_runtime_context(data_dir=directory)
            )
            self.assertEqual(
                versions,
                [migration.version for migration in context.migrations.migrations],
            )


if __name__ == "__main__":
    unittest.main()
