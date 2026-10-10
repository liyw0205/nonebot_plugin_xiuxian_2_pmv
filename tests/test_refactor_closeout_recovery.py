from __future__ import annotations

import asyncio
from contextlib import closing
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.infrastructure.database import (
    BackupService,
    DatabaseUnitOfWork,
    Migration,
    MigrationRunner,
    OutboxStore,
    ReconcileService,
)
from nonebot_plugin_xiuxian_2.infrastructure.filesystem import atomic_write
from nonebot_plugin_xiuxian_2.plugin import (
    build_migrations,
    migrations_for_database,
    shutdown,
    startup,
)


def _snapshot(context):
    result = {}
    for spec in context.database.specs():
        with closing(sqlite3.connect(f"{spec.path.resolve().as_uri()}?mode=ro", uri=True)) as conn:
            result[spec.key] = hashlib.sha256("\n".join(conn.iterdump()).encode()).hexdigest()
    result["config"] = hashlib.sha256(context.paths.config_file.read_bytes()).hexdigest()
    return result


def _versions(context, catalog):
    result = {}
    for spec in context.database.specs():
        expected = [migration.version for migration in migrations_for_database(catalog, spec.key)]
        with DatabaseUnitOfWork(spec.path, read_only=True) as uow:
            actual = [str(row["version"]) for row in uow.query_all("SELECT version FROM schema_migrations ORDER BY version")]
            assert actual == expected
            assert MigrationRunner(migrations_for_database(catalog, spec.key)).preview(uow) == []
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
        result[spec.key] = actual
    with DatabaseUnitOfWork(context.database.path("player_db"), read_only=True) as uow:
        attached = [row["version"] for row in uow.query_all("SELECT version FROM attached_schema_migrations ORDER BY version")]
    assert attached == [f"accessory_package.player_data.00{index}" for index in (1, 2, 3)]
    assert "buff.013" in result["player_db"] and "buff.013" not in result["game_db"]
    assert "game_events.001" in result["player_db"] and "game_events.001" not in result["game_db"]
    assert "economy_ledger.001" in result["game_db"] and "economy_ledger.001" not in result["player_db"]
    return result, attached


def _start_and_stop(context):
    async def run():
        state, readiness, _, lifecycle = await startup(context)
        try:
            report = readiness.report().to_dict()
            assert state.phase.value == "ready", state.error
            assert report["ready"] and all(report["checks"].values())
            assert set(report["checks"]) == {"filesystem", "database", "migrations", "repositories", "jobs", "web"}
            assert context.web_app is None
            return report
        finally:
            stopped = await shutdown(lifecycle)
            assert stopped.phase.value == "stopped"
    return asyncio.run(run())


def test_closeout_five_database_migration_backup_restore_and_reconcile(tmp_path: Path, record_property):
    context = build_runtime_context(data_dir=tmp_path / "data", legacy_startup=False)
    atomic_write(context.paths.config_file, b'{"closeout_fixture": "original"}\n')
    specs = context.database.specs()
    keys = {spec.key for spec in specs}
    assert keys == {"game_db", "player_db", "trade_db", "impart_db", "message_db"}
    for spec in specs:
        with DatabaseUnitOfWork(spec.path) as uow:
            uow.execute("CREATE TABLE closeout_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
            uow.execute("INSERT INTO closeout_probe VALUES (1, 'original')")
    initial = _snapshot(context)
    catalog = build_migrations()
    previews = {}
    for spec in specs:
        selected = migrations_for_database(catalog, spec.key)
        with DatabaseUnitOfWork(spec.path, read_only=True) as uow:
            previews[spec.key] = MigrationRunner(selected).preview(uow)
        assert previews[spec.key] == [migration.version for migration in selected]
    assert _snapshot(context) == initial

    def fail_migration(uow):
        uow.execute("CREATE TABLE closeout_rollback_probe (id INTEGER)")
        raise RuntimeError("closeout injected migration failure")

    # A failure after each real routed migration batch must roll back its DDL,
    # metadata and data together, preserving the pre-migration baseline.
    for spec in specs:
        selected = migrations_for_database(catalog, spec.key)
        with pytest.raises(RuntimeError, match="closeout injected migration failure"):
            with DatabaseUnitOfWork(spec.path) as uow:
                MigrationRunner((*selected, Migration("zzcloseout.001", "injected_failure", fail_migration))).apply(uow)
        assert _snapshot(context) == initial

    backups = BackupService(context.database, extra_files={"config": context.paths.config_file})
    before = backups.create(tmp_path / "backups-before")
    assert set(backups.restore(before, dry_run=True)["restored"]) == keys | {"config"}
    assert _snapshot(context) == initial
    ready = _start_and_stop(context)
    versions, attached = _versions(context, catalog)
    outbox = OutboxStore()
    for spec in specs:
        with DatabaseUnitOfWork(spec.path) as uow:
            outbox.append(
                uow, event_id=f"closeout:{spec.key}", aggregate_type="closeout",
                aggregate_id="fixture", event_type="closeout.test", payload={"database": spec.key},
            )
    migrated = _snapshot(context)
    after = backups.create(tmp_path / "backups-after")
    for spec in specs:
        with DatabaseUnitOfWork(spec.path) as uow:
            uow.execute("UPDATE closeout_probe SET value='mutated'")
    atomic_write(context.paths.config_file, b'{"closeout_fixture": "mutated"}\n')
    mutated = _snapshot(context)
    assert all(mutated[key] != migrated[key] for key in migrated)
    assert set(backups.restore(after, dry_run=True)["restored"]) == keys | {"config"}
    assert _snapshot(context) == mutated
    assert set(backups.restore(after)["restored"]) == keys | {"config"}
    assert _snapshot(context) == migrated
    assert _versions(context, catalog) == (versions, attached)

    deliveries, reconciliation = [], {}
    for spec in specs:
        with DatabaseUnitOfWork(spec.path) as uow:
            service = ReconcileService()
            pending = service.inspect(uow)
            assert pending.outbox_events == 1 and pending.operations == 0
            report = service.run(uow, handlers={"closeout.test": lambda event: deliveries.append(event["payload"]["database"])})
            assert report.clean
            assert service.run(uow, handlers={"closeout.test": lambda event: deliveries.append("unexpected")}).clean
            reconciliation[spec.key] = report.to_dict()
    assert sorted(deliveries) == sorted(keys)

    assert set(backups.restore(before)["restored"]) == keys | {"config"}
    assert _snapshot(context) == initial
    restored_ready = _start_and_stop(context)
    assert _versions(context, catalog) == (versions, attached)
    record_property("closeout_recovery", json.dumps({
        "data_dir": str(context.paths.data), "before_backup": str(before), "after_backup": str(after),
        "migration_preview_counts": {key: len(value) for key, value in previews.items()},
        "versions_by_database": versions, "attached": attached,
        "readiness": ready, "restored_readiness": restored_ready,
        "reconcile": reconciliation, "migration_rollback_databases": sorted(keys),
        "pre_snapshot_restored": True, "post_snapshot_restored": True,
        "synthetic_only": True,
    }, sort_keys=True))
