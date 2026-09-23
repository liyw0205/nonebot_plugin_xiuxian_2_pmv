"""Create and validate an isolated backup/recovery rehearsal.

This script never reads the default data directory; callers must provide a
temporary ``--data-dir`` when exercising deployment tooling.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Importing the plugin package emits transport diagnostics on stdout.  Keep the
# real stdout reserved for the JSON receipt so operators can pipe this command
# into JSON tooling.
with redirect_stdout(sys.stderr):
    from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
    from nonebot_plugin_xiuxian_2.infrastructure.database import (
        BackupService,
        DatabaseUnitOfWork,
        MigrationRunner,
        ReconcileService,
    )
    from nonebot_plugin_xiuxian_2.infrastructure.database.attached_uow import (
        AttachedDatabaseUnitOfWork,
    )
    from nonebot_plugin_xiuxian_2.infrastructure.filesystem import atomic_write
    from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
    from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import (
        ATTACHED_OPERATION_VERSION,
        ATTACHED_SCHEMA_VERSION,
        apply_attached_player_accessory,
        apply_attached_player_accessory_operations,
    )
    from nonebot_plugin_xiuxian_2.features.daily_fortune.repository import DailyFortuneRepository


def _non_empty_data_dir(value: str) -> str:
    """Require an explicit path so recovery tooling cannot touch default data."""
    if not str(value).strip():
        raise argparse.ArgumentTypeError("--data-dir must be a non-empty path")
    return value


def _apply_catalog_migrations(context) -> tuple[dict[str, list[str]], dict[str, list[str]], list[str]]:
    """Mirror startup migration routing for every catalogued SQLite database."""
    catalog = build_migrations()
    applied_by_database: dict[str, list[str]] = {}
    migrations_by_database: dict[str, list[str]] = {}
    for spec in context.database.specs():
        selected = migrations_for_database(catalog, spec.key)
        with DatabaseUnitOfWork(spec.path) as uow:
            runner = MigrationRunner(selected, clock=context.clock)
            applied_by_database[spec.key] = runner.apply(uow)
            migrations_by_database[spec.key] = [
                str(row["version"])
                for row in uow.query_all("SELECT version FROM schema_migrations ORDER BY version")
            ]

    player_database = context.database.path("player_db")
    with AttachedDatabaseUnitOfWork(
        context.database.path("game_db"),
        attachments={"player_data": player_database},
        immediate=True,
    ) as attached_uow:
        attached_versions = [
            version
            for version, changed in (
                (ATTACHED_SCHEMA_VERSION, apply_attached_player_accessory(attached_uow, clock=context.clock)),
                (
                    ATTACHED_OPERATION_VERSION,
                    apply_attached_player_accessory_operations(
                        attached_uow, clock=context.clock
                    ),
                ),
            )
            if changed
        ]
    return applied_by_database, migrations_by_database, attached_versions


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, type=_non_empty_data_dir)
    parser.add_argument(
        "--evidence",
        help="write a machine-readable recovery receipt for the compatibility release gate",
    )
    args = parser.parse_args(argv)
    context = build_runtime_context(data_dir=args.data_dir)
    # Create every catalogued database before backup so a rehearsal covers the
    # same five files that a normal startup owns.
    for spec in context.database.specs():
        with DatabaseUnitOfWork(spec.path) as uow:
            if spec.key == "game_db":
                DailyFortuneRepository().ensure_schema(uow)
    backup = BackupService(context.database, extra_files={"config": context.paths.config_file}).create(context.paths.backups)
    dry_run = BackupService(context.database, extra_files={"config": context.paths.config_file}).restore(backup, dry_run=True)
    restored = BackupService(context.database, extra_files={"config": context.paths.config_file}).restore(backup)
    applied_by_database, migrations_by_database, attached_migrations = _apply_catalog_migrations(context)
    with DatabaseUnitOfWork(context.database.path("game_db")) as uow:
        report = ReconcileService().inspect(uow)
    migrations = sorted(
        {version for versions in migrations_by_database.values() for version in versions}
    )
    applied = sorted(
        {version for versions in applied_by_database.values() for version in versions}
    )
    result = {
        "schema": 1,
        "created_at": context.clock.now().isoformat(),
        "backup": backup.name,
        "backup_manifest_sha256": hashlib.sha256(
            (backup / "manifest.json").read_bytes()
        ).hexdigest(),
        "restore_dry_run": dry_run["restored"],
        "restore": restored["restored"],
        "migrations": migrations,
        "applied_migrations": applied,
        "migrations_by_database": migrations_by_database,
        "applied_migrations_by_database": applied_by_database,
        "attached_migrations": attached_migrations,
        "reconcile": report.to_dict(),
    }
    if args.evidence:
        evidence_path = Path(args.evidence).expanduser().resolve()
        atomic_write(
            evidence_path,
            json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"),
        )
        result["evidence"] = str(evidence_path)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if report.clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
