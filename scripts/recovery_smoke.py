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
    from nonebot_plugin_xiuxian_2.infrastructure.database import BackupService, DatabaseUnitOfWork, MigrationRunner, ReconcileService
    from nonebot_plugin_xiuxian_2.infrastructure.filesystem import atomic_write
    from nonebot_plugin_xiuxian_2.plugin import build_migrations
    from nonebot_plugin_xiuxian_2.features.daily_fortune.repository import DailyFortuneRepository


def _non_empty_data_dir(value: str) -> str:
    """Require an explicit path so recovery tooling cannot touch default data."""
    if not str(value).strip():
        raise argparse.ArgumentTypeError("--data-dir must be a non-empty path")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, type=_non_empty_data_dir)
    parser.add_argument(
        "--evidence",
        help="write a machine-readable recovery receipt for the compatibility release gate",
    )
    args = parser.parse_args(argv)
    context = build_runtime_context(data_dir=args.data_dir)
    with DatabaseUnitOfWork(context.database.path("game_db")) as uow:
        DailyFortuneRepository().ensure_schema(uow)
    backup = BackupService(context.database, extra_files={"config": context.paths.config_file}).create(context.paths.backups)
    dry_run = BackupService(context.database, extra_files={"config": context.paths.config_file}).restore(backup, dry_run=True)
    restored = BackupService(context.database, extra_files={"config": context.paths.config_file}).restore(backup)
    with DatabaseUnitOfWork(context.database.path("game_db")) as uow:
        runner = MigrationRunner(build_migrations())
        applied = runner.apply(uow)
        migration_rows = uow.query_all("SELECT version FROM schema_migrations ORDER BY version")
        migrations = [str(row["version"]) for row in migration_rows]
        report = ReconcileService().inspect(uow)
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
