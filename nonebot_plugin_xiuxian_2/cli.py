from __future__ import annotations

import argparse
import asyncio
import json

from .bootstrap import build_runtime_context
from .plugin import build_migrations, build_registry, startup
from .infrastructure.database import (
    BackupService,
    DatabaseUnitOfWork,
    MigrationRunner,
    OperationLedger,
    OutboxStore,
    ReconcileService,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="xiuxian-refactor")
    parser.add_argument("command", choices=("manifest", "health", "migrate", "reconcile", "backup", "restore", "serve"))
    parser.add_argument("--data-dir")
    parser.add_argument("--backup")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5888)
    args = parser.parse_args(argv)
    if args.command == "manifest":
        print(json.dumps(build_registry().export(), ensure_ascii=False, indent=2))
        return 0
    context = build_runtime_context(
        data_dir=args.data_dir,
        # ``serve`` is the deployment entry point and must retain legacy
        # command compatibility; maintenance commands stay isolated.
        legacy_startup=(args.command == "serve"),
    )
    if args.command == "serve":
        state, readiness, context, lifecycle = asyncio.run(startup(context))
        if not readiness.is_ready():
            print(json.dumps({"state": state.phase.value, **readiness.report().to_dict()}, ensure_ascii=False))
            asyncio.run(lifecycle.shutdown())
            return 1
        try:
            context.web_app.run(host=args.host, port=args.port, debug=False, use_reloader=False)
        finally:
            asyncio.run(lifecycle.shutdown())
        return 0
    if args.command == "health":
        state, readiness, context, lifecycle = asyncio.run(startup(context))
        report = readiness.report().to_dict()
        asyncio.run(lifecycle.shutdown())
        print(json.dumps({"state": state.phase.value, "error": state.error, **report}, ensure_ascii=False))
        return 0 if report["ready"] else 1
    if args.command == "migrate":
        migrations = build_migrations()
        game_migrations = tuple(
            migration for migration in migrations
            if migration.version not in {"combat_settlement.003", "combat_settlement.004", "map.003", "map.005", "map.008", "map.013", "map.015", "map.016", "tianti_settlement.002", "tianti_training.003", "tianti_training.004", "tianti_training.005", "tianti_training.006"}
        )
        applied: dict[str, list[str]] = {}
        pending: dict[str, list[str]] = {}
        for spec in context.database.specs():
            if spec.key == "game_db":
                selected = game_migrations
            elif spec.key == "player_db":
                selected = tuple(
                    migration for migration in migrations
                    if migration.version in {"title.001", "combat_settlement.003", "combat_settlement.004", "map.003", "map.005", "map.008", "map.013", "map.015", "map.016", "tianti_settlement.002", "tianti_training.003", "tianti_training.004", "tianti_training.005"}
                )
            else:
                selected = ()
            with DatabaseUnitOfWork(spec.path) as uow:
                runner = MigrationRunner(selected, clock=context.clock)
                if args.dry_run:
                    pending[spec.key] = runner.preview(uow)
                else:
                    changed = runner.apply(uow)
                    if changed:
                        applied[spec.key] = changed
                    if spec.key == "game_db":
                        OperationLedger(clock=context.clock).ensure_schema(uow)
                        OutboxStore(clock=context.clock).ensure_schema(uow)
        if args.dry_run:
            print(json.dumps({"dry_run": True, "pending": pending}, ensure_ascii=False))
        else:
            print(json.dumps({"dry_run": False, "applied": applied}, ensure_ascii=False))
        return 0
    if args.command == "backup":
        directory = BackupService(context.database, extra_files={"config": context.paths.config_file}, clock=context.clock).create(context.paths.backups)
        print(json.dumps({"backup": str(directory)}, ensure_ascii=False))
        return 0
    if args.command == "restore":
        if not args.backup:
            parser.error("restore requires --backup")
        result = BackupService(context.database, extra_files={"config": context.paths.config_file}, clock=context.clock).restore(args.backup, dry_run=args.dry_run)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    with DatabaseUnitOfWork(context.database.path("game_db")) as uow:
        report = ReconcileService().inspect(uow)
    print(json.dumps(report.to_dict(), ensure_ascii=False, default=str))
    return 0 if report.clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
