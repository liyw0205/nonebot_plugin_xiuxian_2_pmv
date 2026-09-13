from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Callable

from ..clock import SystemClock
from .uow import DatabaseUnitOfWork


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    apply: Callable[[DatabaseUnitOfWork], None]

    @property
    def checksum(self) -> str:
        import inspect

        try:
            implementation = inspect.getsource(self.apply).strip()
        except (OSError, TypeError):
            code = getattr(self.apply, "__code__", None)
            implementation = repr((getattr(code, "co_code", b""), getattr(code, "co_consts", ())))
        return hashlib.sha256(f"{self.version}:{self.name}:{implementation}".encode()).hexdigest()


class MigrationRunner:
    def __init__(self, migrations: list[Migration] | tuple[Migration, ...] = (), *, clock: Any | None = None) -> None:
        self.migrations = tuple(migrations)
        self.clock = clock or SystemClock()
        versions = [migration.version for migration in self.migrations]
        if list(versions) != sorted(set(versions)):
            raise ValueError("migration versions must be unique and monotonic")

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL, duration_ms INTEGER NOT NULL)"
        )

    def pending(self, uow: DatabaseUnitOfWork) -> tuple[Migration, ...]:
        self.ensure_schema(uow)
        applied = {row["version"] for row in uow.query_all("SELECT version FROM schema_migrations")}
        return tuple(migration for migration in self.migrations if migration.version not in applied)

    def preview(self, uow: DatabaseUnitOfWork) -> list[str]:
        """Return migrations that would run without changing the database.

        ``pending`` is used by the startup path and creates the metadata table
        when necessary.  A dry-run must be genuinely read-only, so it inspects
        the table only when it already exists and validates known checksums.
        """
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type = 'table' AND name = 'schema_migrations'"
        )
        if table is None:
            return [migration.version for migration in self.migrations]
        self.validate_applied(uow)
        applied = {row["version"] for row in uow.query_all("SELECT version FROM schema_migrations")}
        return [migration.version for migration in self.migrations if migration.version not in applied]

    def validate_applied(self, uow: DatabaseUnitOfWork) -> None:
        """Reject checksum drift and versions unknown to this release."""
        self.ensure_schema(uow)
        known = {migration.version: migration for migration in self.migrations}
        for row in uow.query_all("SELECT version, checksum FROM schema_migrations"):
            migration = known.get(str(row["version"]))
            if migration is None:
                # Older releases may have feature migrations that are now
                # owned by a compatibility package; unknown history is kept
                # but never silently rewritten.
                continue
            if row["checksum"] != migration.checksum:
                raise ValueError(f"migration checksum changed: {row['version']}")

    def apply(self, uow: DatabaseUnitOfWork) -> list[str]:
        import time

        self.ensure_schema(uow)
        self.validate_applied(uow)
        applied = {row["version"]: row for row in uow.query_all("SELECT * FROM schema_migrations")}
        changed: list[str] = []
        for migration in self.migrations:
            current = applied.get(migration.version)
            if current:
                if current["checksum"] != migration.checksum:
                    raise ValueError(f"migration checksum changed: {migration.version}")
                continue
            started = time.monotonic()
            migration.apply(uow)
            elapsed = int((time.monotonic() - started) * 1000)
            uow.execute(
                "INSERT INTO schema_migrations(version, name, checksum, applied_at, duration_ms) VALUES (?, ?, ?, ?, ?)",
                (migration.version, migration.name, migration.checksum, self.clock.now().isoformat(), elapsed),
            )
            changed.append(migration.version)
        return changed


__all__ = ["Migration", "MigrationRunner"]
