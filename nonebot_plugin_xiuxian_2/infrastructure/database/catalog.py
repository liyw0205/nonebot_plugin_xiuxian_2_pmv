from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class DatabaseSpec:
    key: str
    path: Path
    writable: bool = True


class DatabaseCatalog:
    """Single declaration of ownership for the plugin's SQLite files."""

    KEYS = ("game_db", "player_db", "trade_db", "impart_db", "message_db")

    def __init__(self, specs: Mapping[str, DatabaseSpec]) -> None:
        unknown = set(specs) - set(self.KEYS)
        if unknown:
            raise ValueError(f"unknown database keys: {sorted(unknown)}")
        missing = set(self.KEYS) - set(specs)
        if missing:
            raise ValueError(f"missing database keys: {sorted(missing)}")
        self._specs = dict(specs)

    @classmethod
    def from_paths(cls, paths: Any) -> "DatabaseCatalog":
        def resolve(key: str) -> Path:
            value = getattr(paths, key, None)
            if value is None and isinstance(paths, Mapping):
                value = paths.get(key)
            if value is None:
                raise ValueError(f"database path is missing: {key}")
            return Path(value)

        return cls(
            {
                key: DatabaseSpec(key, resolve(key))
                for key in cls.KEYS
            }
        )

    def get(self, key: str) -> DatabaseSpec:
        try:
            return self._specs[key]
        except KeyError as exc:
            raise KeyError(f"unknown database: {key}") from exc

    def path(self, key: str) -> Path:
        return self.get(key).path

    def keys(self) -> tuple[str, ...]:
        return tuple(self._specs)

    def specs(self) -> tuple[DatabaseSpec, ...]:
        return tuple(self._specs.values())

    def owner(self, key: str) -> str:
        """Return the stable ownership key used by repositories and logs."""
        self.get(key)
        return key

    def unit_of_work(self, key: str):
        spec = self.get(key)
        if not spec.writable:
            raise PermissionError(f"database is read-only: {key}")
        from .uow import DatabaseUnitOfWork

        return DatabaseUnitOfWork(spec.path)

    def readonly_connection(self, key: str):
        """Return a read-only sqlite connection for compatibility projections."""
        import sqlite3

        uri = f"file:{self.path(key).resolve()}?mode=ro"
        return sqlite3.connect(uri, uri=True)

    def readonly_query(self, key: str):
        from .readonly import ReadOnlyQuery

        return ReadOnlyQuery(self.path(key))

    def export(self, *, redact: bool = False) -> dict[str, Any]:
        return {
            key: {"path": str(spec.path) if not redact else spec.path.name, "writable": spec.writable}
            for key, spec in self._specs.items()
        }


__all__ = ["DatabaseCatalog", "DatabaseSpec"]
