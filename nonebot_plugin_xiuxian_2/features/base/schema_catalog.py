from __future__ import annotations

from pathlib import Path
from typing import Any

from ...xiuxian.xiuxian_utils.shared_read_cache import (
    process_read_cache,
    shared_snapshot_key,
)


def read_sqlite_table_names(connection: Any) -> frozenset[str]:
    """Read table names live within the caller's current transaction."""
    return frozenset(
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    )


def sqlite_table_names(connection: Any, database_path: str | Path) -> frozenset[str]:
    """Read a bounded table-name snapshot keyed by the live SQLite schema version."""
    version_row = connection.execute("PRAGMA schema_version").fetchone()
    schema_version = int(version_row[0]) if version_row is not None else -1
    key = shared_snapshot_key(
        database_path, "sqlite-table-catalog-v1", schema_version
    )
    return process_read_cache.get_or_load(
        key,
        database_path=database_path,
        ttl=60 * 60,
        snapshot=True,
        loader=lambda: read_sqlite_table_names(connection),
    )


def sqlite_table_columns(
    connection: Any,
    database_path: str | Path,
    table: str,
    *,
    schema: str = "main",
) -> frozenset[str]:
    """Read bounded column metadata keyed by the live SQLite schema version."""
    if schema not in {"main", "player_data"}:
        raise ValueError("unsupported SQLite schema")
    table_name = str(table).strip()
    if not table_name:
        return frozenset()
    version_row = connection.execute(f"PRAGMA {schema}.schema_version").fetchone()
    schema_version = int(version_row[0]) if version_row is not None else -1
    key = shared_snapshot_key(
        database_path,
        "sqlite-table-columns-v1",
        schema,
        table_name.casefold(),
        schema_version,
    )

    def load_columns() -> frozenset[str]:
        escaped = table_name.replace('"', '""')
        return frozenset(
            str(row[1]).casefold()
            for row in connection.execute(
                f'PRAGMA {schema}.table_info("{escaped}")'
            ).fetchall()
        )

    return process_read_cache.get_or_load(
        key,
        database_path=database_path,
        ttl=60 * 60,
        snapshot=True,
        loader=load_columns,
    )


__all__ = ["read_sqlite_table_names", "sqlite_table_columns", "sqlite_table_names"]
