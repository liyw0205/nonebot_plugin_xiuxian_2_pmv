"""Read-only query facade for compatibility and Web projections."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Mapping


class ReadOnlyQuery:
    """A deliberately small query-only boundary.

    It rejects statements other than SELECT/PRAGMA and opens SQLite in
    ``mode=ro`` so compatibility pages cannot accidentally write assets.
    """

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.database.resolve()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _validate(sql: str) -> None:
        statement = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        if statement not in {"SELECT", "PRAGMA", "EXPLAIN"}:
            raise ValueError("read-only queries must be SELECT/PRAGMA/EXPLAIN statements")

    def one(self, sql: str, params: Any = ()) -> Mapping[str, Any] | None:
        self._validate(sql)
        with self._connect() as connection:
            row = connection.execute(sql, params).fetchone()
            return dict(row) if row is not None else None

    def all(self, sql: str, params: Any = ()) -> list[Mapping[str, Any]]:
        self._validate(sql)
        with self._connect() as connection:
            return [dict(row) for row in connection.execute(sql, params).fetchall()]


__all__ = ["ReadOnlyQuery"]
