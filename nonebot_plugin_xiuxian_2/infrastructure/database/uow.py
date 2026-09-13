from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping


class DatabaseUnitOfWork:
    """SQLite transaction with explicit commit/rollback semantics."""

    def __init__(self, database: str | Path, *, timeout: float = 30, immediate: bool = False) -> None:
        self.database = Path(database)
        self.timeout = timeout
        self.immediate = bool(immediate)
        self.connection: sqlite3.Connection | None = None

    def __enter__(self) -> "DatabaseUnitOfWork":
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.database, timeout=self.timeout)
        try:
            self.connection.row_factory = sqlite3.Row
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA busy_timeout=30000")
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("BEGIN IMMEDIATE" if self.immediate else "BEGIN")
        except Exception:
            # Exceptions raised while entering a context do not trigger
            # __exit__, so close the partially initialized connection here.
            self.connection.close()
            self.connection = None
            raise
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        if self.connection is None:
            return None
        try:
            if exc_type is None:
                self.connection.commit()
            else:
                self.connection.rollback()
        finally:
            self.connection.close()
            self.connection = None
        return None

    def _conn(self) -> sqlite3.Connection:
        if self.connection is None:
            raise RuntimeError("unit of work is not active")
        return self.connection

    def execute(self, sql: str, params: Any = ()) -> sqlite3.Cursor:
        return self._conn().execute(sql, params)

    def executemany(self, sql: str, params: Any) -> sqlite3.Cursor:
        return self._conn().executemany(sql, params)

    def query_one(self, sql: str, params: Any = ()) -> Mapping[str, Any] | None:
        row = self.execute(sql, params).fetchone()
        return dict(row) if row is not None else None

    def query_all(self, sql: str, params: Any = ()) -> list[Mapping[str, Any]]:
        return [dict(row) for row in self.execute(sql, params).fetchall()]

    @contextmanager
    def savepoint(self, name: str = "nested") -> Iterator[None]:
        safe_name = "".join(char if char.isalnum() or char == "_" else "_" for char in name)
        self.execute(f'SAVEPOINT "{safe_name}"')
        try:
            yield
        except Exception:
            self.execute(f'ROLLBACK TO SAVEPOINT "{safe_name}"')
            self.execute(f'RELEASE SAVEPOINT "{safe_name}"')
            raise
        else:
            self.execute(f'RELEASE SAVEPOINT "{safe_name}"')


__all__ = ["DatabaseUnitOfWork"]
