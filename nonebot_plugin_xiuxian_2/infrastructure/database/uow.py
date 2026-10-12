from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping


class DatabaseUnitOfWork:
    """SQLite transaction with explicit commit/rollback semantics."""

    def __init__(
        self,
        database: str | Path,
        *,
        timeout: float = 30,
        immediate: bool = False,
        read_only: bool = False,
        foreign_keys: bool = True,
        require_exists: bool = False,
        query_only: bool = False,
    ) -> None:
        self.database = Path(database)
        self.timeout = timeout
        self.immediate = bool(immediate)
        self.read_only = bool(read_only)
        self.foreign_keys = bool(foreign_keys)
        self.require_exists = bool(require_exists)
        self.query_only = bool(query_only)
        self.connection: sqlite3.Connection | None = None
        self._attached_schemas: set[str] = set()

    def __enter__(self) -> "DatabaseUnitOfWork":
        if self.read_only:
            self.connection = sqlite3.connect(
                f"{self.database.resolve().as_uri()}?mode=ro",
                timeout=self.timeout,
                uri=True,
            )
        elif self.require_exists:
            # mode=rw keeps an explicit existence check free of the create-on-open
            # race, so a missing business database never becomes an empty file.
            if not self.database.is_file():
                raise FileNotFoundError(f"database is unavailable: {self.database}")
            self.connection = sqlite3.connect(
                f"{self.database.resolve().as_uri()}?mode=rw",
                timeout=self.timeout,
                uri=True,
            )
        else:
            self.database.parent.mkdir(parents=True, exist_ok=True)
            self.connection = sqlite3.connect(self.database, timeout=self.timeout)
        try:
            self.connection.row_factory = sqlite3.Row
            # busy_timeout must be in effect before any statement that could
            # block on another connection's lock.
            self.connection.execute(f"PRAGMA busy_timeout={max(int(self.timeout * 1000), 0)}")
            if not self.read_only:
                self._ensure_wal()
            self.connection.execute(f"PRAGMA foreign_keys={'ON' if self.foreign_keys else 'OFF'}")
            if self.query_only:
                self.connection.execute("PRAGMA query_only=ON")
            self.connection.execute(
                "BEGIN IMMEDIATE" if self.immediate and not self.read_only else "BEGIN"
            )
        except Exception:
            # Exceptions raised while entering a context do not trigger
            # __exit__, so close the partially initialized connection here.
            self.connection.close()
            self.connection = None
            raise
        return self

    def _ensure_wal(self) -> None:
        """Put the database in WAL mode once; the mode is persistent.

        The mode is stored in the database header, so only the initializing
        connection needs to convert it; every later open just reads "wal" and
        skips the write-lock-taking conversion.  The conversion itself does
        not honor busy_timeout — it returns SQLITE_BUSY immediately when
        another connection is mid-transaction — so retry it within the same
        overall timeout budget the connection was opened with.
        """
        connection = self._conn()
        deadline = time.monotonic() + max(self.timeout, 0.0)
        while True:
            row = connection.execute("PRAGMA journal_mode").fetchone()
            if row is not None and str(row[0]).lower() == "wal":
                return
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                return
            except sqlite3.OperationalError as exc:
                if not is_database_busy(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None:
        if self.connection is None:
            return None
        try:
            if exc_type is None:
                self.connection.commit()
            else:
                self.connection.rollback()
        finally:
            for schema in tuple(self._attached_schemas):
                self.connection.execute(f'DETACH DATABASE "{schema}"')
            self._attached_schemas.clear()
            self.connection.close()
            self.connection = None
        return None

    def _conn(self) -> sqlite3.Connection:
        if self.connection is None:
            raise RuntimeError("unit of work is not active")
        return self.connection

    def commit(self) -> None:
        """Commit before invoking an external post-transaction effect."""
        self._conn().commit()

    def execute(self, sql: str, params: Any = ()) -> sqlite3.Cursor:
        return self._conn().execute(sql, params)

    def executemany(self, sql: str, params: Any) -> sqlite3.Cursor:
        return self._conn().executemany(sql, params)

    def query_one(self, sql: str, params: Any = ()) -> Mapping[str, Any] | None:
        row = self.execute(sql, params).fetchone()
        return dict(row) if row is not None else None

    def query_all(self, sql: str, params: Any = ()) -> list[Mapping[str, Any]]:
        return [dict(row) for row in self.execute(sql, params).fetchall()]

    def attach_database(self, database: str | Path, schema: str, *, read_only: bool = False) -> None:
        """Attach a catalogued secondary database to this transaction."""
        safe_schema = "".join(char if char.isalnum() or char == "_" else "_" for char in schema)
        target = database
        if read_only:
            target = f"{Path(database).resolve().as_uri()}?mode=ro"
        self.execute(f'ATTACH DATABASE ? AS "{safe_schema}"', (str(target),))
        self._attached_schemas.add(safe_schema)

    def detach_database(self, schema: str) -> None:
        safe_schema = "".join(char if char.isalnum() or char == "_" else "_" for char in schema)
        self.execute(f'DETACH DATABASE "{safe_schema}"')
        self._attached_schemas.discard(safe_schema)

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


def is_database_busy(exc: BaseException) -> bool:
    return (
        isinstance(exc, sqlite3.OperationalError)
        and (getattr(exc, "sqlite_errorcode", 0) & 0xFF) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
    )


__all__ = ["DatabaseUnitOfWork", "is_database_busy"]
