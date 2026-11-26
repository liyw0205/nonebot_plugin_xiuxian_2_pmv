from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from threading import RLock, current_thread, get_ident, main_thread
from typing import Any, Mapping

from ...core.numeric import adapt_sqlite_params, register_numeric_functions


class _ConnectionScope:
    def __init__(self) -> None:
        self.thread_id = get_ident()
        self.active = True
        self.idle: tuple[tuple[Any, ...], sqlite3.Connection, Any] | None = None

    def available(self) -> bool:
        return self.active and self.thread_id == get_ident()

    def take(self, key: tuple[Any, ...]) -> tuple[sqlite3.Connection, Any] | None:
        if not self.available() or self.idle is None or self.idle[0] != key:
            return None
        _, connection, numeric_functions = self.idle
        self.idle = None
        try:
            if not connection.in_transaction:
                return connection, numeric_functions
        except sqlite3.ProgrammingError:
            pass
        connection.close()
        return None

    def put(self, key: tuple[Any, ...], connection: sqlite3.Connection, numeric_functions: Any) -> None:
        if not self.available():
            connection.close()
            return
        if self.idle is not None:
            _, previous, _ = self.idle
            self.idle = None
            previous.close()
        self.idle = key, connection, numeric_functions

    def close(self) -> None:
        self.active = False
        if self.idle is not None:
            _, connection, _ = self.idle
            self.idle = None
            connection.close()


_CONNECTION_SCOPE: ContextVar[_ConnectionScope | None] = ContextVar("database_uow_connection_scope", default=None)


class _IdleConnectionOwner:
    """Retain one idle main-thread connection, never a transaction or result.

    Callers must finish each unit of work synchronously. Other threads and
    reentrant calls use independent connections that close at their exit.
    Use query_one/query_all for fetched results, or propagate raw cursor errors.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._active = False
        self._retain = True
        self._idle: tuple[tuple[Any, ...], sqlite3.Connection, Any] | None = None

    def when_unscoped(self) -> _IdleConnectionOwner | None:
        """Select at UOW construction, preserving an explicit request's key/lifetime."""
        scope = _CONNECTION_SCOPE.get()
        return None if scope is not None and scope.available() else self

    def _acquire(self, key: tuple[Any, ...]) -> tuple[bool, tuple[sqlite3.Connection, Any] | None]:
        if current_thread() is not main_thread():
            return False, None
        self._lock.acquire()
        if self._active:
            self._lock.release()
            return False, None
        self._active, self._retain = True, True
        try:
            idle, self._idle = self._idle, None
            if idle is not None:
                previous_key, connection, numeric_functions = idle
                try:
                    clean = not connection.in_transaction
                except sqlite3.ProgrammingError:
                    clean = False
                if previous_key == key and clean:
                    return True, (connection, numeric_functions)
                connection.close()
            return True, None
        except BaseException:
            self._active = False
            self._lock.release()
            raise

    def _release(self, key, connection, numeric_functions, *, reusable: bool) -> None:
        try:
            if reusable and self._retain:
                self._idle = key, connection, numeric_functions
            elif connection is not None:
                connection.close()
        finally:
            self._active = False
            self._lock.release()

    def close(self) -> None:
        """Close on the main lifecycle thread; the next call may reopen lazily."""
        if current_thread() is not main_thread():
            raise RuntimeError("connection owner must close on the main thread")
        with self._lock:
            self._retain = False
            idle, self._idle = self._idle, None
            if idle is not None:
                idle[1].close()


class ReadOnlyConnectionOwner(_IdleConnectionOwner):
    """Retain a query-only reader across independent live read transactions."""


class WriteConnectionOwner(_IdleConnectionOwner):
    """Retain an opt-in writer across independent synchronous transactions."""


# Opt-in live projections share one idle handle, including across repository types.
LIVE_PROJECTION_READ_OWNER = ReadOnlyConnectionOwner()
# A separate player handle avoids evicting the live game projection reader.
PLAYER_PROJECTION_READ_OWNER = ReadOnlyConnectionOwner()
PLAYER_ACTIVITY_WRITE_OWNER = WriteConnectionOwner()


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
        read_owner: ReadOnlyConnectionOwner | None = None,
        write_owner: WriteConnectionOwner | None = None,
    ) -> None:
        self.database = Path(database)
        self.timeout = timeout
        self.immediate = bool(immediate)
        self.read_only = bool(read_only)
        self.foreign_keys = bool(foreign_keys)
        self.require_exists = bool(require_exists)
        self.query_only = bool(query_only) or read_owner is not None
        if read_owner is not None and not self.read_only:
            raise ValueError("a retained reader requires read_only=True")
        if write_owner is not None and (self.read_only or self.query_only or read_owner is not None):
            raise ValueError("a retained writer requires a writable unit of work")
        self._read_owner = read_owner or write_owner
        self._write_owned = write_owner is not None
        self._read_owner_active = False
        self._read_owner_key: tuple[Any, ...] | None = None
        self._read_cursors: list[sqlite3.Cursor] = []
        self._read_failed = False
        self.connection: sqlite3.Connection | None = None
        self._attached_schemas: set[str] = set()
        self._connection_scope: _ConnectionScope | None = None
        self._scope_key: tuple[Any, ...] | None = None

    @staticmethod
    @contextmanager
    def connection_scope() -> Iterator[None]:
        """Reuse at most one idle connection within an explicit sync request.

        Each unit of work still commits or rolls back its own transaction.
        Nested scopes are independent; copied contexts on another thread do
        not borrow the originating thread's connection.
        """
        scope = _ConnectionScope()
        token = _CONNECTION_SCOPE.set(scope)
        try:
            yield
        finally:
            _CONNECTION_SCOPE.reset(token)
            scope.close()

    def _connection_key(self) -> tuple[Any, ...]:
        path = self.database.resolve()
        try:
            stat = path.stat()
            identity = stat.st_dev, stat.st_ino
        except FileNotFoundError:
            identity = None
        return (path, identity, self.timeout, self.immediate, self.read_only,
                self.foreign_keys, self.require_exists, self.query_only)

    def __enter__(self) -> "DatabaseUnitOfWork":
        self._read_failed = False
        if self._read_owner is not None:
            self._read_owner_key = self._connection_key()
            self._read_owner_active, borrowed = self._read_owner._acquire(self._read_owner_key)
            try:
                return self._enter_connection(borrowed)
            except BaseException:
                connection, self.connection = self.connection, None
                if self._read_owner_active:
                    self._read_owner_active = False
                    self._read_owner._release(self._read_owner_key, connection, None, reusable=False)
                elif connection is not None:
                    connection.close()
                raise
        return self._enter_connection()

    @property
    def database_identity(self) -> tuple[str, int, int]:
        """Identity bound to this transaction's retained or scoped connection."""
        bound_key = self._read_owner_key or self._scope_key
        path, identity, *_ = bound_key or self._connection_key()
        if identity is None:
            raise FileNotFoundError(f"database is unavailable: {path}")
        return str(path), *identity

    @property
    def has_bound_database_identity(self) -> bool:
        return self._read_owner_key is not None or self._scope_key is not None

    def _check_read_identity(self) -> None:
        if self._read_owner is not None and self._read_owner_key != self._connection_key():
            kind = "write" if self._write_owned else "read-only"
            raise sqlite3.OperationalError(f"{kind} database identity changed")

    def _check_scoped_database_identity(self) -> None:
        if self._connection_scope is None or self._scope_key is None:
            return
        current_key = self._connection_key()
        if self._scope_key[1] is not None and self._scope_key != current_key:
            raise sqlite3.OperationalError("scoped database identity changed")
        self._scope_key = current_key

    def _enter_connection(self, borrowed=None) -> "DatabaseUnitOfWork":
        scope = _CONNECTION_SCOPE.get()
        self._connection_scope = scope if self._read_owner is None and scope is not None and scope.available() else None
        self._scope_key = self._connection_key() if self._connection_scope is not None else None
        if self.require_exists and not self.read_only and not self.database.is_file():
            raise FileNotFoundError(f"database is unavailable: {self.database}")
        if self._connection_scope is not None:
            borrowed = self._connection_scope.take(self._scope_key)
        if borrowed is not None:
            self.connection, self._numeric_functions = borrowed
            try:
                self.connection.execute("BEGIN IMMEDIATE" if self.immediate and not self.read_only else "BEGIN")
                self._check_scoped_database_identity()
                self._check_read_identity()
            except Exception:
                self.connection.close()
                self.connection = None
                raise
            return self
        if self.read_only:
            self.connection = sqlite3.connect(
                f"{self.database.resolve().as_uri()}?mode=ro",
                timeout=self.timeout,
                uri=True,
            )
        elif self.require_exists:
            # mode=rw keeps an explicit existence check free of the create-on-open
            # race, so a missing business database never becomes an empty file.
            self.connection = sqlite3.connect(
                f"{self.database.resolve().as_uri()}?mode=rw",
                timeout=self.timeout,
                uri=True,
            )
        else:
            self.database.parent.mkdir(parents=True, exist_ok=True)
            self.connection = sqlite3.connect(self.database, timeout=self.timeout)
        try:
            self._check_scoped_database_identity()
            self._numeric_functions = register_numeric_functions(self.connection)
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
            self._check_scoped_database_identity()
            self._check_read_identity()
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
        connection = self.connection
        reusable = False
        try:
            for cursor in self._read_cursors:
                cursor.close()
            self._read_cursors.clear()
            if exc_type is None:
                if self._write_owned:
                    self._check_read_identity()
                connection.commit()
            else:
                connection.rollback()
            for schema in tuple(self._attached_schemas):
                connection.execute(f'DETACH DATABASE "{schema}"')
            self._check_read_identity()
            reusable = not connection.in_transaction
        finally:
            self._read_cursors.clear()
            self._attached_schemas.clear()
            self.connection = None
            scope, key = self._connection_scope, self._scope_key
            self._connection_scope = None
            self._scope_key = None
            if self._read_owner_active:
                self._read_owner_active = False
                self._read_owner._release(
                    self._read_owner_key, connection, self._numeric_functions,
                    reusable=reusable and exc_type is None and not self._read_failed,
                )
            elif reusable and scope is not None and key is not None:
                try:
                    scope.put(key, connection, self._numeric_functions)
                except Exception:
                    connection.close()
                    raise
            else:
                connection.close()
        return None

    def _conn(self) -> sqlite3.Connection:
        if self.connection is None:
            raise RuntimeError("unit of work is not active")
        return self.connection

    def commit(self) -> None:
        """Commit before invoking an external post-transaction effect."""
        try:
            if self._write_owned:
                self._check_read_identity()
            self._conn().commit()
        except BaseException:
            self._read_failed = True
            raise

    def execute(self, sql: str, params: Any = ()) -> sqlite3.Cursor:
        # Central overflow guard: the raw driver aborts the whole transaction on
        # any int outside int64, so every statement binds normalized params.
        try:
            cursor = self._numeric_functions.run(self._conn().execute, sql, adapt_sqlite_params(params))
        except BaseException:
            self._read_failed = True
            raise
        if self._read_owner is not None:
            self._read_cursors.append(cursor)
        return cursor

    def executemany(self, sql: str, params: Any) -> sqlite3.Cursor:
        try:
            cursor = self._numeric_functions.run(self._conn().executemany,
                sql, (adapt_sqlite_params(row) for row in params)
            )
        except BaseException:
            self._read_failed = True
            raise
        if self._read_owner is not None:
            self._read_cursors.append(cursor)
        return cursor

    def query_one(self, sql: str, params: Any = ()) -> Mapping[str, Any] | None:
        cursor = self.execute(sql, params)
        try:
            row = self._numeric_functions.run(cursor.fetchone)
            return dict(row) if row is not None else None
        except BaseException:
            self._read_failed = True
            raise
        finally:
            if self._read_owner is not None:
                cursor.close()
                self._read_cursors.remove(cursor)

    def query_all(self, sql: str, params: Any = ()) -> list[Mapping[str, Any]]:
        cursor = self.execute(sql, params)
        try:
            return [dict(row) for row in self._numeric_functions.run(cursor.fetchall)]
        except BaseException:
            self._read_failed = True
            raise
        finally:
            if self._read_owner is not None:
                cursor.close()
                self._read_cursors.remove(cursor)

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


__all__ = ["DatabaseUnitOfWork", "ReadOnlyConnectionOwner", "WriteConnectionOwner", "LIVE_PROJECTION_READ_OWNER", "PLAYER_ACTIVITY_WRITE_OWNER", "is_database_busy"]
