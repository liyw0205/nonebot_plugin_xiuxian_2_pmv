from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

try:
    from nonebot.log import logger
except Exception:  # pragma: no cover
    logger = None


Error = sqlite3.Error
OperationalError = sqlite3.OperationalError
IntegrityError = sqlite3.IntegrityError
Row = sqlite3.Row
Connection = sqlite3.Connection

_backend_initialized = False
_BTRIM_RE = re.compile(r"\bbtrim\s*\(", flags=re.IGNORECASE)
_ILIKE_RE = re.compile(r"\bILIKE\b", flags=re.IGNORECASE)
_NULLS_LAST_RE = re.compile(r"\s+NULLS\s+LAST\b", flags=re.IGNORECASE)
_SKIP_LOCKED_RE = re.compile(
    r"\s+FOR\s+UPDATE\s+SKIP\s+LOCKED\b",
    flags=re.IGNORECASE,
)
_SET_LOCAL_RE = re.compile(r"^\s*SET\s+LOCAL\b.*$", flags=re.IGNORECASE | re.DOTALL)


def _log_info(message: str):
    if logger:
        logger.info(message)


def get_database_backend() -> str:
    return "sqlite"


def get_active_database_backend() -> str:
    _ensure_backend_initialized()
    return "sqlite"


def initialize_backend(force: bool = False) -> str:
    global _backend_initialized
    if _backend_initialized and not force:
        return "sqlite"
    _backend_initialized = True
    _log_info("[xiuxian-db] use SQLite backend")
    return "sqlite"


def is_backend_initialized() -> bool:
    return _backend_initialized


def _ensure_backend_initialized():
    if not _backend_initialized:
        initialize_backend()


def database_exists(database: str | Path) -> bool:
    return Path(database).exists()


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def qualify_ident(schema: str, name: str) -> str:
    return quote_ident(name)


def date_expression(value_sql: str) -> str:
    return f"substr(CAST({value_sql} AS TEXT), 1, 10)"


def _sql_segments(sql: str) -> Iterator[tuple[bool, str]]:
    """Yield ``(is_code, text)`` segments without parsing protected SQL text."""
    start = 0
    index = 0
    length = len(sql)

    while index < length:
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < length else ""
        if char not in {"'", '"'} and not (char == "-" and next_char == "-") and not (
            char == "/" and next_char == "*"
        ):
            index += 1
            continue

        if start < index:
            yield True, sql[start:index]

        protected_start = index
        if char in {"'", '"'}:
            quote = char
            index += 1
            while index < length:
                if sql[index] != quote:
                    index += 1
                    continue
                if index + 1 < length and sql[index + 1] == quote:
                    index += 2
                    continue
                index += 1
                break
        elif char == "-":
            newline = sql.find("\n", index + 2)
            index = length if newline < 0 else newline
        else:
            comment_end = sql.find("*/", index + 2)
            index = length if comment_end < 0 else comment_end + 2

        yield False, sql[protected_start:index]
        start = index

    if start < length:
        yield True, sql[start:]


def _convert_sql_code(sql: str) -> str:
    sql = _BTRIM_RE.sub("trim(", sql)
    sql = _ILIKE_RE.sub("LIKE", sql)
    sql = _NULLS_LAST_RE.sub("", sql)
    sql = _SKIP_LOCKED_RE.sub("", sql)
    return sql.replace("%s", "?")


def _convert_sql(sql: str) -> str:
    sql = str(sql or "")
    if _SET_LOCAL_RE.fullmatch(sql):
        return "SELECT 1"
    return "".join(
        _convert_sql_code(text) if is_code else text
        for is_code, text in _sql_segments(sql)
    )


def _adapt_param(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    return value


def _adapt_params(params: Any) -> Any:
    if params is None:
        return ()
    if isinstance(params, dict):
        return {key: _adapt_param(value) for key, value in params.items()}
    if isinstance(params, (list, tuple)):
        return tuple(_adapt_param(value) for value in params)
    return (_adapt_param(params),)


def _least(*values):
    present = [value for value in values if value is not None]
    return min(present) if present else None


def _greatest(*values):
    present = [value for value in values if value is not None]
    return max(present) if present else None


class SQLiteCursor:
    def __init__(self, conn: "SQLiteConnection", cursor: sqlite3.Cursor):
        self.connection = conn
        self._cursor = cursor

    @property
    def description(self):
        return self._cursor.description

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def close(self):
        self._cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def execute(self, sql: str, params: Any = None):
        self._cursor.execute(_convert_sql(sql), _adapt_params(params))
        return self

    def executemany(self, sql: str, seq_of_params: Iterable[Any]):
        self._cursor.executemany(
            _convert_sql(sql),
            (_adapt_params(params) for params in seq_of_params),
        )
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def __iter__(self):
        return iter(self._cursor)


class SQLiteConnection:
    def __init__(self, raw: sqlite3.Connection, db_path: Path):
        self._raw = raw
        self.db_path = db_path
        self._pk_cache: dict[str, list[str]] = {}

    @property
    def row_factory(self):
        return self._raw.row_factory

    @row_factory.setter
    def row_factory(self, value):
        if value is Row or value == Row:
            self._raw.row_factory = sqlite3.Row
        else:
            self._raw.row_factory = value

    def cursor(self):
        return SQLiteCursor(self, self._raw.cursor())

    def execute(self, sql: str, params: Any = None):
        cur = self.cursor()
        return cur.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Iterable[Any]):
        cur = self.cursor()
        return cur.executemany(sql, seq_of_params)

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def close(self):
        self._raw.close()

    def table_exists(self, table_name: str) -> bool:
        cur = self._raw.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND lower(name)=lower(?) LIMIT 1",
            (str(table_name),),
        )
        return cur.fetchone() is not None

    def list_tables(self) -> list[str]:
        cur = self._raw.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return [row[0] for row in cur.fetchall()]

    def table_info(self, table_name: str) -> list[tuple[Any, ...]]:
        cur = self._raw.execute(f"PRAGMA table_info({quote_ident(table_name)})")
        return [tuple(row) for row in cur.fetchall()]

    def column_names(self, table_name: str) -> list[str]:
        return [row[1] for row in self.table_info(table_name)]

    def column_exists(self, table_name: str, column_name: str) -> bool:
        return str(column_name).lower() in {str(name).lower() for name in self.column_names(table_name)}

    def get_primary_key_columns(self, table_name: str) -> list[str]:
        table_key = str(table_name).lower()
        if table_key not in self._pk_cache:
            pk_cols = sorted(
                ((int(row[5]), str(row[1])) for row in self.table_info(table_name) if row[5]),
                key=lambda item: item[0],
            )
            self._pk_cache[table_key] = [name for _, name in pk_cols]
        return self._pk_cache[table_key]


def connect(database: str | Path, *args, **kwargs) -> SQLiteConnection:
    _ensure_backend_initialized()
    db_path = Path(database)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    raw = sqlite3.connect(db_path, *args, **kwargs)
    raw.execute("PRAGMA journal_mode=WAL")
    raw.execute("PRAGMA busy_timeout=30000")
    raw.execute("PRAGMA synchronous=NORMAL")
    raw.create_function("LEAST", -1, _least)
    raw.create_function("GREATEST", -1, _greatest)
    return SQLiteConnection(raw, db_path)


@contextmanager
def connection(database: str | Path, *args, row_factory: Any = Row, **kwargs):
    """统一的短连接入口，供 Web/API 查询和脚本任务复用。"""
    conn = connect(database, *args, **kwargs)
    if row_factory is not None:
        conn.row_factory = row_factory
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(database: str | Path, *args, row_factory: Any = Row, **kwargs):
    """事务入口：异常回滚，正常提交。"""
    conn = connect(database, *args, **kwargs)
    if row_factory is not None:
        conn.row_factory = row_factory
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _row_to_dict(row: Any) -> Any:
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return row


def query_all(database: str | Path, sql: str, params: Any = None) -> list[Any]:
    """执行查询并返回 dict 列表。"""
    with connection(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [_row_to_dict(row) for row in cur.fetchall()]


def query_one(database: str | Path, sql: str, params: Any = None) -> Any:
    """执行查询并返回单行 dict；无记录返回 None。"""
    with connection(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return _row_to_dict(cur.fetchone())


def execute_write(database: str | Path, sql: str, params: Any = None) -> int:
    """执行写 SQL 并返回影响行数。"""
    with transaction(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


def execute_sql(database: str | Path, sql: str, params: Any = None) -> Any:
    """
    兼容旧 Web 层的统一 SQL 执行入口。

    查询语句返回 dict 列表，写语句返回 {"affected_rows": n}。
    """
    with connection(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                return [_row_to_dict(row) for row in cur.fetchall()]
            conn.commit()
            return {"affected_rows": cur.rowcount}


def execute_sql_safely(database: str | Path, sql: str, params: Any = None) -> Any:
    try:
        return execute_sql(database, sql, params)
    except Exception as e:
        return {"error": str(e)}


def ensure_columns(database: str | Path, table_name: str, columns: dict[str, str]) -> list[str]:
    """按需为表补列，返回本次新增列名。"""
    added: list[str] = []
    with transaction(database) as conn:
        existing = {name.lower() for name in conn.column_names(table_name)}
        cur = conn.cursor()
        for column_name, definition in columns.items():
            if str(column_name).lower() in existing:
                continue
            cur.execute(
                f"ALTER TABLE {quote_ident(table_name)} "
                f"ADD COLUMN {quote_ident(column_name)} {definition}"
            )
            added.append(str(column_name))
    return added


def list_tables(database: str | Path) -> list[str]:
    conn = connect(database)
    try:
        return conn.list_tables()
    finally:
        conn.close()


def table_info(database: str | Path, table_name: str) -> list[tuple[Any, ...]]:
    conn = connect(database)
    try:
        return conn.table_info(table_name)
    finally:
        conn.close()


def table_exists(database: str | Path, table_name: str) -> bool:
    conn = connect(database)
    try:
        return conn.table_exists(table_name)
    finally:
        conn.close()


def column_exists(database: str | Path, table_name: str, column_name: str) -> bool:
    conn = connect(database)
    try:
        return conn.column_exists(table_name, column_name)
    finally:
        conn.close()
