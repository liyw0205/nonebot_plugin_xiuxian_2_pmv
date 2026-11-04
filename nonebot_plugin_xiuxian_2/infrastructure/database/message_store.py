"""消息库（message_db）的权威 schema：`messages`、`user_nicknames`、`season_rank`。

这三张表此前只由请求路径创建：

- `xiuxian_utils/message_db.py::_ensure_message_db_schema`（每次连接消息库时建
  `messages` + `user_nicknames` + 15 个索引，并对历史库补 `messages.reference_id`）；
- `xiuxian_utils/season_rank_service.py::ensure_season_rank_table`（赛季排行榜每次读/写前建表）。

`message.001` 现在是唯一创建者；请求路径只校验形状，缺表或缺列即视为未就绪。
DDL 与索引名逐字保留，不改变任何列类型、默认值或查询语义。
"""

from __future__ import annotations

from typing import Any

from .uow import DatabaseUnitOfWork


SCHEMA_MISSING = "schema_missing"

MESSAGES_COLUMNS = frozenset(
    {
        "id", "adapter", "bot_id", "direction", "scene", "message_id", "reference_id",
        "source_message_id", "group_id", "group_name", "user_id", "username", "nickname",
        "avatar", "content", "reply_used_count", "created_at",
    }
)
USER_NICKNAMES_COLUMNS = frozenset(
    {
        "user_id", "username", "adapter", "bot_id", "source_scene", "source_group_id",
        "first_seen_at", "last_seen_at",
    }
)
SEASON_RANK_COLUMNS = frozenset(
    {
        "rank_key", "mode", "period_key", "rank_type", "user_id", "sect_id", "score",
        "extra", "updated_at",
    }
)

MESSAGES_DDL = """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            adapter TEXT,
            bot_id TEXT,

            direction TEXT NOT NULL,
            scene TEXT NOT NULL,

            message_id TEXT,
            reference_id TEXT,
            source_message_id TEXT,

            group_id TEXT,
            group_name TEXT,

            user_id TEXT,
            username TEXT,
            nickname TEXT,
            avatar TEXT,

            content TEXT,

            reply_used_count NUMERIC DEFAULT 0,

            created_at TEXT NOT NULL
        )
"""

USER_NICKNAMES_DDL = """
        CREATE TABLE IF NOT EXISTS user_nicknames (
            user_id TEXT PRIMARY KEY,

            username TEXT NOT NULL,

            adapter TEXT,
            bot_id TEXT,

            source_scene TEXT,
            source_group_id TEXT,

            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
"""

SEASON_RANK_DDL = """
            CREATE TABLE IF NOT EXISTS season_rank (
                rank_key TEXT NOT NULL,
                mode TEXT NOT NULL,
                period_key TEXT NOT NULL,
                rank_type TEXT NOT NULL,
                user_id TEXT NOT NULL DEFAULT '',
                sect_id INTEGER NOT NULL DEFAULT 0,
                score INTEGER NOT NULL DEFAULT 0,
                extra TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (rank_key, user_id, sect_id)
            )
"""

MESSAGES_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_messages_direction ON messages(direction)",
    "CREATE INDEX IF NOT EXISTS idx_messages_scene ON messages(scene)",
    "CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_reference_id ON messages(reference_id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_source_message_id ON messages(source_message_id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_group_id ON messages(group_id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_messages_group_latest ON messages(scene, adapter, group_id, id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_private_latest ON messages(scene, adapter, user_id, id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_group_time ON messages(scene, adapter, group_id, created_at, id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_private_time ON messages(scene, adapter, user_id, created_at, id)",
)

USER_NICKNAMES_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_user_nicknames_username ON user_nicknames(username)",
    "CREATE INDEX IF NOT EXISTS idx_user_nicknames_source_group_id ON user_nicknames(source_group_id)",
)

SEASON_RANK_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_season_rank_query ON season_rank(mode, period_key, rank_type, score)",
)

TABLES = {
    "messages": MESSAGES_COLUMNS,
    "user_nicknames": USER_NICKNAMES_COLUMNS,
    "season_rank": SEASON_RANK_COLUMNS,
}


def apply_message_store_schema(uow: DatabaseUnitOfWork) -> None:
    """Create the message-database tables (idempotent)."""
    uow.execute(MESSAGES_DDL)
    uow.execute(USER_NICKNAMES_DDL)
    # 历史库：messages 早期没有 reference_id 列，必须先补列再建引用它的索引。
    live = table_columns(uow, "messages")
    if live and "reference_id" not in live:
        uow.execute("ALTER TABLE messages ADD COLUMN reference_id TEXT")
    for statement in MESSAGES_INDEXES + USER_NICKNAMES_INDEXES:
        uow.execute(statement)


def apply_season_rank_schema(uow: DatabaseUnitOfWork) -> None:
    """Create the season-rank table in the game database (idempotent).

    `season_rank` joins `user_xiuxian`/`sects`, so it lives in the game database
    and not in the message database.
    """
    uow.execute(SEASON_RANK_DDL)
    for statement in SEASON_RANK_INDEXES:
        uow.execute(statement)


def table_columns(connection: Any, table: str) -> frozenset[str]:
    """Return the live column set of ``table`` for a UoW or a raw connection."""
    if hasattr(connection, "query_all"):
        rows = connection.query_all(f'PRAGMA table_info("{table}")')
        return frozenset(_column_name(row) for row in rows)
    cursor = connection.execute(f'PRAGMA table_info("{table}")')
    try:
        rows = cursor.fetchall()
    finally:
        close = getattr(cursor, "close", None)
        if callable(close):
            close()
    return frozenset(_column_name(row) for row in rows)


def table_ready(connection: Any, table: str, required: frozenset[str] = frozenset()) -> bool:
    actual = table_columns(connection, table)
    return bool(actual) and all(name.casefold() in actual for name in required)


MESSAGE_TABLES = {"messages": MESSAGES_COLUMNS, "user_nicknames": USER_NICKNAMES_COLUMNS}


def schema_ready(connection: Any) -> bool:
    """True when every message-database table has its full contract columns."""
    return all(table_ready(connection, table, columns) for table, columns in MESSAGE_TABLES.items())


def _column_name(row: Any) -> str:
    try:
        value = row["name"]
    except (TypeError, IndexError, KeyError):
        value = row[1]
    return str(value).casefold()


__all__ = [
    "MESSAGES_COLUMNS",
    "MESSAGES_DDL",
    "MESSAGES_INDEXES",
    "SCHEMA_MISSING",
    "SEASON_RANK_COLUMNS",
    "SEASON_RANK_DDL",
    "SEASON_RANK_INDEXES",
    "TABLES",
    "USER_NICKNAMES_COLUMNS",
    "USER_NICKNAMES_DDL",
    "USER_NICKNAMES_INDEXES",
    "MESSAGE_TABLES",
    "apply_message_store_schema",
    "apply_season_rank_schema",
    "schema_ready",
    "table_columns",
    "table_ready",
]
