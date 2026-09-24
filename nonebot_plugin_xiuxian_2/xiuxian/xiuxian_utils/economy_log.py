from __future__ import annotations

from datetime import datetime
from typing import Any

try:
    from nonebot.log import logger
except Exception:  # pragma: no cover
    logger = None

from .json_store import safe_json_dumps as _json_dumps
from .xiuxian2_handle import XiuxianDateManage


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_time(value: datetime | str | None = None) -> str:
    if value is None:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def ensure_economy_log_table() -> None:
    sql_message = XiuxianDateManage()
    with sql_message.lock:
        cur = sql_message.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS economy_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                sect_id INTEGER,
                source TEXT NOT NULL,
                action TEXT NOT NULL,
                stone_delta INTEGER NOT NULL DEFAULT 0,
                exp_delta INTEGER NOT NULL DEFAULT 0,
                sect_contribution_delta INTEGER NOT NULL DEFAULT 0,
                sect_scale_delta INTEGER NOT NULL DEFAULT 0,
                sect_materials_delta INTEGER NOT NULL DEFAULT 0,
                item_delta TEXT NOT NULL DEFAULT '[]',
                detail TEXT NOT NULL DEFAULT '{}',
                trace_id TEXT,
                event_id TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        if not sql_message.conn.column_exists("economy_log", "trace_id"):
            cur.execute("ALTER TABLE economy_log ADD COLUMN trace_id TEXT")
        if not sql_message.conn.column_exists("economy_log", "event_id"):
            cur.execute("ALTER TABLE economy_log ADD COLUMN event_id TEXT")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_economy_log_user_time "
            "ON economy_log(user_id, created_at)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_economy_log_sect_time "
            "ON economy_log(sect_id, created_at)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_economy_log_source_action "
            "ON economy_log(source, action)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_economy_log_trace_id "
            "ON economy_log(trace_id)"
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_economy_log_event_id "
            "ON economy_log(event_id) WHERE event_id IS NOT NULL"
        )
        sql_message._commit_write()


def log_economy_change(
    *,
    source: str,
    action: str,
    user_id: str | int | None = None,
    sect_id: str | int | None = None,
    stone_delta: int = 0,
    exp_delta: int = 0,
    sect_contribution_delta: int = 0,
    sect_scale_delta: int = 0,
    sect_materials_delta: int = 0,
    item_delta: list[dict[str, Any]] | dict[str, Any] | None = None,
    detail: dict[str, Any] | None = None,
    trace_id: str | int | None = None,
    event_id: str | None = None,
    created_at: datetime | str | None = None,
) -> int:
    ensure_economy_log_table()
    sql_message = XiuxianDateManage()
    user_id_text = None if user_id is None else str(user_id)
    sect_id_int = None if sect_id in (None, "") else _to_int(sect_id)
    item_delta_text = _json_dumps(item_delta, [])
    detail_text = _json_dumps(detail, {})
    event_id_text = None if event_id in (None, "") else str(event_id)

    with sql_message.lock:
        cur = sql_message.conn.cursor()
        values = (
            user_id_text,
            sect_id_int,
            str(source),
            str(action),
            _to_int(stone_delta),
            _to_int(exp_delta),
            _to_int(sect_contribution_delta),
            _to_int(sect_scale_delta),
            _to_int(sect_materials_delta),
            item_delta_text,
            detail_text,
            None if trace_id in (None, "") else str(trace_id),
            event_id_text,
            _normalize_time(created_at),
        )
        insert_sql = """
            INSERT INTO economy_log (
                user_id, sect_id, source, action,
                stone_delta, exp_delta, sect_contribution_delta,
                sect_scale_delta, sect_materials_delta,
                item_delta, detail, trace_id, event_id, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        if event_id_text:
            cur.execute(insert_sql.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1), values)
            cur.execute("SELECT id,user_id,sect_id,source,action,stone_delta,exp_delta,"
                        "sect_contribution_delta,sect_scale_delta,sect_materials_delta,"
                        "item_delta,detail,trace_id FROM economy_log WHERE event_id=%s", (event_id_text,))
            row = cur.fetchone()
            expected = values[:12]
            actual = tuple(row[index] for index in range(1, 13)) if row else None
            if actual != expected:
                raise ValueError(f"economy event id payload conflict: {event_id_text}")
        else:
            cur.execute(insert_sql, values)
            cur.execute("SELECT last_insert_rowid()")
            row = cur.fetchone()
        sql_message._commit_write()
        if not row:
            return 0
        return int(row[0])


def safe_log_economy_change(**kwargs: Any) -> int:
    try:
        return log_economy_change(**kwargs)
    except Exception as exc:
        if logger:
            logger.warning(f"记录经济流水失败：{exc}")
        return 0
