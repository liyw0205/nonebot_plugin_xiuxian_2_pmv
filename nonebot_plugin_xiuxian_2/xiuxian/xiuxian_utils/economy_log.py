from __future__ import annotations

from datetime import datetime
from typing import Any

try:
    from nonebot.log import logger
except Exception:  # pragma: no cover
    logger = None

from ...infrastructure.database.economy import (
    ECONOMY_LOG_COLUMNS,
    ECONOMY_LOG_DDL,
    ECONOMY_LOG_INDEXES,
)
from .json_store import safe_json_dumps as _json_dumps
from .xiuxian2_handle import XiuxianDateManage
from ...core.numeric import bind_sqlite_param, require_exact_integer


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


def ensure_economy_log_table() -> bool:
    """Verify the startup-owned canonical ledger; only bootstrap if it is absent.

    `economy_ledger.002` is the owner of the canonical `economy_log` shape and its
    indexes.  The write path therefore performs no DDL once migrations have run;
    the creation branch below remains only as a compatibility fallback for
    databases that never ran the startup migrations.
    """
    sql_message = XiuxianDateManage()
    with sql_message.lock:
        cur = sql_message.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='economy_log'")
        present = cur.fetchone() is not None
        if present:
            columns = {str(row[1]).casefold() for row in cur.execute('PRAGMA table_info("economy_log")')}
            indexes = {
                str(row[0])
                for row in cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='economy_log'"
                )
            }
            if ECONOMY_LOG_COLUMNS.issubset(columns) and set(ECONOMY_LOG_INDEXES).issubset(indexes):
                return True
        for statement in ECONOMY_LOG_DDL:
            cur.execute(statement)
        sql_message._commit_write()
        return True


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
            bind_sqlite_param(require_exact_integer(stone_delta, field="stone_delta", subject=user_id_text or "ledger"), exact=True),
            bind_sqlite_param(require_exact_integer(exp_delta, field="exp_delta", subject=user_id_text or "ledger"), exact=True),
            bind_sqlite_param(require_exact_integer(sect_contribution_delta, field="sect_contribution_delta", subject=user_id_text or "ledger"), exact=True),
            bind_sqlite_param(require_exact_integer(sect_scale_delta, field="sect_scale_delta", subject=user_id_text or "ledger"), exact=True),
            bind_sqlite_param(require_exact_integer(sect_materials_delta, field="sect_materials_delta", subject=user_id_text or "ledger"), exact=True),
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
            if actual is not None:
                actual = actual[:4] + tuple(require_exact_integer(v, field="ledger_delta", subject=user_id_text or "ledger") for v in actual[4:9]) + actual[9:]
                expected = expected[:4] + tuple(require_exact_integer(v, field="ledger_delta", subject=user_id_text or "ledger") for v in expected[4:9]) + expected[9:]
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
