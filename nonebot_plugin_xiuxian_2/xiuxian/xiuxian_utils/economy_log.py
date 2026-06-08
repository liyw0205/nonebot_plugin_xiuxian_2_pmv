from __future__ import annotations

import json
from datetime import datetime
from typing import Any

try:
    from nonebot.log import logger
except Exception:  # pragma: no cover
    logger = None

from .xiuxian2_handle import XiuxianDateManage


def _json_dumps(value: Any, default: Any) -> str:
    if value is None:
        value = default
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps(str(value), ensure_ascii=False)


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
                created_at TEXT NOT NULL
            )
            """
        )
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
    created_at: datetime | str | None = None,
) -> int:
    ensure_economy_log_table()
    sql_message = XiuxianDateManage()
    user_id_text = None if user_id is None else str(user_id)
    sect_id_int = None if sect_id in (None, "") else _to_int(sect_id)
    item_delta_text = _json_dumps(item_delta, [])
    detail_text = _json_dumps(detail, {})

    with sql_message.lock:
        cur = sql_message.conn.cursor()
        cur.execute(
            """
            INSERT INTO economy_log (
                user_id, sect_id, source, action,
                stone_delta, exp_delta, sect_contribution_delta,
                sect_scale_delta, sect_materials_delta,
                item_delta, detail, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
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
                _normalize_time(created_at),
            ),
        )
        cur.execute("SELECT last_insert_rowid()")
        row = cur.fetchone()
        sql_message._commit_write()
        return int(row[0]) if row else 0


def safe_log_economy_change(**kwargs: Any) -> int:
    try:
        return log_economy_change(**kwargs)
    except Exception as exc:
        if logger:
            logger.warning(f"记录经济流水失败：{exc}")
        return 0
