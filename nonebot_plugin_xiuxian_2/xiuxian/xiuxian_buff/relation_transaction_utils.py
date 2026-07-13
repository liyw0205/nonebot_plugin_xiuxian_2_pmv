from __future__ import annotations

import json
from datetime import datetime

from ..xiuxian_utils import db_backend


def ensure_player_field(conn, table: str, field: str, data_type: str = "TEXT") -> None:
    table_sql = db_backend.quote_ident(table)
    field_sql = db_backend.quote_ident(field)
    conn.execute(f"CREATE TABLE IF NOT EXISTS player_data.{table_sql} (user_id TEXT PRIMARY KEY)")
    if field not in {str(row[1]) for row in conn.execute(f"PRAGMA player_data.table_info({table_sql})").fetchall()}:
        conn.execute(f"ALTER TABLE player_data.{table_sql} ADD COLUMN {field_sql} {data_type}")


def get_json_field(conn, table: str, user_id: str, field: str, default):
    ensure_player_field(conn, table, field)
    row = conn.execute(
        f"SELECT {db_backend.quote_ident(field)} FROM player_data.{db_backend.quote_ident(table)} WHERE user_id=%s",
        (str(user_id),),
    ).fetchone()
    if row is None or row[0] in (None, ""):
        return default
    if isinstance(row[0], (dict, list)):
        return row[0]
    try:
        return json.loads(str(row[0]))
    except (TypeError, ValueError):
        return default


def set_field(conn, table: str, user_id: str, field: str, value, data_type: str = "TEXT") -> None:
    ensure_player_field(conn, table, field, data_type)
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    table_sql = db_backend.quote_ident(table)
    field_sql = db_backend.quote_ident(field)
    changed = conn.execute(
        f"UPDATE player_data.{table_sql} SET {field_sql}=%s WHERE user_id=%s",
        (value, str(user_id)),
    )
    if changed.rowcount == 0:
        conn.execute(
            f"INSERT INTO player_data.{table_sql} (user_id,{field_sql}) VALUES (%s,%s)",
            (str(user_id), value),
        )


def increment_stat(conn, user_id: str, key: str, amount: int) -> None:
    ensure_player_field(conn, "statistics", key, "INTEGER")
    table_sql = db_backend.quote_ident("statistics")
    field_sql = db_backend.quote_ident(key)
    changed = conn.execute(
        f"UPDATE player_data.{table_sql} SET {field_sql}=COALESCE({field_sql},0)+%s WHERE user_id=%s",
        (int(amount), str(user_id)),
    )
    if changed.rowcount == 0:
        conn.execute(
            f"INSERT INTO player_data.{table_sql} (user_id,{field_sql}) VALUES (%s,%s)",
            (str(user_id), int(amount)),
        )


def append_mentor_history(conn, user_id: str, event_type: str, related_id: str, description: str, limit: int) -> None:
    history = get_json_field(conn, "mentor", user_id, "mentor_history", [])
    if not isinstance(history, list):
        history = []
    history.append(
        {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": str(event_type),
            "related_id": str(related_id),
            "description": str(description),
        }
    )
    set_field(conn, "mentor", user_id, "mentor_history", history[-int(limit):])
