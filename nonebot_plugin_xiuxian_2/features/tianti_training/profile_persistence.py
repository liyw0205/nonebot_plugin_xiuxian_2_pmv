from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from ...infrastructure.database import DatabaseUnitOfWork


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def upsert_tianti_profile(
    uow: DatabaseUnitOfWork,
    user_id: str,
    fields: Sequence[str],
    data: Mapping[str, Any],
    *,
    database_alias: str = "main",
) -> None:
    if database_alias not in {"main", "player_data"}:
        raise ValueError("unsupported Tianti profile database alias")

    columns = tuple(str(field) for field in fields)
    if not columns or len(columns) != len(set(columns)) or "user_id" in columns:
        raise ValueError("Tianti profile fields must be unique and must not include user_id")
    table = f'{_quote_identifier(database_alias)}.{_quote_identifier("tianti_info")}'
    values = [
        json.dumps(data[field], ensure_ascii=False)
        if isinstance(data[field], (list, dict))
        else data[field]
        for field in columns
    ]
    column_sql = ", ".join(_quote_identifier(field) for field in ("user_id", *columns))
    placeholders = ", ".join("?" for _ in range(len(values) + 1))
    updates = ", ".join(
        f"{_quote_identifier(field)}=excluded.{_quote_identifier(field)}"
        for field in columns
    )
    uow.execute(
        f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT(user_id) DO UPDATE SET {updates}",
        (str(user_id), *values),
    )


__all__ = ["upsert_tianti_profile"]
