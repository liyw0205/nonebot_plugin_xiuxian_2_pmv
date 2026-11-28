"""Bounded inventory projections within the caller's current transaction."""
from __future__ import annotations

from typing import Any, Iterable

from .uow import DatabaseUnitOfWork


def read_inventory_rows(
    uow: DatabaseUnitOfWork,
    user_id: str,
    item_ids: Iterable[int | str],
    *,
    projection: str = "counts",
    has_bind_num: bool = True,
) -> list[dict[str, Any] | None]:
    """Return one point-read result per requested ID, including missing rows.

    Keep SQLite's comparison affinity and first matching legacy row rather
    than collapsing duplicate rows or numeric aliases into a Python ID map.
    Each chunk still performs per-ID lookups inside one SQLite statement.
    """
    bound = "b.bind_num" if has_bind_num else "0 AS bind_num"
    fields = {
        "quantity": "b.goods_num",
        "counts": f"b.goods_num,{bound}",
        "details": f"b.goods_name,b.goods_type,b.goods_num,{bound}",
    }
    if projection not in fields:
        raise ValueError("unsupported inventory projection")
    requested = list(item_ids)
    if len(requested) == 1:
        row = uow.query_one(
            f"SELECT {fields[projection]} FROM back AS b WHERE user_id=? AND goods_id=?",
            (user_id, requested[0]),
        )
        return [dict(row) if row is not None else None]
    result: list[dict[str, Any] | None] = []
    # Two binds per ID plus one user bind stay below SQLite's legacy 999 cap.
    for start in range(0, len(requested), 400):
        chunk = requested[start:start + 400]
        values = ",".join("(?,?)" for _ in chunk)
        params = tuple(value for pair in enumerate(chunk) for value in pair) + (user_id,)
        rows = uow.query_all(
            f"SELECT b.rowid AS _found,{fields[projection]} FROM "
            f"(SELECT column1 AS ordinal,column2 AS goods_id FROM (VALUES {values})) AS wanted "
            "LEFT JOIN back AS b ON b.rowid=(SELECT rowid FROM back "
            "WHERE user_id=? AND goods_id=wanted.goods_id LIMIT 1) "
            "ORDER BY wanted.ordinal",
            params,
        )
        for row in rows:
            state = dict(row)
            found = state.pop("_found")
            result.append(state if found is not None else None)
    return result
