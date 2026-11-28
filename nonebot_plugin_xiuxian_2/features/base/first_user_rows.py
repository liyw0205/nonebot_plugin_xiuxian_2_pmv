"""Fixed-table first-record projections for legacy ranking identities."""
from __future__ import annotations


def first_user_rows(table: str, *, schema: str = "main") -> str:
    """Group original identities before filtering out blank or NULL players."""
    tables = {"training": "training", "boss_limit": "boss_limit"}
    if table not in tables or schema not in {"main", "player_data"}:
        raise ValueError("unsupported first-user ranking table")
    return (
        f"SELECT user_id,MIN(rowid) AS first_rowid FROM {schema}.{tables[table]} "
        "GROUP BY user_id"
    )


__all__ = ["first_user_rows"]
