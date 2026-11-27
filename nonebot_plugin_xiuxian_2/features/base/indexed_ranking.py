"""Controlled exact-integer indexes for live feature ranking reads."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any

from ...core.numeric import as_int_like
from ...xiuxian.xiuxian_utils.shared_read_cache import process_read_cache, shared_snapshot_key
from .ranking_position import PersonalRank


@dataclass(frozen=True)
class _RankIndex:
    table: str
    field: str
    name: str
    valid_numeric: bool = True
    predicate: str = ""

    @property
    def key(self) -> str:
        value = f'COALESCE("{self.field}",0)'
        key = f"xiuxian_int_sort_key_compact({value})"
        if self.valid_numeric:
            return f"CASE WHEN xiuxian_int_cmp({value},0) IS NOT NULL THEN {key} END"
        return key

    @property
    def create_sql(self) -> str:
        predicate = f" WHERE {self.predicate}" if self.predicate else ""
        return (
            f'CREATE INDEX IF NOT EXISTS "{self.name}" '
            f'ON "{self.table}" ({self.key} DESC){predicate}'
        )


_TRAINING_USER = "user_id IS NOT NULL AND TRIM(CAST(user_id AS TEXT))<>''"
TRAINING_RANK_INDEXES = {
    field: _RankIndex("training", field, f"training_{field}_exact_rank_v1", predicate=_TRAINING_USER)
    for field in ("completed", "points")
}
ARENA_SCORE_RANK_INDEX = _RankIndex("arena", "score", "arena_score_exact_rank_v1")
ARENA_SCORE_ORDER_INDEX = _RankIndex(
    "arena", "score", "arena_score_exact_order_v1", valid_numeric=False
)
_INDEXES = (*TRAINING_RANK_INDEXES.values(), ARENA_SCORE_RANK_INDEX, ARENA_SCORE_ORDER_INDEX)


def _validate(spec: _RankIndex, schema: str) -> None:
    if spec not in _INDEXES or schema not in {"main", "player_data"}:
        raise ValueError("unsupported indexed ranking")


def _normalized_sql(value: str) -> str:
    return " ".join(value.split()).casefold().replace("create index if not exists ", "create index ")


def rank_index_available(
    connection: Any,
    database_path: str | Path,
    spec: _RankIndex,
    *,
    schema: str = "main",
    require_unique_user: bool = False,
) -> bool:
    """Cache only schema evidence, never a player's values or computed position."""
    _validate(spec, schema)
    # Read evidence from this connection, not the pathname: an open reader can
    # still see the previous file after an atomic replacement or DDL rollback.
    definitions = tuple(tuple(row) for row in connection.execute(
        f"SELECT type,name,sql FROM {schema}.sqlite_master "
        "WHERE (type='table' AND name=?) OR (type='index' AND tbl_name=?) "
        "ORDER BY type,name LIMIT 65", (spec.table, spec.table),
    ).fetchall())
    encoded = repr(definitions).encode("utf-8")
    if len(definitions) > 64 or len(encoded) > 65536:
        return False
    key = shared_snapshot_key(
        database_path, "exact-ranking-index-schema-v1", schema, spec.name,
        hashlib.sha256(encoded).hexdigest(), require_unique_user,
    )

    def inspect() -> bool:
        table = next((row for row in definitions if row[:2] == ("table", spec.table)), None)
        if not table or not table[2]:
            return False
        table_sql = " ".join(str(table[2]).split()).casefold()
        if "without rowid" in table_sql or "create virtual" in table_sql:
            return False
        columns = connection.execute(f'PRAGMA {schema}.table_info("{spec.table}")').fetchall()
        if any(str(row[1]).casefold() in {"rowid", "_rowid_", "oid"} for row in columns):
            return False
        indexes = connection.execute(f'PRAGMA {schema}.index_list("{spec.table}")').fetchall()
        matching = next((row for row in indexes if str(row[1]) == spec.name), None)
        if matching is None or bool(matching[4]) != bool(spec.predicate):
            return False
        definition = next((row for row in definitions if row[:2] == ("index", spec.name)), None)
        if not definition or _normalized_sql(str(definition[2])) != _normalized_sql(spec.create_sql):
            return False
        parts = connection.execute(f'PRAGMA {schema}.index_xinfo("{spec.name}")').fetchall()
        indexed = [row for row in parts if row[5]]
        if len(indexed) != 1 or tuple(indexed[0][1:5]) != (-2, None, 1, "BINARY"):
            return False
        if not require_unique_user:
            return True
        # A collation on the table may give the legacy equality test different
        # identity semantics from an otherwise unique BINARY index.
        if "collate" in table_sql:
            return False
        for index in indexes:
            if not index[2] or index[4]:
                continue
            name = str(index[1]).replace('"', '""')
            parts = connection.execute(f'PRAGMA {schema}.index_xinfo("{name}")').fetchall()
            indexed = [row for row in parts if row[5]]
            if len(indexed) == 1 and str(indexed[0][2]).casefold() == "user_id" and indexed[0][4] == "BINARY":
                return True
        return False

    return process_read_cache.get_or_load(
        key, database_path=database_path, ttl=60 * 60, snapshot=True, loader=inspect,
    )


def indexed_personal_rank(
    connection: Any, spec: _RankIndex, user_id: str, *, schema: str = "main"
) -> PersonalRank:
    """Read one target and count two exact index ranges in the same transaction."""
    _validate(spec, schema)
    if not spec.valid_numeric:
        raise ValueError("personal rank requires a validated numeric key")
    table = f'{schema}."{spec.table}"'
    predicate = f"{spec.predicate} AND " if spec.predicate else ""
    row = connection.query_one(
        f'WITH target AS (SELECT COALESCE("{spec.field}",0) AS value,{spec.key} AS rank_key '
        f"FROM {table} WHERE {predicate}user_id=? ORDER BY rowid ASC LIMIT 1) "
        "SELECT COALESCE((SELECT value FROM target),0) AS value,"
        f"(SELECT COUNT(*) FROM {table} WHERE {predicate}{spec.key}>xiuxian_int_sort_key_compact(0)) AS total,"
        f"(SELECT COUNT(*) FROM {table} WHERE {predicate}{spec.key}>(SELECT rank_key FROM target)) AS better",
        (user_id,),
    ) or {}
    value = max(0, as_int_like(row.get("value")))
    total = max(0, int(row.get("total") or 0))
    better = max(0, int(row.get("better") or 0))
    return PersonalRank(better + 1 if value > 0 else 0, total, value)


__all__ = [
    "ARENA_SCORE_ORDER_INDEX", "ARENA_SCORE_RANK_INDEX", "TRAINING_RANK_INDEXES",
    "indexed_personal_rank", "rank_index_available",
]
