"""Rollback-only, bounded measurement of exact-rank trigger overhead.

The trigger-free control is an isolated experiment, never a gameplay mode.
It must run only after all command requests and their tasks have drained.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import closing
from itertools import product
from pathlib import Path
import sqlite3
import time


def _digest(rows) -> str:
    encoded = json.dumps([list(row) for row in rows], ensure_ascii=True,
                         default=lambda value: {"blob_hex": bytes(value).hex()}).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def measure_rank_generation_cost(database: Path, *, rows: int = 2048, repeats: int = 3) -> dict:
    if not 1 <= rows <= 4096 or not 1 <= repeats <= 3:
        raise ValueError("bulk probe is limited to 4096 rows and 3 repetitions")
    from nonebot_plugin_xiuxian_2.core.numeric import exact_crystal_integer, register_numeric_functions

    with closing(sqlite3.connect(database, timeout=2)) as connection:
        register_numeric_functions(connection)
        if connection.in_transaction:
            raise RuntimeError("bulk probe requires an idle connection")
        trigger_sql = connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
            "AND name IN ('xiuxian_numeric_rank_generation_insert',"
            "'xiuxian_numeric_rank_generation_delete','xiuxian_numeric_rank_generation_update') "
            "ORDER BY name"
        ).fetchall()
        if len(trigger_sql) != 3:
            raise RuntimeError("bulk probe requires all three exact-rank generation triggers")
        original_state = tuple(connection.execute(
            "SELECT * FROM xiuxian_numeric_rank_generation WHERE singleton=1"
        ).fetchone())
        original_rows = connection.execute(
            "SELECT id,exp,stone FROM user_xiuxian ORDER BY id LIMIT ?", (rows,)
        ).fetchall()
        if len(original_rows) != rows:
            raise RuntimeError("bulk probe has fewer synthetic rows than requested")
        selected_ids = [row[0] for row in original_rows]
        placeholders = ",".join("?" for _ in selected_ids)
        projection_sql = f"SELECT id,exp,stone FROM user_xiuxian WHERE id IN ({placeholders}) ORDER BY id"
        original_digest = _digest(original_rows)
        samples = []
        # Rank fixtures intentionally retain ambiguous REAL currency. Seed only
        # this bounded synthetic control inside an outer rollback, never relax
        # the real arithmetic guard or silently benchmark rejected writes.
        connection.execute("SAVEPOINT mixed_bulk_fixture")
        try:
            connection.execute(
                f"UPDATE user_xiuxian SET exp=100000+(id%1024),stone=500000000+(id%2048) "
                f"WHERE id IN ({placeholders})", selected_ids)
            before_digest = _digest(connection.execute(projection_sql, selected_ids).fetchall())
            state_before = tuple(connection.execute(
                "SELECT * FROM xiuxian_numeric_rank_generation WHERE singleton=1"
            ).fetchone())
            for column_set, repetition in product((("stone",), ("exp",), ("exp", "stone")), range(repeats)):
                variants = (True, False) if repetition % 2 == 0 else (False, True)
                for triggers_enabled in variants:
                    connection.execute("SAVEPOINT mixed_bulk_cost")
                    try:
                        if not triggers_enabled:
                            for name, _ in trigger_sql:
                                connection.execute(f'DROP TRIGGER "{name}"')
                        assignments = ",".join(
                            f"{column}=xiuxian_int_store(xiuxian_int_add(COALESCE({column},0),1,'{column}','actor'))"
                            for column in column_set)
                        started = time.perf_counter()
                        updated = connection.execute(
                            f"UPDATE user_xiuxian SET {assignments} WHERE id IN ({placeholders})",
                            selected_ids).rowcount
                        elapsed_ms = (time.perf_counter() - started) * 1000
                        state_after = tuple(connection.execute(
                            "SELECT * FROM xiuxian_numeric_rank_generation WHERE singleton=1"
                        ).fetchone())
                        generation_delta = state_after[1] - state_before[1]
                        if updated != rows or generation_delta != (rows if triggers_enabled else 0):
                            raise AssertionError("bulk control changed unexpected rows/generation")
                        if len(state_after) == 4 and tuple(a - b for a, b in zip(state_after[2:], state_before[2:])) != tuple(
                            rows if triggers_enabled and column in column_set else 0 for column in ("exp", "stone")
                        ):
                            raise AssertionError("bulk control changed incorrect field generations")
                        samples.append({
                            "repetition": repetition, "columns": list(column_set),
                            "triggers_enabled": triggers_enabled, "updated_rows": updated,
                            "update_ms": round(elapsed_ms, 3),
                            "legacy_generation_delta": generation_delta,
                            "field_generation_delta": [a - b for a, b in zip(state_after[2:], state_before[2:])],
                        })
                    finally:
                        connection.execute("ROLLBACK TO mixed_bulk_cost")
                        connection.execute("RELEASE mixed_bulk_cost")
                    if tuple(connection.execute(
                        "SELECT * FROM xiuxian_numeric_rank_generation WHERE singleton=1"
                    ).fetchone()) != state_before:
                        raise AssertionError("bulk probe failed to restore generation state")
                    if connection.execute(
                        "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
                        "AND name IN ('xiuxian_numeric_rank_generation_insert',"
                        "'xiuxian_numeric_rank_generation_delete','xiuxian_numeric_rank_generation_update') "
                        "ORDER BY name"
                    ).fetchall() != trigger_sql:
                        raise AssertionError("bulk probe failed to restore trigger definitions")
            if _digest(connection.execute(projection_sql, selected_ids).fetchall()) != before_digest:
                raise AssertionError("bulk controls failed to restore seeded values")
        finally:
            connection.execute("ROLLBACK TO mixed_bulk_fixture")
            connection.execute("RELEASE mixed_bulk_fixture")
        after_digest = _digest(connection.execute(projection_sql, selected_ids).fetchall())
        if after_digest != original_digest or tuple(connection.execute(
            "SELECT * FROM xiuxian_numeric_rank_generation WHERE singleton=1"
        ).fetchone()) != original_state or connection.in_transaction:
            raise AssertionError("bulk probe failed to restore original score data/generation/idle state")
    summary = {}
    for columns in (("stone",), ("exp",), ("exp", "stone")):
        enabled = sorted(row["update_ms"] for row in samples
                         if row["triggers_enabled"] and row["columns"] == list(columns))
        disabled = sorted(row["update_ms"] for row in samples
                          if not row["triggers_enabled"] and row["columns"] == list(columns))
        enabled_median, disabled_median = enabled[len(enabled) // 2], disabled[len(disabled) // 2]
        summary["+".join(columns)] = {"trigger_enabled_p50_ms": enabled_median,
            "trigger_disabled_control_p50_ms": disabled_median,
            "incremental_p50_ms": round(enabled_median - disabled_median, 3)}
    return {
        "status": "pass",
        "scope": "rollback-only subset; not a measured million-row update_ls_all",
        "rows": rows,
        "repetitions_per_variant": repeats,
        "same_stone_expression_as": "XiuxianDateManage.update_ls_all; selected exact IDs only",
        "selection": {"selected_rows": rows,
                      "original_inexact_rows": sum(any(value is not None and exact_crystal_integer(value) is None
                                                       for value in row[1:]) for row in original_rows),
                      "selected_ids_sha256": _digest([(value,) for value in selected_ids])},
        "seed": "outer-savepoint-only exp=100000+(id%1024), stone=500000000+(id%2048); original values restored",
        "seeded_data_sha256": before_digest,
        "samples": samples,
        "by_columns": summary,
        "selected_data_sha256_before": original_digest,
        "selected_data_sha256_after": after_digest,
        "triggers_and_generation_restored": True,
        "limits": "Existing expression indexes/UDFs retained; control only disables generation triggers inside a rolled-back savepoint.",
    }
