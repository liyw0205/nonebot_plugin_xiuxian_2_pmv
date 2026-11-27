"""Drained rollback-only owner-index size and bounded maintenance control."""
from contextlib import closing
from pathlib import Path
import sqlite3
import time

INDEX = "idx_user_xiuxian_user_id_numeric_v1"


def measure_owner_index_cost(database: Path) -> dict:
    from nonebot_plugin_xiuxian_2.core.numeric import register_numeric_functions

    with closing(sqlite3.connect(database, timeout=2)) as connection:
        register_numeric_functions(connection)
        definition = connection.execute("SELECT sql FROM sqlite_master WHERE name=?", (INDEX,)).fetchone()
        size = connection.execute("SELECT COUNT(*),COALESCE(SUM(pgsize),0) FROM dbstat WHERE name=?",
                                  (INDEX,)).fetchone()
        rows = connection.execute("SELECT id,user_id,exp,stone FROM user_xiuxian ORDER BY id LIMIT 32").fetchall()
        if len(rows) != 32:
            raise ValueError("owner cost control requires32 existing synthetic rows")
        before_generation = connection.execute("SELECT * FROM xiuxian_numeric_rank_generation").fetchall()
        samples = []
        connection.execute("SAVEPOINT owner_cost_fixture")
        try:
            connection.execute("UPDATE user_xiuxian SET exp=100000,stone=500000000 WHERE id<=?", (rows[-1][0],))
            for operation, assignments in (("exp_stone", "exp=exp+1,stone=stone+1"),
                                            ("identity_same_value", "user_id=user_id")):
                for repetition in range(3):
                    variants = (True, False) if definition else (False,)
                    if repetition % 2:
                        variants = tuple(reversed(variants))
                    for indexed in variants:
                        connection.execute("SAVEPOINT owner_cost_sample")
                        try:
                            if definition and not indexed:
                                connection.execute("DROP INDEX " + INDEX)
                            started = time.perf_counter()
                            count = connection.execute(
                                f"UPDATE user_xiuxian SET {assignments} WHERE id<=?", (rows[-1][0],)).rowcount
                            elapsed_ms = (time.perf_counter() - started) * 1000
                            if count != 32:
                                raise AssertionError("unexpected maintenance control population")
                            samples.append({"operation": operation, "repetition": repetition,
                                            "indexed": indexed, "rows": count, "update_ms": elapsed_ms})
                        finally:
                            connection.execute("ROLLBACK TO owner_cost_sample")
                            connection.execute("RELEASE owner_cost_sample")
        finally:
            connection.execute("ROLLBACK TO owner_cost_fixture")
            connection.execute("RELEASE owner_cost_fixture")
        if (connection.execute("SELECT id,user_id,exp,stone FROM user_xiuxian ORDER BY id LIMIT 32").fetchall() != rows
                or connection.execute("SELECT * FROM xiuxian_numeric_rank_generation").fetchall() != before_generation
                or connection.execute("SELECT sql FROM sqlite_master WHERE name=?", (INDEX,)).fetchone() != definition
                or connection.in_transaction):
            raise AssertionError("owner cost probe did not restore rows/generation/index/idle state")
    return {"status": "pass", "index_present": bool(definition), "index_pages": size[0],
            "index_bytes": size[1], "samples": samples, "rollback_verified": True,
            "scope": "32-row uncommitted control; excludes drop/rollback time, not gameplay throughput or commit cost"}
