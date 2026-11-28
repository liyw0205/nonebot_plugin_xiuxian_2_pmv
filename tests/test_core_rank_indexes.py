from __future__ import annotations

import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.db_migrations import (
    _create_numeric_rank_generation,
    run_core_migrations,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage


def _is_rank_computation(sql):
    return "COUNT(DISTINCT" in sql or sql.startswith("WITH RECURSIVE numeric_rank_keys")


class CoreRankIndexTests(unittest.TestCase):
    def setUp(self):
        process_read_cache.clear()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "game.db"

        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(id INTEGER PRIMARY KEY, user_id TEXT, user_name TEXT, level TEXT, "
                "exp TEXT, stone TEXT, power TEXT, root_level INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian(user_id, exp, stone) VALUES(%s, %s, %s)",
                (
                    ("leader", "100000000000000000002", "900"),
                    ("tie-a", "1e3", "100000000000000000001"),
                    ("tie-b", "1000", "100000000000000000001"),
                    ("viewer", "900", "1e3"),
                    ("legacy-invalid", "not-an-integer", "not-an-integer"),
                ),
            )

    def _plan(self, sql: str, user_id: str):
        with db_backend.connection(self.database) as conn:
            return [
                str(row[3])
                for row in conn.execute("EXPLAIN QUERY PLAN " + sql, (user_id,)).fetchall()
            ]

    def test_startup_migration_adds_numeric_rank_indexes_and_query_uses_them(self):
        self.assertEqual(
            [
                "20260707_0001_runtime_marker",
                "20260717_0001_dedupe_user_xiuxian",
                "20261125_0001_compact_numeric_rank_keys",
                "20261125_0002_realm_rank_index",
                "20261125_0003_public_rank_indexes",
                "20261126_0001_exact_numeric_rank_generation",
                "20261126_0002_independent_numeric_rank_generation",
            ],
            run_core_migrations(self.database),
        )
        self.assertEqual([], run_core_migrations(self.database))

        exp_sql = (
            "SELECT rank_value FROM ("
            "SELECT user_id, dense_rank() OVER ("
            "ORDER BY xiuxian_int_text(exp) COLLATE xiuxian_integer DESC"
            ") AS rank_value FROM user_xiuxian"
            ") AS ranked_users WHERE user_id=%s"
        )
        stone_sql = exp_sql.replace("exp", "stone")
        exp_plan = self._plan(exp_sql, "viewer")
        stone_plan = self._plan(stone_sql, "viewer")
        self.assertTrue(any("idx_user_xiuxian_exp_numeric_rank" in item for item in exp_plan))
        self.assertTrue(any("idx_user_xiuxian_stone_numeric_rank" in item for item in stone_plan))
        self.assertFalse(any("USE TEMP B-TREE FOR ORDER BY" in item for item in exp_plan))
        self.assertFalse(any("USE TEMP B-TREE FOR ORDER BY" in item for item in stone_plan))
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO user_xiuxian(user_id,user_name,level,exp,stone) "
                "VALUES(%s,%s,%s,%s,%s)",
                ("rank-plan", "排行计划", "江湖好手", "1", "1"),
            )
        with db_backend.connection(self.database) as conn:
            stone_top_plan = [
                str(row[3])
                for row in conn.execute(
                    "EXPLAIN QUERY PLAN SELECT user_name,stone FROM user_xiuxian "
                    "WHERE user_name IS NOT NULL ORDER BY "
                    'xiuxian_int_sort_key_compact("stone") DESC LIMIT 50'
                ).fetchall()
            ]
        self.assertTrue(
            any("idx_user_xiuxian_stone_rank_key_v2" in item for item in stone_top_plan)
        )

        key_rank_sql = (
            "WITH target AS (SELECT xiuxian_int_sort_key_compact(exp) AS value "
            "FROM user_xiuxian WHERE user_id=%s) "
            "SELECT 1 + COUNT(DISTINCT xiuxian_int_sort_key_compact(exp)) "
            "FROM user_xiuxian, target "
            "WHERE xiuxian_int_sort_key_compact(exp) > target.value"
        )
        key_plan = self._plan(key_rank_sql, "viewer")
        self.assertTrue(any("idx_user_xiuxian_exp_rank_key_v2" in item for item in key_plan))
        stone_key_plan = self._plan(key_rank_sql.replace("exp", "stone"), "viewer")
        self.assertTrue(
            any("idx_user_xiuxian_stone_rank_key_v2" in item for item in stone_key_plan)
        )

        with db_backend.connection(self.database) as conn:
            handle = type("RankHandle", (), {})()
            rank_reads = []

            def read_query(sql, params=None, **kwargs):
                rank_reads.append((sql, params, kwargs))
                return tuple(conn.execute(sql, params or ()).fetchone())

            handle._read_query = read_query
            self.assertEqual((2,), XiuxianDateManage.get_exp_rank(handle, "tie-a"))
            self.assertEqual((1,), XiuxianDateManage.get_stone_rank(handle, "tie-a"))
            reads_before = len(rank_reads)
            self.assertEqual((2, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(1, len(rank_reads) - reads_before)
            combined_plan = [
                str(row[3])
                for row in conn.execute(
                    "EXPLAIN QUERY PLAN " + rank_reads[-1][0],
                    rank_reads[-1][1],
                ).fetchall()
            ]
            self.assertTrue(any("idx_user_xiuxian_exp_rank_key_v2" in item for item in combined_plan))
            self.assertTrue(any("idx_user_xiuxian_stone_rank_key_v2" in item for item in combined_plan))

    def test_compact_key_migration_rebuilds_existing_rank_indexes(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE INDEX idx_user_xiuxian_exp_rank_key ON user_xiuxian(user_id)"
            )
            conn.execute(
                "CREATE INDEX idx_user_xiuxian_stone_rank_key ON user_xiuxian(user_id)"
            )

        run_core_migrations(self.database)

        with db_backend.connection(self.database) as conn:
            index_sql = {
                row["name"]: row["sql"]
                for row in conn.execute(
                    "SELECT name, sql FROM sqlite_master WHERE type='index'"
                ).fetchall()
            }
            self.assertIn(
                'xiuxian_int_sort_key_compact("exp")',
                index_sql["idx_user_xiuxian_exp_rank_key_v2"],
            )
            self.assertIn(
                'xiuxian_int_sort_key_compact("stone")',
                index_sql["idx_user_xiuxian_stone_rank_key_v2"],
            )
            self.assertNotIn("idx_user_xiuxian_exp_rank_key", index_sql)
            self.assertNotIn("idx_user_xiuxian_stone_rank_key", index_sql)

    def test_numeric_rank_range_query_preserves_ties_and_nulls(self):
        run_core_migrations(self.database)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO user_xiuxian(user_id, exp, stone) VALUES(%s, %s, %s)",
                ("null-exp", None, "0"),
            )
            handle = type("RankHandle", (), {})()
            handle._read_query = lambda sql, params=None, **kwargs: tuple(
                conn.execute(sql, params or ()).fetchone()
            )
            self.assertEqual((1,), XiuxianDateManage.get_exp_rank(handle, "leader"))
            self.assertEqual((2,), XiuxianDateManage.get_exp_rank(handle, "tie-a"))
            self.assertEqual((3,), XiuxianDateManage.get_exp_rank(handle, "viewer"))
            self.assertEqual((5,), XiuxianDateManage.get_exp_rank(handle, "null-exp"))
            self.assertEqual((5, 4), XiuxianDateManage.get_exp_and_stone_ranks(handle, "null-exp"))

            plan = [
                str(row[3])
                for row in conn.execute(
                    "EXPLAIN QUERY PLAN "
                    "WITH target AS (SELECT xiuxian_int_sort_key_compact(\"exp\") AS value "
                    "FROM user_xiuxian WHERE user_id=%s) "
                    "SELECT 1 + COUNT(DISTINCT xiuxian_int_sort_key_compact(\"exp\")) "
                    "FROM user_xiuxian, target "
                    "WHERE xiuxian_int_sort_key_compact(\"exp\") > target.value",
                    ("viewer",),
                ).fetchall()
            ]
        self.assertTrue(any("idx_user_xiuxian_exp_rank_key_v2" in item for item in plan))

    def test_exact_personal_rank_cache_tracks_cross_connection_score_changes(self):
        run_core_migrations(self.database)
        with db_backend.connection(self.database) as conn:
            handle = type("RankHandle", (), {})()
            handle.database_path = self.database
            rank_reads = []
            generation_reads = []

            def trace_query(sql):
                if _is_rank_computation(sql):
                    rank_reads.append(sql)
                if "xiuxian_numeric_rank_generation" in sql:
                    generation_reads.append(sql)

            @contextmanager
            def read_connection():
                yield conn

            conn._raw.set_trace_callback(trace_query)
            handle._repository_read_connection = read_connection

            self.assertEqual((2, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual((2, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(2, len(rank_reads))
            self.assertEqual(2, process_read_cache.stats()["hits"])
            self.assertEqual(2, len(generation_reads))
            self.assertTrue(all("COUNT(DISTINCT" not in sql for sql in generation_reads))
            self.assertTrue(all("target.user_id=" in sql for sql in generation_reads))

            # The profile command writes an unrelated last-viewed timestamp;
            # that must not evict an exact rank keyed by the DB score generation.
            process_read_cache.invalidate_database(self.database)
            self.assertEqual((2, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(2, len(rank_reads))
            self.assertEqual(4, process_read_cache.stats()["hits"])
            self.assertEqual(3, len(generation_reads))

            with db_backend.transaction(self.database) as writer:
                writer.execute(
                    "UPDATE user_xiuxian SET exp=%s WHERE user_id=%s", ("2000", "viewer")
                )
            self.assertEqual((3, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(3, len(rank_reads))
            self.assertEqual(2, sum('("exp")' in sql for sql in rank_reads))
            self.assertEqual(1, sum('("stone")' in sql for sql in rank_reads))

            with db_backend.transaction(self.database) as writer:
                writer.execute(
                    "UPDATE user_xiuxian SET user_id=%s WHERE user_id=%s",
                    ("viewer-renamed", "viewer"),
                )
            self.assertIsNone(
                XiuxianDateManage.get_exp_and_stone_ranks(handle, "viewer")
            )
            self.assertEqual(3, len(rank_reads))
            self.assertEqual(
                (2, 2),
                XiuxianDateManage.get_exp_and_stone_ranks(handle, "viewer-renamed"),
            )
            self.assertEqual(5, len(rank_reads))
            self.assertEqual((3, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(5, len(rank_reads))

            with db_backend.transaction(self.database) as writer:
                writer.execute(
                    "UPDATE user_xiuxian SET user_name=%s WHERE user_id=%s",
                    ("名字变化", "viewer-renamed"),
                )
            self.assertEqual((3, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(5, len(rank_reads))

            with db_backend.transaction(self.database) as writer:
                writer.execute(
                    "INSERT INTO user_xiuxian(user_id,exp,stone) VALUES(%s,%s,%s)",
                    ("new-leader", "3000", "0"),
            )
            self.assertEqual((4, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(7, len(rank_reads))

            with db_backend.transaction(self.database) as writer:
                writer.execute("DELETE FROM user_xiuxian WHERE user_id=%s", ("new-leader",))
            self.assertEqual((3, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(9, len(rank_reads))

    def test_exact_rank_cache_shares_canonical_scores_between_viewers(self):
        run_core_migrations(self.database)
        with db_backend.connection(self.database) as conn:
            rank_reads = []

            @contextmanager
            def read_connection():
                yield conn

            conn._raw.set_trace_callback(
                lambda sql: rank_reads.append(sql) if _is_rank_computation(sql) else None
            )
            handle = SimpleNamespace(
                database_path=self.database,
                _repository_read_connection=read_connection,
            )
            self.assertEqual((2, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            # Scientific notation and decimal text have the same numeric key.
            self.assertEqual((2, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-b"))
            self.assertEqual(2, len(rank_reads))
            self.assertEqual(2, process_read_cache.stats()["hits"])
            self.assertEqual(2, process_read_cache.stats()["misses"])

            with db_backend.transaction(self.database) as writer:
                writer.execute(
                    "INSERT INTO user_xiuxian(user_id,exp,stone) VALUES(%s,%s,%s)",
                    ("null-exp", None, "0"),
                )
            self.assertEqual((5, 4), XiuxianDateManage.get_exp_and_stone_ranks(handle, "null-exp"))
            self.assertEqual((5, 4), XiuxianDateManage.get_exp_and_stone_ranks(handle, "null-exp"))
            self.assertEqual(4, len(rank_reads))
            self.assertFalse(conn.in_transaction)

    def test_independent_rank_generation_upgrades_legacy_state_without_resetting_it(self):
        with db_backend.transaction(self.database) as conn:
            _create_numeric_rank_generation(conn)
            conn.execute("UPDATE user_xiuxian SET exp=%s WHERE user_id=%s", ("901", "viewer"))
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT generation FROM xiuxian_numeric_rank_generation WHERE singleton=1"
                ).fetchone()[0],
            )

        self.assertIn(
            "20261126_0002_independent_numeric_rank_generation",
            run_core_migrations(self.database),
        )
        self.assertEqual([], run_core_migrations(self.database))
        with db_backend.transaction(self.database) as conn:
            state_sql = (
                "SELECT generation,exp_generation,stone_generation "
                "FROM xiuxian_numeric_rank_generation WHERE singleton=1"
            )
            self.assertEqual((1, 0, 0), tuple(conn.execute(state_sql).fetchone()))
            conn.execute(
                "UPDATE user_xiuxian SET user_id=%s WHERE user_id=%s",
                ("viewer-renamed", "viewer"),
            )
            self.assertEqual((2, 0, 0), tuple(conn.execute(state_sql).fetchone()))
            conn.execute(
                "UPDATE user_xiuxian SET exp=%s WHERE user_id=%s",
                ("902", "viewer-renamed"),
            )
            self.assertEqual((3, 1, 0), tuple(conn.execute(state_sql).fetchone()))
            conn.execute(
                "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s",
                ("1001", "viewer-renamed"),
            )
            self.assertEqual((4, 1, 1), tuple(conn.execute(state_sql).fetchone()))
            conn.execute(
                "UPDATE user_xiuxian SET stone=stone,exp=exp WHERE user_id=%s",
                ("viewer-renamed",),
            )
            self.assertEqual((4, 1, 1), tuple(conn.execute(state_sql).fetchone()))
            conn.execute(
                "INSERT INTO user_xiuxian(user_id,exp,stone) VALUES(%s,%s,%s)",
                ("new-user", "0", "0"),
            )
            self.assertEqual((5, 2, 2), tuple(conn.execute(state_sql).fetchone()))
            conn.execute("DELETE FROM user_xiuxian WHERE user_id=%s", ("new-user",))
            self.assertEqual((6, 3, 3), tuple(conn.execute(state_sql).fetchone()))
            row_count = conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0]
            conn.execute("UPDATE user_xiuxian SET stone=%s", ("777",))
            self.assertEqual(
                (6 + row_count, 3, 3 + row_count),
                tuple(conn.execute(state_sql).fetchone()),
            )
            self.assertEqual(
                3,
                conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' "
                    "AND tbl_name='user_xiuxian'"
                ).fetchone()[0],
            )

    def test_exact_personal_rank_without_generation_table_stays_live_and_uncached(self):
        with db_backend.connection(self.database) as conn:
            rank_reads = []

            @contextmanager
            def read_connection():
                yield conn

            def trace_query(sql):
                if _is_rank_computation(sql):
                    rank_reads.append(sql)

            conn._raw.set_trace_callback(trace_query)
            handle = SimpleNamespace(
                database_path=self.database,
                _repository_read_connection=read_connection,
            )
            self.assertFalse(conn.table_exists("xiuxian_numeric_rank_generation"))
            self.assertEqual(
                (2, 1), tuple(XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            )
            self.assertFalse(conn.in_transaction)

            with db_backend.transaction(self.database) as writer:
                writer.execute(
                    "UPDATE user_xiuxian SET exp=%s,stone=%s WHERE user_id=%s",
                    ("2000", "200000000000000000000", "viewer"),
                )
            self.assertEqual(
                (3, 2), tuple(XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            )
            self.assertEqual(
                (3, 2), tuple(XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            )
            self.assertEqual(3, len(rank_reads))
            self.assertEqual(0, process_read_cache.stats()["hits"])
            self.assertEqual(0, process_read_cache.stats()["misses"])
            self.assertFalse(conn.in_transaction)

    def test_exact_rank_state_lookup_does_not_hide_unrelated_database_errors(self):
        run_core_migrations(self.database)
        with db_backend.connection(self.database) as conn:
            original_execute = conn.execute

            @contextmanager
            def read_connection():
                yield conn

            def execute(sql, params=None):
                if sql.startswith("SELECT rank_state.exp_generation"):
                    raise db_backend.OperationalError("no such column: target.unrelated")
                return original_execute(sql, params)

            conn.execute = execute
            handle = SimpleNamespace(
                database_path=self.database,
                _repository_read_connection=read_connection,
            )
            with self.assertRaisesRegex(db_backend.OperationalError, "target.unrelated"):
                XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a")
            self.assertFalse(conn.in_transaction)
            self.assertEqual(0, process_read_cache.stats()["misses"])

    def test_exact_rank_legacy_generation_schema_stays_live_and_uncached(self):
        with db_backend.transaction(self.database) as writer:
            _create_numeric_rank_generation(writer)
        with db_backend.connection(self.database) as conn:
            rank_reads = []

            @contextmanager
            def read_connection():
                yield conn

            conn._raw.set_trace_callback(
                lambda sql: rank_reads.append(sql) if _is_rank_computation(sql) else None
            )
            handle = SimpleNamespace(
                database_path=self.database,
                _repository_read_connection=read_connection,
            )
            self.assertEqual(
                (2, 1), tuple(XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            )
            with db_backend.transaction(self.database) as writer:
                writer.execute(
                    "UPDATE user_xiuxian SET exp=%s,stone=%s WHERE user_id=%s",
                    ("2000", "200000000000000000000", "viewer"),
                )
            self.assertEqual(
                (3, 2), tuple(XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            )
            self.assertEqual(
                (3, 2), tuple(XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            )
            self.assertEqual(3, len(rank_reads))
            self.assertEqual(0, process_read_cache.stats()["hits"])
            self.assertEqual(0, process_read_cache.stats()["misses"])
            self.assertFalse(conn.in_transaction)

    def test_exact_personal_rank_generation_and_values_share_wal_snapshot(self):
        run_core_migrations(self.database)
        with db_backend.connection(self.database) as conn:
            self.assertEqual("wal", conn.execute("PRAGMA journal_mode").fetchone()[0])
            rank_reads = []
            generations = []
            writer_committed = False
            original_execute = conn.execute

            @contextmanager
            def read_connection():
                yield conn

            def execute(sql, params=None):
                cursor = original_execute(sql, params)
                if not sql.startswith("SELECT rank_state.exp_generation"):
                    return cursor

                def fetch_generation():
                    nonlocal writer_committed
                    row = cursor.fetchone()
                    generations.append((row[0], row[1]))
                    if not writer_committed:
                        # Commit after pinning the generation read snapshot,
                        # before the cold rank query reads either score index.
                        with db_backend.transaction(self.database) as writer:
                            writer.execute(
                                "UPDATE user_xiuxian SET exp=%s,stone=%s WHERE user_id=%s",
                                ("2000", "200000000000000000000", "viewer"),
                            )
                        writer_committed = True
                    return row

                return SimpleNamespace(fetchone=fetch_generation)

            def trace_query(sql):
                if _is_rank_computation(sql):
                    rank_reads.append(sql)

            conn.execute = execute
            conn._raw.set_trace_callback(trace_query)
            handle = SimpleNamespace(
                database_path=self.database,
                _repository_read_connection=read_connection,
            )

            self.assertEqual((2, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertTrue(writer_committed)
            self.assertFalse(conn.in_transaction)
            self.assertEqual((3, 2), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual((3, 2), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(4, len(rank_reads))
            updated_generations = tuple(value + 1 for value in generations[0])
            self.assertEqual(
                [generations[0], updated_generations, updated_generations], generations
            )
            self.assertEqual(4, process_read_cache.stats()["misses"])
            self.assertEqual(2, process_read_cache.stats()["hits"])
            self.assertFalse(conn.in_transaction)

    def test_exact_personal_rank_stone_changes_commit_and_rollback_atomically(self):
        run_core_migrations(self.database)
        with db_backend.connection(self.database) as conn:
            rank_reads = []

            @contextmanager
            def read_connection():
                yield conn

            def trace_query(sql):
                if _is_rank_computation(sql):
                    rank_reads.append(sql)

            conn._raw.set_trace_callback(trace_query)
            handle = SimpleNamespace(
                database_path=self.database,
                _repository_read_connection=read_connection,
            )
            self.assertEqual((2, 1), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            with db_backend.transaction(self.database) as writer:
                writer.execute(
                    "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s",
                    ("200000000000000000000", "viewer"),
                )
            self.assertEqual((2, 2), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(3, len(rank_reads))
            self.assertEqual(1, sum('("exp")' in sql for sql in rank_reads))
            self.assertEqual(2, sum('("stone")' in sql for sql in rank_reads))

            with db_backend.connection(self.database) as writer:
                writer.execute("BEGIN")
                generation = writer.execute(
                    "SELECT generation FROM xiuxian_numeric_rank_generation WHERE singleton=1"
                ).fetchone()[0]
                writer.execute(
                    "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s",
                    ("300000000000000000000", "tie-b"),
                )
                self.assertEqual(
                    generation + 1,
                    writer.execute(
                        "SELECT generation FROM xiuxian_numeric_rank_generation WHERE singleton=1"
                    ).fetchone()[0],
                )
                self.assertEqual((2, 2), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
                writer.rollback()
                self.assertEqual(
                    generation,
                    writer.execute(
                        "SELECT generation FROM xiuxian_numeric_rank_generation WHERE singleton=1"
                    ).fetchone()[0],
                )
            self.assertEqual((2, 2), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(3, len(rank_reads))

            with db_backend.transaction(self.database) as writer:
                writer.execute(
                    "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s",
                    ("300000000000000000000", "tie-b"),
                )
            self.assertEqual((2, 3), XiuxianDateManage.get_exp_and_stone_ranks(handle, "tie-a"))
            self.assertEqual(4, len(rank_reads))
            self.assertEqual(1, sum('("exp")' in sql for sql in rank_reads))
            self.assertEqual(3, sum('("stone")' in sql for sql in rank_reads))
            self.assertEqual(4, process_read_cache.stats()["misses"])
            self.assertEqual(6, process_read_cache.stats()["hits"])
            self.assertFalse(conn.in_transaction)

    def test_numeric_rank_generation_migration_skips_incomplete_legacy_schema(self):
        for missing in ("user_id", "exp", "stone"):
            with self.subTest(missing=missing):
                database = Path(self.temp.name) / f"missing-{missing}.db"
                columns = [
                    f"{name} TEXT"
                    for name in ("user_id", "exp", "stone")
                    if name != missing
                ]
                with db_backend.transaction(database) as conn:
                    conn.execute(
                        "CREATE TABLE user_xiuxian (id INTEGER PRIMARY KEY, "
                        + ", ".join(columns)
                        + ")"
                    )
                    _create_numeric_rank_generation(conn)
                    self.assertFalse(conn.table_exists("xiuxian_numeric_rank_generation"))
                    self.assertEqual(
                        [],
                        conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='trigger' "
                            "AND tbl_name='user_xiuxian'"
                        ).fetchall(),
                    )

                    conn.execute(f"ALTER TABLE user_xiuxian ADD COLUMN {missing} TEXT")
                    _create_numeric_rank_generation(conn)
                    self.assertEqual(
                        3,
                        conn.execute(
                            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' "
                            "AND tbl_name='user_xiuxian'"
                        ).fetchone()[0],
                    )
                    conn.execute(
                        "INSERT INTO user_xiuxian(user_id,exp,stone) VALUES(%s,%s,%s)",
                        ("new-user", "100", "10"),
                    )
                    self.assertEqual(
                        1,
                        conn.execute(
                            "SELECT generation FROM xiuxian_numeric_rank_generation WHERE singleton=1"
                        ).fetchone()[0],
                    )

    def test_legacy_non_numeric_text_does_not_block_index_migration(self):
        run_core_migrations(self.database)
        with db_backend.connection(self.database) as conn:
            indexes = {
                row[1]
                for row in conn.execute(
                    "SELECT type, name FROM sqlite_master "
                    "WHERE type='index' AND tbl_name='user_xiuxian'"
                ).fetchall()
            }
        self.assertIn("idx_user_xiuxian_exp_numeric_rank", indexes)
        self.assertIn("idx_user_xiuxian_stone_numeric_rank", indexes)
        self.assertIn("idx_user_xiuxian_exp_rank_key_v2", indexes)
        self.assertIn("idx_user_xiuxian_stone_rank_key_v2", indexes)
        self.assertIn("idx_user_xiuxian_realm_rank_v1", indexes)
        self.assertIn("idx_user_xiuxian_power_rank_key_v1", indexes)
        self.assertIn("idx_user_xiuxian_root_rank_v1", indexes)

    def test_realm_leaderboard_uses_composite_index_and_keeps_tie_order(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.db_migrations import (
            realm_level_order_expression,
        )

        run_core_migrations(self.database)
        with db_backend.transaction(self.database) as conn:
            conn.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name,level,exp,stone) "
                "VALUES(%s,%s,%s,%s,%s)",
                (
                    ("realm-low", "低境界", "江湖好手", "1000", "0"),
                    ("realm-high", "高境界", "至高", "1000", "0"),
                    ("realm-exp", "高修为", "至高", "1001", "0"),
                ),
            )
            handle = type("RankHandle", (), {})()
            handle.database_path = self.database
            handle._conn_lock = threading.RLock()
            handle.conn = db_backend.connect(self.database)
            self.addCleanup(handle.conn.close)
            handle._read_query = lambda sql, params=None, **kwargs: conn.execute(
                sql, params or ()
            ).fetchall()
            rows = XiuxianDateManage.realm_top(handle)
            self.assertEqual(
                [
                    ("高修为", "至高", "1001"),
                    ("低境界", "江湖好手", "1000"),
                    ("高境界", "至高", "1000"),
                ],
                [tuple(row) for row in rows],
            )
            sql = (
                "SELECT user_name, level, exp FROM user_xiuxian "
                "WHERE user_name IS NOT NULL ORDER BY "
                'xiuxian_int_sort_key_compact("exp") DESC, '
                + realm_level_order_expression()
                + " ASC LIMIT 50"
            )
            plan = [
                str(row[3])
                for row in conn.execute("EXPLAIN QUERY PLAN " + sql).fetchall()
            ]
        self.assertTrue(any("idx_user_xiuxian_realm_rank_v1" in item for item in plan))
        self.assertFalse(any("USE TEMP B-TREE FOR ORDER BY" in item for item in plan))

    def test_public_power_and_root_rank_indexes_preserve_numeric_order(self):
        run_core_migrations(self.database)
        with db_backend.transaction(self.database) as conn:
            conn.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name,power,root_level) "
                "VALUES(%s,%s,%s,%s)",
                (
                    ("power-a", "战力甲", str(10**30 + 2), 3),
                    ("power-b", "战力乙", str(10**30 + 10), 1),
                    ("power-c", "战力丙", str(10**30 + 1), 8),
                ),
            )
            handle = type("RankHandle", (), {})()
            handle.database_path = self.database
            handle._conn_lock = threading.RLock()
            handle.conn = db_backend.connect(self.database)
            self.addCleanup(handle.conn.close)
            handle._read_query = lambda sql, params=None, **kwargs: conn.execute(
                sql, params or ()
            ).fetchall()

            power_rows = XiuxianDateManage.power_top(handle)
            root_rows = XiuxianDateManage.root_top(handle)
            self.assertEqual(
                ["战力乙", "战力甲", "战力丙"], [row[0] for row in power_rows]
            )
            self.assertEqual(
                ["战力丙", "战力甲", "战力乙"], [row[0] for row in root_rows]
            )

            power_plan = [
                str(row[3])
                for row in conn.execute(
                    "EXPLAIN QUERY PLAN SELECT user_name,power FROM user_xiuxian "
                    "WHERE user_name IS NOT NULL ORDER BY "
                    'xiuxian_int_sort_key_compact("power") DESC LIMIT 50'
                ).fetchall()
            ]
            root_plan = [
                str(row[3])
                for row in conn.execute(
                    "EXPLAIN QUERY PLAN SELECT user_name,root_level FROM user_xiuxian "
                    "WHERE user_name IS NOT NULL ORDER BY root_level DESC LIMIT 50"
                ).fetchall()
            ]
        self.assertTrue(
            any("idx_user_xiuxian_power_rank_key_v1" in item for item in power_plan)
        )
        self.assertTrue(
            any("idx_user_xiuxian_root_rank_v1" in item for item in root_plan)
        )

    def test_realm_snapshot_shares_across_handles_until_explicit_invalidation(self):
        run_core_migrations(self.database)
        with db_backend.transaction(self.database) as conn:
            conn.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name,level,exp,stone) "
                "VALUES(%s,%s,%s,%s,%s)",
                (
                    ("cache-a", "缓存甲", "江湖好手", "10", "0"),
                    ("cache-b", "缓存乙", "江湖好手", "9", "0"),
                ),
            )

        class Handle:
            def __init__(self):
                self.database_path = self_outer.database
                self._conn_lock = threading.RLock()
                self.conn = db_backend.connect(self.database_path)
                self.rank_reads = 0

            def _read_query(self, sql, params=None, **kwargs):
                self.rank_reads += 1
                return self.conn.execute(sql, params or ()).fetchall()

            def _clear_read_cache(self):
                return None

        self_outer = self
        first = Handle()
        second = Handle()
        self.addCleanup(first.conn.close)
        self.addCleanup(second.conn.close)

        first_rows = XiuxianDateManage.realm_top(first)
        self.assertEqual("缓存甲", first_rows[0][0])
        self.assertEqual(1, first.rank_reads)
        self.assertEqual(1, process_read_cache.stats()["misses"])
        self.assertEqual(first_rows, XiuxianDateManage.realm_top(second))
        self.assertEqual(0, second.rank_reads)
        self.assertEqual(1, process_read_cache.stats()["hits"])

        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET exp=%s WHERE user_id=%s", ("20", "cache-b")
            )
        snapshot_rows = XiuxianDateManage.realm_top(second)
        self.assertEqual(first_rows, snapshot_rows)
        self.assertEqual(0, second.rank_reads)

        first.conn.execute(
            "UPDATE user_xiuxian SET exp=%s WHERE user_id=%s", ("30", "cache-a")
        )
        XiuxianDateManage._commit_write(first)
        committed_snapshot = XiuxianDateManage.realm_top(second)
        self.assertEqual(first_rows, committed_snapshot)
        self.assertEqual(0, second.rank_reads)

        process_read_cache.invalidate_snapshot_database(self.database)
        refreshed_rows = XiuxianDateManage.realm_top(second)
        self.assertEqual("30", refreshed_rows[0][2])
        self.assertEqual(1, second.rank_reads)
        self.assertEqual(2, process_read_cache.stats()["misses"])
        self.assertGreaterEqual(process_read_cache.stats()["hits"], 3)
        self.assertLessEqual(
            process_read_cache.stats()["estimated_bytes"],
            process_read_cache.stats()["max_bytes"],
        )

    def test_pure_sect_scale_preview_is_shared_snapshot(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE sects(sect_id INTEGER,sect_name TEXT,sect_scale TEXT,sect_owner TEXT)"
            )
            conn.execute("INSERT INTO sects VALUES(1,'青云宗','100','owner')")

        class Handle:
            def __init__(self):
                self.database_path = self_outer.database
                self.conn = db_backend.connect(self.database_path)
                self.reads = 0

            def _read_query(self, sql, params=None, **kwargs):
                self.reads += 1
                return self.conn.execute(sql, params or ()).fetchall()

        self_outer = self
        first = Handle()
        second = Handle()
        self.addCleanup(first.conn.close)
        self.addCleanup(second.conn.close)

        expected = [(1, "青云宗", "100")]
        self.assertEqual(expected, [tuple(row) for row in XiuxianDateManage.scale_top(first)])
        self.assertEqual(expected, [tuple(row) for row in XiuxianDateManage.scale_top(second)])
        self.assertEqual((1, 0), (first.reads, second.reads))

        first.conn.execute("UPDATE sects SET sect_scale='200' WHERE sect_id=1")
        first.conn.commit()
        self.assertEqual(expected, [tuple(row) for row in XiuxianDateManage.scale_top(second)])
        process_read_cache.invalidate_snapshot_database(self.database)
        self.assertEqual(
            [(1, "青云宗", "200")],
            [tuple(row) for row in XiuxianDateManage.scale_top(second)],
        )


if __name__ == "__main__":
    unittest.main()
