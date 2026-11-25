from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.db_migrations import run_core_migrations
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage


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
