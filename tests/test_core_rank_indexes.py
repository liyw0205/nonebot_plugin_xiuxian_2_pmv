from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.db_migrations import run_core_migrations
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage


class CoreRankIndexTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "game.db"

        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(id INTEGER PRIMARY KEY, user_id TEXT, exp TEXT, stone TEXT)"
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


if __name__ == "__main__":
    unittest.main()
