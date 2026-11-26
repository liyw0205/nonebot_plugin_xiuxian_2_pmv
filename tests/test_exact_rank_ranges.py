from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import random
import tempfile
import unittest

from nonebot_plugin_xiuxian_2.core.numeric import sqlite_int_sort_key
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.db_migrations import run_core_migrations
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import ProcessReadCache, process_read_cache, shared_snapshot_key
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage


class ExactRankRangeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "rank.db"
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        self.values = [None, "invalid", "-1000000000000000000000000000000", -5,
                       0, "0e2", 2, "2.0", "1e3", 1000, b"1000000000000000000000000000001"]
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT,exp,stone)")
            conn.executemany("INSERT INTO user_xiuxian(user_id,exp,stone) VALUES(?,?,?)",
                             [(str(index), value, value) for index, value in enumerate(self.values)])
        run_core_migrations(self.database)

    @contextmanager
    def handle(self):
        with db_backend.connection(self.database) as conn:
            @contextmanager
            def connection():
                yield conn
            yield SimpleNamespace(database_path=self.database, _repository_read_connection=connection), conn

    def rank(self, handle, user):
        return XiuxianDateManage.get_exp_and_stone_ranks(handle, str(user))

    def test_ranges_preserve_dense_ranks_for_both_orders_ties_and_exact_values(self):
        expected_keys = [sqlite_int_sort_key(value) for value in self.values]
        expected = [1 + len({key for key in expected_keys if key is not None and (target is None or key > target)})
                    for target in expected_keys]
        order = list(range(len(self.values)))
        shuffled = order.copy()
        random.Random(812).shuffle(shuffled)
        with self.handle() as (handle, conn):
            for sequence in (order, order[::-1], shuffled):
                process_read_cache.clear()
                for index in sequence:
                    with self.subTest(sequence=sequence, user=index):
                        self.assertEqual((expected[index], expected[index]), self.rank(handle, index))
                self.assertFalse(conn.in_transaction)
                self.assertLessEqual(process_read_cache.stats()["entries"], 32)

    def test_ceiling_limits_index_range_but_lower_anchor_and_null_do_not(self):
        with self.handle() as (handle, conn):
            self.rank(handle, 8)  # 1000, rank2
            sql = []
            conn._raw.set_trace_callback(lambda value: sql.append(value) if "COUNT(DISTINCT" in value else None)
            self.assertEqual((3, 3), self.rank(handle, 6))
            self.assertEqual(2, len(sql))
            self.assertTrue(all("<=" in query for query in sql))
            plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql[0])]
            self.assertTrue(any("idx_user_xiuxian_exp_rank_key_v2" in row and "<expr><?" in row for row in plan))
            self.assertFalse(any("TEMP" in row for row in plan))
            sql.clear()
            self.assertEqual((1, 1), self.rank(handle, 10))
            self.assertEqual(2, len(sql))
            self.assertTrue(all("<=" not in query for query in sql))
            sql.clear()
            self.rank(handle, 0)
            self.assertEqual(2, len(sql))
            self.assertTrue(all("IS NOT NULL" in query and "<=" not in query for query in sql))

    def test_anchor_and_count_share_snapshot_across_committed_and_rolled_back_writes(self):
        with self.handle() as (handle, conn):
            self.rank(handle, 8)
            execute = conn.execute
            changed = False
            def intercept(sql, params=None):
                nonlocal changed
                cursor = execute(sql, params)
                if sql.startswith("SELECT rank_state.exp_generation") and not changed:
                    row = cursor.fetchone()
                    with db_backend.transaction(self.database) as writer:
                        writer.execute("UPDATE user_xiuxian SET exp=1,stone=1 WHERE user_id='8'")
                        writer.execute("UPDATE user_xiuxian SET exp=1,stone=1 WHERE user_id='9'")
                    changed = True
                    return SimpleNamespace(fetchone=lambda: row)
                return cursor
            conn.execute = intercept
            self.assertEqual((3, 3), self.rank(handle, 6))
            self.assertEqual(2, process_read_cache.stats()["partial_hits"])
            # New generation must not reuse the old 1000 anchor or the old target result.
            self.assertEqual((3, 3), self.rank(handle, 8))
            self.assertEqual((2, 2), self.rank(handle, 6))
            with db_backend.connection(self.database) as writer:
                writer.execute("BEGIN")
                writer.execute("UPDATE user_xiuxian SET exp=-10,stone=-10 WHERE user_id='8'")
                writer.rollback()
            self.assertEqual((2, 2), self.rank(handle, 6))
            prior = process_read_cache.stats()["partial_hits"]
            self.assertEqual((4, 4), self.rank(handle, 4))
            self.assertEqual(prior + 2, process_read_cache.stats()["partial_hits"])


class SnapshotCeilingTests(unittest.TestCase):
    def test_matching_is_bounded_by_namespace_identity_generation_ttl_and_invalidation(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "rank.db"
            database.touch()
            now = [0]
            cache = ProcessReadCache(max_entries=3, clock=lambda: now[0])
            prefix = shared_snapshot_key(database, "exact", "exp", 1)
            for value in (b"2", b"4", b"6"):
                cache.get_or_load((*prefix, value), database_path=database, ttl=10,
                                  snapshot=True, loader=lambda value=value: [value])
            found = cache.get_snapshot_ceiling(prefix, b"3", database_path=database)
            self.assertEqual((b"4", [b"4"]), found)
            found[1].append("caller mutation")
            self.assertEqual((b"4", [b"4"]), cache.get_snapshot_ceiling(prefix, b"3", database_path=database))
            self.assertIsNone(cache.get_snapshot_ceiling(prefix, b"9", database_path=database))
            cache.get_or_load((*prefix, b"8"), database_path=database, ttl=10,
                              snapshot=True, loader=lambda: [b"8"])
            self.assertEqual(1, cache.stats()["evictions"])
            self.assertEqual((b"4", [b"4"]), cache.get_snapshot_ceiling(prefix, b"1", database_path=database))
            for other in (shared_snapshot_key(database, "exact", "stone", 1),
                          shared_snapshot_key(database, "exact", "exp", 2)):
                self.assertIsNone(cache.get_snapshot_ceiling(other, b"3", database_path=database))
            replacement = Path(temp) / "replacement.db"
            replacement.touch()
            replacement.replace(database)
            self.assertIsNone(cache.get_snapshot_ceiling(shared_snapshot_key(database, "exact", "exp", 1), b"3", database_path=database))
            now[0] = 11
            self.assertIsNone(cache.get_snapshot_ceiling(prefix, b"3", database_path=database))
            now[0] = 0
            cache.invalidate_snapshot_database(database)
            self.assertIsNone(cache.get_snapshot_ceiling(prefix, b"3", database_path=database))
            self.assertLessEqual(cache.stats()["entries"], 3)

    def test_ceiling_does_not_cross_snapshot_database_or_dependency_boundaries(self):
        with tempfile.TemporaryDirectory() as temp:
            database, dependency = Path(temp) / "main.db", Path(temp) / "other.db"
            prefix, cache = ("same",), ProcessReadCache()
            for path, snapshot, dependencies, value in (
                (database, False, (), b"1"), (dependency, True, (), b"2"),
                (database, True, (dependency,), b"3"),
            ):
                cache.get_or_load((*prefix, value), database_path=path, ttl=10,
                                  snapshot=snapshot, dependency_databases=dependencies, loader=lambda: 7)
            self.assertIsNone(cache.get_snapshot_ceiling(prefix, b"0", database_path=database))
            self.assertEqual((b"3", 7), cache.get_snapshot_ceiling(prefix, b"0", database_path=database, dependency_databases=(dependency,)))
            cache.invalidate_snapshot_database(dependency)
            self.assertIsNone(cache.get_snapshot_ceiling(prefix, b"0", database_path=database, dependency_databases=(dependency,)))
