from contextlib import contextmanager
from pathlib import Path
import sqlite3
import tempfile
import unittest

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage


class GoodsTotalsBatchTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "holdings.db"
        self.writer = db_backend.connect(self.path)
        self.addCleanup(self.writer.close)
        self.writer.execute("CREATE TABLE back(goods_id INTEGER,goods_num INTEGER)")
        self.writer.execute("CREATE INDEX idx_back_goods_id ON back(goods_id)")
        self.writer.executemany("INSERT INTO back VALUES(%s,%s)", [(1, 9), (1, -2), (2, None), (3, 0)])
        self.writer.commit()
        self.manager = object.__new__(XiuxianDateManage)
        self.borrows = 0
        self.queries = []
        self.before_query = None

        @contextmanager
        def reader():
            self.borrows += 1
            conn = db_backend.connect_readonly(self.path)
            owner = self

            class Observed:
                def execute(self, sql, params=None):
                    if sql.startswith("SELECT requested.column1"):
                        owner.queries.append((sql, params))
                        if owner.before_query:
                            owner.before_query(len(owner.queries))
                    return conn.execute(sql, params)

                def rollback(self):
                    conn.rollback()

            try:
                yield Observed()
            finally:
                self.assertFalse(conn.in_transaction)
                conn.close()

        self.manager._repository_read_connection = reader

    def test_aliases_duplicates_nulls_negatives_and_missing_match_individual_sums(self):
        ids = [1, "01", "1", 1, 2, 3, 404, "1 OR 1=1", None]
        expected = {}
        for goods_id in ids:
            value = self.writer.execute("SELECT SUM(goods_num) FROM back WHERE goods_id=%s", (goods_id,)).fetchone()[0]
            expected[goods_id] = value if value is not None else 0
        self.assertEqual(self.manager.goods_max_nums(ids), expected)
        self.assertEqual((self.borrows, len(self.queries)), (1, 1))
        self.assertNotIn("1 OR 1=1", self.queries[0][0])
        plan = self.writer.execute("EXPLAIN QUERY PLAN " + self.queries[0][0], self.queries[0][1]).fetchall()
        self.assertTrue(any("idx_back_goods_id" in row[3] for row in plan))

    def test_empty_input_avoids_connection_and_standalone_call_is_fresh(self):
        self.assertEqual(self.manager.goods_max_nums([]), {})
        self.assertEqual(self.borrows, 0)
        self.assertEqual(self.manager.goods_max_nums([1]), {1: 7})
        self.writer.execute("UPDATE back SET goods_num=goods_num+1 WHERE goods_id=1")
        self.writer.commit()
        self.assertEqual(self.manager.goods_max_nums([1]), {1: 9})
        self.assertEqual(self.borrows, 2)

    def test_chunks_share_one_snapshot_despite_concurrent_commit(self):
        self.writer.execute("INSERT INTO back VALUES(401,10)")
        self.writer.commit()

        def between_chunks(number):
            if number == 2:
                self.writer.execute("UPDATE back SET goods_num=99 WHERE goods_id=401")
                self.writer.commit()

        self.before_query = between_chunks
        totals = self.manager.goods_max_nums(range(1, 804))
        self.assertEqual((self.borrows, len(self.queries)), (1, 3))
        self.assertEqual([len(params) for _, params in self.queries], [400, 400, 3])
        self.assertEqual(totals[401], 10)
        self.assertEqual(totals[803], 0)
        self.before_query = None
        self.assertEqual(self.manager.goods_max_nums([401]), {401: 99})

    def test_sql_errors_and_integer_sum_overflow_propagate_and_close_transaction(self):
        self.writer.executemany("INSERT INTO back VALUES(7,%s)", [(2**63 - 1,), (1,)])
        self.writer.commit()
        with self.assertRaises(sqlite3.OperationalError):
            self.manager.goods_max_nums([7])
        self.writer.execute("DROP TABLE back")
        self.writer.commit()
        with self.assertRaises(sqlite3.OperationalError):
            self.manager.goods_max_nums([1])
