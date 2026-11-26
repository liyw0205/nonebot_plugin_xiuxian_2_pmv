from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import sqlite3
import tempfile
import unittest

from nonebot_plugin_xiuxian_2.core.numeric import (
    register_numeric_functions,
    sqlite_int_sort_key,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage


class _RecordedConnection:
    def __init__(self, connection):
        self.connection = connection
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self.connection.execute(sql, params)

    def count_queries(self):
        return [sql for sql, _ in self.calls if "COUNT(DISTINCT" in sql.upper()]

    def recursive_queries(self):
        return [sql for sql, _ in self.calls if sql.lstrip().upper().startswith("WITH RECURSIVE")]


class ExactRankSeekTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name) / "ranks.db"
        with sqlite3.connect(self.database) as connection:
            register_numeric_functions(connection)
            connection.execute(
                "CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT,exp,stone)"
            )
            for column in ("exp", "stone"):
                connection.execute(
                    f'CREATE INDEX idx_user_xiuxian_{column}_rank_key_v2 '
                    f'ON user_xiuxian(xiuxian_int_sort_key_compact("{column}"))'
                )

    def _replace(self, values):
        with sqlite3.connect(self.database) as connection:
            register_numeric_functions(connection)
            connection.execute("DELETE FROM user_xiuxian")
            connection.executemany(
                "INSERT INTO user_xiuxian(user_id,exp,stone) VALUES(?,?,?)",
                [(str(index), value, value) for index, value in enumerate(values)],
            )

    @contextmanager
    def _connection(self):
        connection = db_backend.connect_readonly(self.database)
        try:
            connection.execute("PRAGMA busy_timeout=0")
            connection.execute("BEGIN")
            yield _RecordedConnection(connection)
        finally:
            connection.rollback()
            connection.close()

    def _rank(self, connection, value, *, column="exp", ceiling=None):
        return XiuxianDateManage._read_numeric_rank_for_value(
            SimpleNamespace(), column, sqlite_int_sort_key(value),
            connection=connection, ceiling=ceiling,
        )

    def test_zero_one_and_128_step_boundaries_keep_exact_dense_rank(self):
        for higher in (0, 1, 127, 128, 129):
            self._replace(list(range(higher + 1)))
            for column in ("exp", "stone"):
                with self.subTest(higher=higher, column=column), self._connection() as connection:
                    self.assertEqual(higher + 1, self._rank(connection, 0, column=column))
                    self.assertEqual(len(connection.count_queries()), int(higher >= 128))
                    self.assertEqual(len(connection.recursive_queries()), 1)
                    # One plan, one bounded seek statement, at most one remainder count.
                    self.assertLessEqual(len(connection.calls), 3)

    def test_ceiling_inclusion_equal_target_and_remainder_boundaries(self):
        self._replace(list(range(301)))
        for upper in (0, 1, 127, 128, 129, 200):
            for column in ("exp", "stone"):
                with self.subTest(upper=upper, column=column), self._connection() as connection:
                    ceiling = (sqlite_int_sort_key(upper), 301 - upper)
                    self.assertEqual(
                        301, self._rank(connection, 0, column=column, ceiling=ceiling)
                    )
                    self.assertEqual(len(connection.count_queries()), int(upper >= 128))
                    self.assertLessEqual(len(connection.calls), 3)

    def test_duplicate_groups_negative_and_arbitrary_integer_storage_keep_precision(self):
        huge = 10**100
        values = [
            (None, None), (b"-1000000000000000000000000000001", -(10**30 + 1)),
            ("-1e30", -(10**30)), (-5, -5), (0, 0), ("0e2", 0),
            ("not-an-integer", 0), (2, 2), ("2.0", 2), ("1e3", 1000),
            (1000, 1000), (b"1000000000000000000000000000001", 10**30 + 1),
            (str(huge), huge), (str(huge + 1).encode("ascii"), huge + 1),
        ]
        self._replace([raw for raw, _ in values for _ in range(17)])
        distinct = {number for _, number in values if number is not None}
        for raw, number in values:
            expected = 1 + sum(number is None or other > number for other in distinct)
            for column in ("exp", "stone"):
                with self.subTest(raw=raw, column=column), self._connection() as connection:
                    self.assertEqual(expected, self._rank(connection, raw, column=column))
                    self.assertEqual(len(connection.count_queries()), int(number is None))
                    self.assertEqual(len(connection.recursive_queries()), int(number is not None))
                    self.assertLessEqual(len(connection.calls), 2)

    def test_missing_or_wrong_named_index_falls_back_once_without_recursive_scan(self):
        self._replace(list(range(300)))
        for column in ("exp", "stone"):
            index = f"idx_user_xiuxian_{column}_rank_key_v2"
            with sqlite3.connect(self.database) as writer:
                register_numeric_functions(writer)
                writer.execute(f'DROP INDEX "{index}"')
            for wrong_index in (False, True):
                if wrong_index:
                    with sqlite3.connect(self.database) as writer:
                        writer.execute(f'CREATE INDEX "{index}" ON user_xiuxian(user_id)')
                with self.subTest(column=column, wrong_index=wrong_index), self._connection() as connection:
                    self.assertEqual(300, self._rank(connection, 0, column=column))
                    self.assertEqual(len(connection.count_queries()), 1)
                    self.assertEqual(len(connection.recursive_queries()), 0)
                    self.assertLessEqual(len(connection.calls), 2)
            with sqlite3.connect(self.database) as writer:
                register_numeric_functions(writer)
                writer.execute(f'DROP INDEX "{index}"')
                writer.execute(
                    f'CREATE INDEX "{index}" '
                    f'ON user_xiuxian(xiuxian_int_sort_key_compact("{column}"))'
                )
            with self._connection() as connection:
                self.assertEqual(300, self._rank(connection, 0, column=column))
                self.assertEqual(len(connection.recursive_queries()), 1)

    def test_read_denial_propagates_without_fallback_and_recovers(self):
        self._replace([0, 1, 2])
        with self._connection() as connection:
            raw = connection.connection._raw
            raw.set_authorizer(
                lambda action, table, *_: sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "user_xiuxian"
                else sqlite3.SQLITE_OK
            )
            try:
                with self.assertRaises(sqlite3.DatabaseError):
                    self._rank(connection, 0)
            finally:
                raw.set_authorizer(None)
            self.assertEqual(len(connection.count_queries()), 0)
            connection.calls.clear()
            self.assertEqual(3, self._rank(connection, 0))
            self.assertEqual(len(connection.count_queries()), 0)

    def test_real_exclusive_lock_propagates_without_fallback_and_recovers(self):
        self._replace([0, 1, 2])
        # The small fixture uses DELETE journaling so an exclusive writer blocks reads.
        with self._connection() as connection:
            with sqlite3.connect(self.database, timeout=0) as writer:
                writer.execute("BEGIN EXCLUSIVE")
                try:
                    with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                        self._rank(connection, 0)
                finally:
                    writer.rollback()
            self.assertEqual(len(connection.count_queries()), 0)
            connection.calls.clear()
            self.assertEqual(3, self._rank(connection, 0))
            self.assertEqual(len(connection.count_queries()), 0)


if __name__ == "__main__":
    unittest.main()
