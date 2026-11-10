from __future__ import annotations

import unittest
import sqlite3

from nonebot_plugin_xiuxian_2.core.numeric import (
    as_int_like,
    normalize_numeric_row,
    normalize_sect_row,
    normalize_user_row,
    register_numeric_functions,
)


class CoreNumericTests(unittest.TestCase):
    def test_user_rows_normalize_oversized_and_scientific_text_without_mutating(self):
        source = {"sect_contribution": str(2**70), "stone": "1.25e3", "name": "u"}

        normalized = normalize_user_row(source)

        self.assertEqual(2**70, normalized["sect_contribution"])
        self.assertEqual(1250, normalized["stone"])
        self.assertEqual("u", normalized["name"])
        self.assertEqual(str(2**70), source["sect_contribution"])

    def test_sect_normalization_is_limited_to_sect_numeric_fields(self):
        normalized = normalize_sect_row(
            {"sect_scale": str(2**70), "sect_contribution": str(2**70)}
        )

        self.assertEqual(2**70, normalized["sect_scale"])
        self.assertEqual(str(2**70), normalized["sect_contribution"])

    def test_generic_normalizer_and_integer_parser_keep_fallbacks(self):
        self.assertEqual({"score": 42}, normalize_numeric_row({"score": "42"}, {"score"}))
        self.assertEqual(7, as_int_like("not-a-number", 7))
        self.assertEqual(0, as_int_like(None))

    def test_sql_sort_keeps_real_normalization_and_exact_text_order(self):
        with sqlite3.connect(':memory:') as conn:
            register_numeric_functions(conn)
            conn.execute('CREATE TABLE numbers(value)')
            values = [None, '-1e3', -2, '9e0', 1e29, str(int(1e29)), str(int(1e29) + 1), '-100000000000000000001']
            conn.executemany('INSERT INTO numbers VALUES(?)', [(value,) for value in values])
            rows = conn.execute('SELECT value,dense_rank() OVER (ORDER BY xiuxian_int_text(value) COLLATE xiuxian_integer DESC) FROM numbers ORDER BY xiuxian_int_text(value) COLLATE xiuxian_integer DESC,rowid').fetchall()
        self.assertEqual([str(int(1e29) + 1), 1e29, str(int(1e29)), '9e0', -2, '-1e3', '-100000000000000000001', None], [row[0] for row in rows])
        self.assertEqual([1, 2, 2, 3, 4, 5, 6, 7], [row[1] for row in rows])

    def test_sql_integer_aggregation_normalizes_each_storage_value(self):
        with sqlite3.connect(':memory:') as conn:
            register_numeric_functions(conn)
            conn.execute('CREATE TABLE numbers(value)')
            values = [2**62, 2**62, 1e29, '1.00000000000000000001e20', None, '-1e3']
            conn.executemany('INSERT INTO numbers VALUES(?)', [(v,) for v in values])
            self.assertEqual(str(2**63 + int(1e29) + 100000000000000000001 - 1000), conn.execute('SELECT xiuxian_int_sum(value) FROM numbers').fetchone()[0])
            self.assertEqual(str(2**63), conn.execute('SELECT xiuxian_int_abs(?)', (-(2**63),)).fetchone()[0])
            self.assertEqual(0, conn.execute('SELECT COALESCE(xiuxian_int_sum(value),0) FROM numbers WHERE 0').fetchone()[0])


if __name__ == "__main__":
    unittest.main()
