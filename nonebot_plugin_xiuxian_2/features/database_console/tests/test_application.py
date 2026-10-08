from __future__ import annotations

import unittest
from unittest.mock import Mock

from ..application import DatabaseConsoleApplication


class _Repository:
    def __init__(self) -> None:
        self.tables = {
            "game": {
                "path": "game.db",
                "tables": {
                    "inventory": {
                        "fields": ["user_id", "goods_id", "amount"],
                        "primary_key": ["user_id", "goods_id"],
                        "is_dynamic": True,
                    }
                },
            }
        }
        self.executed: list[tuple[str, list[object]]] = []
        self.table_exists = Mock(return_value=True)

    def all_tables(self):
        return self.tables

    def resolve_table(self, name):
        for group in self.tables.values():
            if name in group["tables"]:
                return group["path"], group["tables"][name]
        return None, None

    @staticmethod
    def quote_identifier(name):
        return f'"{name}"'

    @staticmethod
    def like_text(field):
        return f'CAST("{field}" AS TEXT) LIKE %s'

    def execute(self, _db, sql, params):
        self.executed.append((sql, params))
        return {"affected_rows": 2}


class DatabaseConsoleApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = _Repository()
        self.application = DatabaseConsoleApplication(self.repository)

    def test_composite_row_key_preserves_underscore_suffix(self) -> None:
        conditions, fields, dynamic = self.application.row_key(
            "impart_cards",
            {"primary_key": ["user_id", "card_name"], "is_dynamic": False},
            "100_card_with_suffix",
        )
        self.assertEqual(conditions, {"user_id": "100", "card_name": "card_with_suffix"})
        self.assertEqual(fields, ["user_id", "card_name"])
        self.assertFalse(dynamic)

    def test_batch_edit_requires_filter_or_explicit_full_table_guard(self) -> None:
        result = self.application.batch_edit(
            "inventory",
            {"batch_field": "amount", "operation": "set", "value": "5"},
        )
        self.assertFalse(result["success"])
        self.assertIn("搜索内容", result["error"])
        self.assertEqual(self.repository.executed, [])

    def test_batch_edit_binds_search_value_and_update_value(self) -> None:
        result = self.application.batch_edit(
            "inventory",
            {
                "batch_field": "amount",
                "operation": "add",
                "value": "5",
                "search_field": "goods_id",
                "search_value": "sword",
            },
        )
        self.assertTrue(result["success"])
        sql, params = self.repository.executed[-1]
        self.assertIn("WHERE", sql)
        self.assertEqual(params, [5, "%sword%"])


if __name__ == "__main__":
    unittest.main()
