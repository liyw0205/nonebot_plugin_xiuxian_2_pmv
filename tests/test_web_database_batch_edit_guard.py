from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import nonebot
import tests  # Keep web-module imports on the isolated test data directory.

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import database as database_routes


class DatabaseBatchEditGuardTests(unittest.TestCase):
    csrf_token = "database-batch-edit-csrf"

    def setUp(self) -> None:
        core.app.config.update(TESTING=True, SECRET_KEY="database-batch-edit-tests")
        admin_ids = patch.object(core, "ADMIN_IDS", {"admin-1"})
        admin_ids.start()
        self.addCleanup(admin_ids.stop)
        self.client = core.app.test_client()
        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = self.csrf_token

    def _post(self, data: dict[str, str]):
        return self.client.post(
            "/batch_edit/inventory",
            data=data,
            headers={"X-CSRF-Token": self.csrf_token},
        )

    def _route_patches(self):
        connection = Mock()
        connection.table_exists.return_value = True
        tables = {
            "player": {
                "path": "test-player.db",
                "tables": {
                    "inventory": {
                        "fields": ["id", "name", "amount"],
                        "primary_key": "id",
                    }
                },
            }
        }
        return (
            patch.object(database_routes, "get_tables", return_value=tables),
            patch.object(database_routes, "get_db_connection", return_value=connection),
            patch.object(database_routes, "execute_sql", return_value={"affected_rows": 2}),
        )

    @staticmethod
    def _base_form() -> dict[str, str]:
        return {"batch_field": "amount", "operation": "set", "value": "5"}

    def test_missing_or_blank_search_does_not_update_any_rows(self) -> None:
        get_tables, get_connection, execute_sql = self._route_patches()
        with get_tables, get_connection as connection, execute_sql as execute:
            cases = (
                self._base_form(),
                {**self._base_form(), "search_field": "name", "search_value": ""},
                {**self._base_form(), "search_field": "name", "search_value": "   "},
                {**self._base_form(), "search_field": "", "search_value": "\t  "},
            )
            for form in cases:
                with self.subTest(form=form):
                    response = self._post(form)
                    self.assertFalse(response.get_json()["success"])
                    self.assertIn("搜索内容", response.get_json()["error"])

            execute.assert_not_called()
            connection.assert_not_called()

    def test_valid_search_filter_is_preserved(self) -> None:
        get_tables, get_connection, execute_sql = self._route_patches()
        with get_tables, get_connection, execute_sql as execute:
            response = self._post(
                {
                    **self._base_form(),
                    "search_field": "name",
                    "search_value": "alice",
                }
            )

        self.assertTrue(response.get_json()["success"])
        sql, params = execute.call_args.args[1:]
        self.assertIn(" WHERE ", sql)
        self.assertIn("name", sql)
        self.assertEqual(params[1], "%alice%")

    def test_valid_search_without_field_uses_full_field_filter(self) -> None:
        get_tables, get_connection, execute_sql = self._route_patches()
        with get_tables, get_connection, execute_sql as execute:
            response = self._post(
                {**self._base_form(), "search_field": "", "search_value": "alice"}
            )

        self.assertTrue(response.get_json()["success"])
        sql, params = execute.call_args.args[1:]
        self.assertIn(" WHERE ", sql)
        self.assertEqual(params[1:], ["%alice%", "%alice%"])

    def test_explicit_apply_to_all_allows_unfiltered_update(self) -> None:
        get_tables, get_connection, execute_sql = self._route_patches()
        with get_tables, get_connection, execute_sql as execute:
            response = self._post({**self._base_form(), "apply_to_all": "on"})

        self.assertTrue(response.get_json()["success"])
        sql, params = execute.call_args.args[1:]
        self.assertNotIn(" WHERE ", sql)
        self.assertEqual(len(params), 1)


if __name__ == "__main__":
    unittest.main()
