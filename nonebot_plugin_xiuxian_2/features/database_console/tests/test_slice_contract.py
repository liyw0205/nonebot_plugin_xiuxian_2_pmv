from __future__ import annotations

import unittest

from .. import (
    application,
    commands,
    jobs,
    migrations,
    repository,
    schemas,
    web,
)
from ..manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface

REPOSITORY_NAMES = (
    "DEFAULT_PRIMARY_KEY",
    "DEFAULT_TABLE_PAGE_SIZE",
    "LIKE_SEARCH_OPERATOR",
    "MAX_RANGE_SEARCH_VALUES",
    "MAX_TABLE_PAGE_SIZE",
    "MIN_TABLE_PAGE_SIZE",
    "RANGE_SEARCH_OPERATORS",
)
APPLICATION_NAMES = (
    "BATCH_ADD_OPERATION",
    "BATCH_SET_OPERATION",
    "BATCH_SUBTRACT_OPERATION",
    "DEFAULT_PRIMARY_KEY",
    "DYNAMIC_TABLE_PRIMARY_KEY",
    "IMPART_CARDS_TABLE",
    "LIKE_SEARCH_OPERATOR",
    "RANGE_SEARCH_OPERATORS",
)


def _broken_connection(_db_path):
    raise AssertionError("table_data must clamp its limits before touching a connection")


class DatabaseConsoleSliceContractTests(unittest.TestCase):
    def make_repository(self) -> repository.DatabaseConsoleRepository:
        return repository.DatabaseConsoleRepository(
            tables_provider=lambda: {},
            dynamic_table_providers=(),
            database_tables_provider=lambda _db_path: {
                "game": {"fields": ["id", "name"], "primary_key": "id"}
            },
            connection_factory=_broken_connection,
            execute_sql=lambda *_args: _broken_connection(None),
            sql_ident=lambda name: f'"{name}"',
            sql_like_text=lambda name: f'"{name}" LIKE %s',
        )

    def test_limits_have_one_home(self) -> None:
        assert_contract_is_single_source(
            self,
            schemas,
            ((repository, REPOSITORY_NAMES), (application, APPLICATION_NAMES)),
        )

    def test_slice_declares_no_autonomous_surface(self) -> None:
        assert_no_autonomous_surface(
            self,
            commands=commands.COMMANDS,
            routes=web.ROUTES,
            jobs=jobs.JOBS,
            migrations=migrations.MIGRATIONS,
            legacy_routes=web.LEGACY_ROUTES,
        )
        self.assertEqual(
            list(web.LEGACY_ROUTES),
            [
                ("GET", "/database", "DatabaseConsoleApplication.list_tables"),
                ("GET", "/table/<table_name>", "DatabaseConsoleApplication.table_data"),
                ("POST", "/table/<table_name>/<row_id>", "DatabaseConsoleApplication.update_row"),
                ("POST", "/batch_edit/<table_name>", "DatabaseConsoleApplication.batch_edit"),
            ],
        )

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "database_console")
        self.assertEqual(FEATURE.test_tag, "database_console")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertIsNone(FEATURE.migration_version)

    def test_declared_delegation_is_a_real_application_method(self) -> None:
        for _method, _path, target in web.LEGACY_ROUTES:
            class_name, _, attribute = target.partition(".")
            self.assertIs(getattr(application, class_name), application.DatabaseConsoleApplication)
            self.assertTrue(callable(getattr(application.DatabaseConsoleApplication, attribute)))

    def test_page_size_is_clamped_before_any_query(self) -> None:
        result = self.make_repository().table_data("db", "game", per_page=9999)
        self.assertEqual(result["per_page"], schemas.MAX_TABLE_PAGE_SIZE)

    def test_unusable_page_falls_back_to_the_declared_minimum(self) -> None:
        result = self.make_repository().table_data("db", "game", page="abc")
        self.assertEqual(result["page"], schemas.MIN_TABLE_PAGE_SIZE)
        self.assertEqual(result["per_page"], schemas.DEFAULT_TABLE_PAGE_SIZE)

    def test_composite_row_key_uses_the_declared_card_keys(self) -> None:
        table_info = {"primary_key": list(schemas.IMPART_CARDS_PRIMARY_KEYS)}
        conditions, fields, is_dynamic = application.DatabaseConsoleApplication(
            self.make_repository()
        ).row_key(schemas.IMPART_CARDS_TABLE, table_info, "u-7_card_of_wind")
        self.assertEqual(conditions, dict(zip(schemas.IMPART_CARDS_PRIMARY_KEYS, ["u-7", "card_of_wind"])))
        self.assertEqual(fields, list(schemas.IMPART_CARDS_PRIMARY_KEYS))
        self.assertFalse(is_dynamic)


if __name__ == "__main__":
    unittest.main()
