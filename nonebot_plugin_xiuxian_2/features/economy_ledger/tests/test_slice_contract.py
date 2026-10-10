from __future__ import annotations

import unittest

from .. import application, commands, jobs, migrations, repository, schemas, web
from ..manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source


APPLICATION_SCHEMA_NAMES = (
    "DEFAULT_ANOMALY_STONE_DELTA",
    "DEFAULT_PAGE_SIZE",
    "FILTER_FIELDS",
    "MAX_PAGE_SIZE",
    "QUICK_PRESETS",
    "TIME_FILTER_FIELDS",
)
REPOSITORY_SCHEMA_NAMES = (
    "DEFAULT_ANOMALY_STONE_DELTA",
    "DELTA_FIELDS",
    "ECONOMY_LOG_FIELDS",
    "FILTER_FIELDS",
    "QUICK_PRESETS",
)


class EconomyLedgerSliceContractTests(unittest.TestCase):
    def test_query_contract_has_one_home(self) -> None:
        assert_contract_is_single_source(
            self,
            schemas,
            (
                (application, APPLICATION_SCHEMA_NAMES),
                (repository, REPOSITORY_SCHEMA_NAMES),
            ),
        )

    def test_migration_declaration_matches_the_runner(self) -> None:
        self.assertEqual(migrations.MIGRATIONS, (FEATURE.migration_version,))
        self.assertEqual(FEATURE.migration_version, "economy_ledger.001")
        self.assertTrue(callable(migrations.apply_economy_ledger_read_indexes))

    def test_surfaces_and_manifest_are_explicit(self) -> None:
        self.assertEqual(commands.COMMANDS, ())
        self.assertEqual(web.ROUTES, ())
        self.assertEqual(jobs.JOBS, ())
        self.assertEqual(FEATURE.key, "economy_ledger")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.test_tag, "economy_ledger")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertEqual(FEATURE.config, ())
        self.assertEqual(
            list(web.LEGACY_ROUTES),
            [
                ("GET", "/api/v1/economy-logs", "EconomyLedgerApplication.query_page"),
                ("GET", "/api/v1/economy-logs/export", "EconomyLedgerApplication.iter_export_rows"),
            ],
        )

    def test_delegations_resolve_to_public_application_methods(self) -> None:
        for _method, _path, target in web.LEGACY_ROUTES:
            class_name, _, attribute = target.partition(".")
            self.assertIs(getattr(application, class_name), application.EconomyLedgerApplication)
            self.assertTrue(callable(getattr(application.EconomyLedgerApplication, attribute)))


if __name__ == "__main__":
    unittest.main()
