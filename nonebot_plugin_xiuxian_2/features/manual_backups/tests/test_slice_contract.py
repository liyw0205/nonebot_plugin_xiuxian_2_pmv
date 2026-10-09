from __future__ import annotations

import unittest

from nonebot_plugin_xiuxian_2.features.manual_backups import (
    application,
    commands,
    jobs,
    migrations,
    repository,
    schemas,
    web,
)
from nonebot_plugin_xiuxian_2.features.manual_backups.manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface


class ManualBackupSliceContractTests(unittest.TestCase):
    def test_result_contract_has_one_home(self) -> None:
        assert_contract_is_single_source(
            self, schemas, ((application, ("ManualBackupResult",)),)
        )
        self.assertIs(application.ManualBackupResult, schemas.ManualBackupResult)

    def test_ports_live_in_the_repository_module(self) -> None:
        self.assertIn("PluginBackupCreationPort", vars(repository))
        self.assertIn("ConfigBackupProvider", vars(repository))
        self.assertNotIn("Protocol", vars(application))

    def test_slice_declares_no_autonomous_surface(self) -> None:
        assert_no_autonomous_surface(
            self,
            commands=commands.COMMANDS,
            routes=web.ROUTES,
            jobs=jobs.JOBS,
            migrations=migrations.MIGRATIONS,
            legacy_routes=web.LEGACY_ROUTES,
        )

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "manual_backups")
        self.assertEqual(FEATURE.test_tag, "manual_backups")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.routes, ())
        self.assertIsNone(FEATURE.migration_version)


if __name__ == "__main__":
    unittest.main()
