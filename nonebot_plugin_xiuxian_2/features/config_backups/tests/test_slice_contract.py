from __future__ import annotations

import unittest

from nonebot_plugin_xiuxian_2.features.config_backups import (
    commands,
    jobs,
    migrations,
    repository,
    schemas,
    web,
)
from nonebot_plugin_xiuxian_2.features.config_backups.manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface

REPOSITORY_NAMES = (
    "MAX_CONFIG_BACKUP_BYTES",
    "MAX_CONFIG_CLOUD_LIST_BYTES",
    "MAX_CONFIG_CLOUD_LIST_ENTRIES",
    "CONFIG_BACKUP_PREFIX",
    "CONFIG_BACKUP_SUFFIX",
)


class ConfigBackupSliceContractTests(unittest.TestCase):
    def test_storage_contract_has_one_home(self) -> None:
        assert_contract_is_single_source(self, schemas, ((repository, REPOSITORY_NAMES),))
        self.assertTrue(repository.is_config_backup_filename("config_backup_a.json"))
        self.assertFalse(repository.is_config_backup_filename("../config_backup_a.json"))

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
        self.assertEqual(FEATURE.key, "config_backups")
        self.assertEqual(FEATURE.test_tag, "config_backups")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertIsNone(FEATURE.migration_version)


if __name__ == "__main__":
    unittest.main()
