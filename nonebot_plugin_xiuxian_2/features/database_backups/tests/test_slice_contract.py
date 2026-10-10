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

APPLICATION_NAMES = ("DATABASE_ALIASES", "MAX_DATABASE_BACKUP_BATCH")
REPOSITORY_NAMES = (
    "DATABASE_BACKUP_ARCHIVE_PATTERN",
    "MAX_DATABASE_BACKUP_CLOUD_LIST_BYTES",
    "MAX_DATABASE_BACKUP_CLOUD_LIST_ENTRIES",
    "MAX_DATABASE_BACKUP_DOWNLOAD_BYTES",
    "MAX_DATABASE_RESTORE_BYTES",
    "MAX_DATABASE_RESTORE_MEMBERS",
)


class DatabaseBackupSliceContractTests(unittest.TestCase):
    def test_storage_contract_has_one_home(self) -> None:
        assert_contract_is_single_source(
            self,
            schemas,
            ((application, APPLICATION_NAMES), (repository, REPOSITORY_NAMES)),
        )
        self.assertTrue(repository.is_database_backup_archive_name("db_backup_x.zip"))
        self.assertFalse(repository.is_database_backup_archive_name("../db_backup_x.zip"))

    def test_archive_affixes_have_one_home(self) -> None:
        """Archive naming is built from the schema affixes, never a retyped literal."""
        assert_contract_is_single_source(
            self,
            schemas,
            ((repository, ("DATABASE_BACKUP_PREFIX", "DATABASE_BACKUP_SUFFIX")),),
        )
        self.assertIs(repository.DATABASE_BACKUP_PREFIX, schemas.DATABASE_BACKUP_PREFIX)
        self.assertIs(repository.DATABASE_BACKUP_SUFFIX, schemas.DATABASE_BACKUP_SUFFIX)
        self.assertIn(
            schemas.DATABASE_BACKUP_PREFIX, schemas.DATABASE_BACKUP_ARCHIVE_PATTERN.pattern
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

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "database_backups")
        self.assertEqual(FEATURE.test_tag, "database_backups")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.routes, ())
        self.assertIsNone(FEATURE.migration_version)


if __name__ == "__main__":
    unittest.main()
