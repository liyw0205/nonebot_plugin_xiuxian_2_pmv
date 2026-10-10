from __future__ import annotations

import unittest

from .. import (
    cloud_application,
    cloud_repository,
    commands,
    creation_repository,
    jobs,
    repository as catalog_repository,
    migrations,
    restore_application,
    restore_repository,
    schemas,
    web,
)
from ..manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface

CLOUD_APPLICATION_NAMES = ("MAX_CLOUD_BACKUP_BATCH", "MAX_CLOUD_LIST_ENTRIES")
CLOUD_REPOSITORY_NAMES = (
    "MAX_CLOUD_LIST_BYTES",
    "MAX_CLOUD_LIST_ENTRIES",
    "MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES",
    "RESTORE_DISK_RESERVE_BYTES",
)
RESTORE_REPOSITORY_NAMES = (
    "PLUGIN_ARCHIVE_ROOT",
    "MAX_ARCHIVE_MEMBERS",
    "RESTORE_DISK_RESERVE_BYTES",
)


class PluginBackupSliceContractTests(unittest.TestCase):
    def test_limits_share_one_definition(self) -> None:
        assert_contract_is_single_source(
            self,
            schemas,
            (
                (cloud_application, CLOUD_APPLICATION_NAMES),
                (cloud_repository, CLOUD_REPOSITORY_NAMES),
                (restore_repository, RESTORE_REPOSITORY_NAMES),
            ),
        )
        # The reserve used to be declared twice with the same value in two files.
        self.assertIs(
            cloud_repository.RESTORE_DISK_RESERVE_BYTES,
            restore_repository.RESTORE_DISK_RESERVE_BYTES,
        )

    def test_archive_patterns_have_one_definition(self) -> None:
        self.assertIs(restore_application._BACKUP_VERSION_RE, schemas.ARCHIVE_NAME_PATTERN)
        self.assertIs(creation_repository._TIMESTAMP, schemas.ARCHIVE_TIMESTAMP_PATTERN)
        self.assertIs(creation_repository._SAFE_VERSION, schemas.VERSION_SAFE_PATTERN)
        self.assertIs(creation_repository._SKIP_DIRECTORY_NAMES, schemas.SKIP_DIRECTORY_NAMES)
        self.assertIs(creation_repository._TRANSIENT_DATA_PATHS, schemas.TRANSIENT_DATA_PATHS)

    def test_archive_affixes_have_one_home(self) -> None:
        """The archive prefix/suffix may not be retyped in an implementing module."""
        assert_contract_is_single_source(
            self,
            schemas,
            (
                (creation_repository, ("ARCHIVE_PREFIX", "ARCHIVE_SUFFIX")),
                (cloud_repository, ("ARCHIVE_SUFFIX",)),
                (restore_repository, ("ARCHIVE_SUFFIX",)),
                (catalog_repository, ("ARCHIVE_PREFIX", "ARCHIVE_SUFFIX")),
            ),
        )
        self.assertIs(creation_repository.ARCHIVE_PREFIX, schemas.ARCHIVE_PREFIX)
        self.assertIs(catalog_repository.ARCHIVE_SUFFIX, schemas.ARCHIVE_SUFFIX)
        # The accepted name shape is built by the two affixes above, never retyped.
        self.assertIn(schemas.ARCHIVE_PREFIX, schemas.ARCHIVE_NAME_PATTERN.pattern)
        self.assertIn(schemas.ARCHIVE_SUFFIX.lstrip("."), schemas.ARCHIVE_NAME_PATTERN.pattern)

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
        self.assertEqual(FEATURE.key, "plugin_backups")
        self.assertEqual(FEATURE.test_tag, "plugin_backups")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.routes, ())
        self.assertIsNone(FEATURE.migration_version)


if __name__ == "__main__":
    unittest.main()
