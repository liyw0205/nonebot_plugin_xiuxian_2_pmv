from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from ..application import PluginBackupCatalogApplication
from ..repository import PluginBackupCatalogRepository


class PluginBackupCatalogTests(unittest.TestCase):
    def test_repository_lists_compatible_metadata_without_absolute_paths(self) -> None:
        with tempfile.TemporaryDirectory(prefix="plugin-backup-catalog-") as directory:
            root = Path(directory)
            archive = root / "backup_20261006_010203_v2.0.0.zip"
            archive.write_bytes(b"zip")
            (root / "backup_20261005_010203_v1.9.0.zip").write_bytes(b"older")
            (root / "not-a-backup.zip").write_bytes(b"ignored")
            (root / "backup_bad.zip").write_bytes(b"ignored")
            (root / "backup_20261006_010203_v2.0.0.txt").write_bytes(b"ignored")

            backups = PluginBackupCatalogRepository(root).list_plugin_backups()

        self.assertEqual(len(backups), 2)
        self.assertEqual(
            backups,
            sorted(backups, key=lambda item: item["created_at"], reverse=True),
        )
        backup = next(item for item in backups if item["filename"].endswith("v2.0.0.zip"))
        self.assertEqual(backup["filename"], "backup_20261006_010203_v2.0.0.zip")
        self.assertEqual(backup["timestamp"], "20261006_010203")
        self.assertEqual(backup["version"], "v2.0.0")
        self.assertEqual(backup["size"], 3)
        self.assertIn("created_at", backup)
        self.assertNotIn("path", backup)

    def test_repository_skips_symlinks_and_missing_directory_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory(prefix="plugin-backup-catalog-") as directory:
            root = Path(directory)
            link = root / "backup_20261006_010203_external.zip"
            target = root / "target.zip"
            target.write_bytes(b"not listed through link")
            link.symlink_to(target)
            missing = root / "missing"

            self.assertEqual(PluginBackupCatalogRepository(root).list_plugin_backups(), [])
            self.assertEqual(PluginBackupCatalogRepository(missing).list_plugin_backups(), [])
            self.assertFalse(missing.exists())

    def test_application_delegates_to_catalog_repository(self) -> None:
        repository = Mock()
        repository.list_plugin_backups.return_value = [{"filename": "backup_1_2_v1.zip"}]

        backups = PluginBackupCatalogApplication(repository).list_plugin_backups()

        self.assertEqual(backups, [{"filename": "backup_1_2_v1.zip"}])
        repository.list_plugin_backups.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
