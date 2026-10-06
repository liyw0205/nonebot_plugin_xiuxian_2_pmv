from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from ..file_application import PluginBackupFileApplication
from ..file_repository import (
    InvalidPluginBackupFile,
    PluginBackupFileNotFound,
    PluginBackupFileRepository,
)


class PluginBackupFileTests(unittest.TestCase):
    def test_repository_opens_only_regular_catalogued_archives(self) -> None:
        with tempfile.TemporaryDirectory(prefix="plugin-backup-files-") as directory:
            root = Path(directory)
            archive = root / "backup_20261006_010203_v2.0.0.zip"
            archive.write_bytes(b"zip-bytes")
            repository = PluginBackupFileRepository(root)

            with repository.open_plugin_backup(archive.name) as source:
                self.assertEqual(source.read(), b"zip-bytes")
            with self.assertRaises(InvalidPluginBackupFile):
                repository.open_plugin_backup("other.zip")
            with self.assertRaises(InvalidPluginBackupFile):
                repository.open_plugin_backup("../backup_20261006_010203_v2.0.0.zip")
            with self.assertRaises(InvalidPluginBackupFile):
                repository.open_plugin_backup(r"backup_20261006_010203_v2.0.0.zip\..")
            with self.assertRaises(PluginBackupFileNotFound):
                repository.open_plugin_backup("backup_20261006_010204_v2.0.0.zip")

    def test_repository_rejects_symlinks_without_following_or_deleting_target(self) -> None:
        with tempfile.TemporaryDirectory(prefix="plugin-backup-files-") as directory:
            root = Path(directory)
            target = root / "backup_20261006_010203_v2.0.0.zip"
            link = root / "backup_20261006_010204_v2.0.0.zip"
            target.write_bytes(b"keep")
            link.symlink_to(target)
            repository = PluginBackupFileRepository(root)

            with self.assertRaises(InvalidPluginBackupFile):
                repository.open_plugin_backup(link.name)
            with self.assertRaises(InvalidPluginBackupFile):
                repository.delete_plugin_backup(link.name)

            self.assertTrue(link.is_symlink())
            self.assertEqual(target.read_bytes(), b"keep")

    def test_repository_deletes_only_catalogued_regular_archives(self) -> None:
        with tempfile.TemporaryDirectory(prefix="plugin-backup-files-") as directory:
            root = Path(directory)
            archive = root / "backup_20261006_010203_v2.0.0.zip"
            archive.write_bytes(b"zip")
            repository = PluginBackupFileRepository(root)

            repository.delete_plugin_backup(archive.name)

            self.assertFalse(archive.exists())
            with self.assertRaises(PluginBackupFileNotFound):
                repository.delete_plugin_backup(archive.name)

    def test_repository_does_not_create_missing_backup_directory(self) -> None:
        with tempfile.TemporaryDirectory(prefix="plugin-backup-files-") as directory:
            missing = Path(directory) / "missing"
            repository = PluginBackupFileRepository(missing)

            with self.assertRaises(PluginBackupFileNotFound):
                repository.open_plugin_backup("backup_20261006_010203_v2.0.0.zip")
            with self.assertRaises(PluginBackupFileNotFound):
                repository.delete_plugin_backup("backup_20261006_010203_v2.0.0.zip")

            self.assertFalse(missing.exists())

    def test_application_batch_delete_reports_partial_results(self) -> None:
        repository = Mock()
        repository.delete_plugin_backup.side_effect = [
            None,
            PluginBackupFileNotFound("missing.zip"),
            InvalidPluginBackupFile("../outside.zip"),
            PermissionError("denied"),
        ]
        application = PluginBackupFileApplication(repository)

        deleted, failed = application.delete_plugin_backups(
            [
                "backup_20261006_010203_v2.0.0.zip",
                "missing.zip",
                "../outside.zip",
                "backup_20261006_010205_v2.0.0.zip",
            ]
        )

        self.assertEqual(deleted, ["backup_20261006_010203_v2.0.0.zip"])
        self.assertEqual(
            failed,
            [
                {"filename": "missing.zip", "reason": "文件不存在"},
                {"filename": "../outside.zip", "reason": "无效文件名"},
                {"filename": "backup_20261006_010205_v2.0.0.zip", "reason": "删除失败"},
            ],
        )
        self.assertEqual(repository.delete_plugin_backup.call_count, 4)


if __name__ == "__main__":
    unittest.main()
