from __future__ import annotations

import os
import tempfile
import unittest
import zipfile
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.plugin_backups import (
    InvalidPluginBackupArchive,
    PluginBackupRestoreApplication,
    PluginBackupRestoreRepository,
)
from nonebot_plugin_xiuxian_2.features.plugin_backups import restore_repository


PLUGIN_ROOT = "src/plugins/nonebot_plugin_xiuxian_2"
ARCHIVE_NAME = "backup_20261006_010203_v2.0.0.zip"


class FakeRestoreRuntime:
    def __init__(self) -> None:
        self.restored: list[tuple[str, Path, Path]] = []
        self.reloaded: list[list[str]] = []
        self.fail_database_restore = False

    def plugin_backup_sqlite_database_names(self) -> list[str]:
        return ["xiuxian.db", "player.db"]

    def restore_plugin_backup_database(
        self, source: Path, target: Path, database_name: str
    ) -> None:
        self.restored.append((database_name, source, target))
        if self.fail_database_restore:
            raise RuntimeError("database restore failed")
        target.write_bytes(source.read_bytes())

    def after_plugin_backup_restore(self, database_names: list[str]) -> None:
        self.reloaded.append(database_names)


class PluginBackupRestoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="plugin-backup-restore-test-")
        self.root = Path(self.temp.name)
        self.backups = self.root / "backups"
        self.backups.mkdir()
        self.data_root = self.root / "configured-data"
        self.plugin_root = self.root / "configured-plugin"
        self.stage_root = self.root / "staging"
        self.stage_root.mkdir()
        self.version_file = self.data_root / "xiuxian" / "version.txt"
        self.runtime = FakeRestoreRuntime()
        self.repository = PluginBackupRestoreRepository(
            self.backups,
            self.data_root,
            self.plugin_root,
            temporary_directory=self.stage_root,
        )
        self.application = PluginBackupRestoreApplication(
            self.repository,
            self.runtime,
            self.version_file,
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_archive(self, filename: str = ARCHIVE_NAME, *, include_database=True) -> Path:
        archive_path = self.backups / filename
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("data/xiuxian/state.json", b'{"state":"restored"}')
            if include_database:
                archive.writestr("data/xiuxian/player.db", b"sqlite-snapshot")
                archive.writestr("data/xiuxian/player.db-wal", b"stale-sidecar")
            archive.writestr(f"{PLUGIN_ROOT}/entry.py", b"restored plugin")
        return archive_path

    def test_restore_overlays_configured_roots_and_reloads_restored_databases(self) -> None:
        self._write_archive()
        data_dir = self.data_root / "xiuxian"
        data_dir.mkdir(parents=True)
        (data_dir / "extra.json").write_text("keep", encoding="utf-8")
        plugin_file = self.plugin_root / "entry.py"
        plugin_file.parent.mkdir(parents=True)
        plugin_file.write_bytes(b"old plugin")

        success, message = self.application.restore_backup(ARCHIVE_NAME)

        self.assertTrue(success)
        self.assertEqual(message, f"成功从备份 {ARCHIVE_NAME} 恢复")
        self.assertEqual((data_dir / "state.json").read_bytes(), b'{"state":"restored"}')
        self.assertEqual((data_dir / "extra.json").read_text(encoding="utf-8"), "keep")
        self.assertEqual((data_dir / "player.db").read_bytes(), b"sqlite-snapshot")
        self.assertFalse((data_dir / "player.db-wal").exists())
        self.assertEqual(plugin_file.read_bytes(), b"restored plugin")
        self.assertEqual(self.version_file.read_text(encoding="utf-8"), "v2.0.0")
        self.assertEqual([name for name, _, _ in self.runtime.restored], ["player.db"])
        self.assertEqual(self.runtime.reloaded, [["player.db"]])
        self.assertEqual(list(self.stage_root.iterdir()), [])

    def test_invalid_member_path_is_rejected_before_any_target_is_written(self) -> None:
        archive_path = self.backups / ARCHIVE_NAME
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("data/xiuxian/state.json", b"must not be applied")
            archive.writestr("data/../../outside.txt", b"escape")

        success, message = self.application.restore_backup(ARCHIVE_NAME)

        self.assertFalse(success)
        self.assertIn("压缩包成员路径非法", message)
        self.assertFalse((self.data_root / "xiuxian" / "state.json").exists())
        self.assertFalse((self.root / "outside.txt").exists())
        self.assertFalse(self.version_file.exists())
        self.assertEqual(list(self.stage_root.iterdir()), [])

    def test_archive_must_contain_supported_payload_and_be_a_valid_zip(self) -> None:
        unsupported = self.backups / "other.zip"
        with zipfile.ZipFile(unsupported, "w") as archive:
            archive.writestr("elsewhere/file.txt", b"no")
        corrupt = self.backups / "broken.zip"
        corrupt.write_bytes(b"not a zip")

        for filename in (unsupported.name, corrupt.name):
            with self.subTest(filename=filename):
                success, message = self.application.restore_backup(filename)
                self.assertFalse(success)
                self.assertIn("恢复备份失败", message)
        self.assertFalse(self.version_file.exists())
        self.assertEqual(list(self.stage_root.iterdir()), [])

    def test_member_count_limit_is_checked_before_extracting(self) -> None:
        self._write_archive(include_database=False)
        with patch.object(restore_repository, "MAX_ARCHIVE_MEMBERS", 1):
            success, message = self.application.restore_backup(ARCHIVE_NAME)

        self.assertFalse(success)
        self.assertIn("成员数量超过限制", message)
        self.assertFalse((self.data_root / "xiuxian" / "state.json").exists())

    def test_disk_preflight_accounts_for_staging_and_target_copies(self) -> None:
        info = zipfile.ZipInfo("data/xiuxian/state.bin")
        info.file_size = 40 * 1024 * 1024
        with patch.object(
            restore_repository.shutil,
            "disk_usage",
            return_value=SimpleNamespace(free=128 * 1024 * 1024),
        ):
            with self.assertRaisesRegex(InvalidPluginBackupArchive, "磁盘空间不足"):
                self.repository._validate_disk_capacity(
                    [(info, info.filename)],
                    info.file_size,
                    self.stage_root,
                    set(),
                )


    def test_existing_symlink_in_target_tree_is_rejected(self) -> None:
        self._write_archive(include_database=False)
        outside = self.root / "outside"
        outside.mkdir()
        self.data_root.mkdir()
        os.symlink(outside, self.data_root / "xiuxian")

        success, message = self.application.restore_backup(ARCHIVE_NAME)

        self.assertFalse(success)
        self.assertIn("符号链接", message)
        self.assertEqual(list(outside.iterdir()), [])
        self.assertFalse(self.version_file.exists())

    def test_restore_failure_does_not_update_version(self) -> None:
        self._write_archive()
        self.runtime.fail_database_restore = True

        success, message = self.application.restore_backup(ARCHIVE_NAME)

        self.assertFalse(success)
        self.assertIn("database restore failed", message)
        self.assertFalse(self.version_file.exists())
        self.assertEqual(list(self.stage_root.iterdir()), [])

    def test_missing_archive_and_symlink_archive_fail_closed(self) -> None:
        success, message = self.application.restore_backup("missing.zip")
        self.assertFalse(success)
        self.assertIn("备份文件不存在", message)

        target = self.root / "external.zip"
        target.write_bytes(b"not read")
        link = self.backups / "linked.zip"
        os.symlink(target, link)
        self.assertTrue(self.application.local_backup_exists(link.name))
        success, message = self.application.restore_backup(link.name)
        self.assertFalse(success)
        self.assertIn("普通文件", message)


if __name__ == "__main__":
    unittest.main()
