from __future__ import annotations

import tempfile
import tarfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ....xiuxian.xiuxian_utils import download_xiuxian_data as updater_module


class UpdateManagerAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manager = updater_module.UpdateManager.__new__(updater_module.UpdateManager)
        self.manager.repo_owner = "owner"
        self.manager.repo_name = "repo"
        self.manager.api_url = "https://api.github.com/repos/owner/repo/releases"
        self.manager.current_version = "v1.0.0"

    def test_release_preflight_requires_requested_official_asset(self) -> None:
        response = Mock()
        response.json.return_value = {
            "tag_name": "v2.0.0",
            "assets": [
                {
                    "name": "project.tar.gz",
                    "browser_download_url": (
                        "https://github.com/owner/repo/releases/download/v2.0.0/project.tar.gz"
                    ),
                }
            ],
        }
        with patch.object(updater_module.requests, "get", return_value=response) as get:
            ok, asset = self.manager.prepare_release_asset("v2.0.0")

        self.assertTrue(ok)
        self.assertEqual(asset["name"], "project.tar.gz")
        get.assert_called_once_with(
            "https://api.github.com/repos/owner/repo/releases/tags/v2.0.0", timeout=10
        )

    def test_release_preflight_rejects_non_official_asset(self) -> None:
        response = Mock()
        response.json.return_value = {
            "tag_name": "v2.0.0",
            "assets": [
                {"name": "project.tar.gz", "browser_download_url": "https://evil.invalid/file"}
            ],
        }
        with patch.object(updater_module.requests, "get", return_value=response):
            ok, message = self.manager.prepare_release_asset("v2.0.0")

        self.assertFalse(ok)
        self.assertIn("不受信任", message)

    def test_failed_download_removes_temporary_directory(self) -> None:
        with tempfile.TemporaryDirectory(prefix="updater-download-") as directory:
            download_dir = Path(directory) / "download"
            download_dir.mkdir()
            self.manager.get_proxy_list = lambda: []
            self.manager.test_proxies = lambda proxies, url: []
            asset = {
                "name": "project.tar.gz",
                "browser_download_url": (
                    "https://github.com/owner/repo/releases/download/v2.0.0/project.tar.gz"
                ),
            }
            with (
                patch.object(updater_module.tempfile, "mkdtemp", return_value=str(download_dir)),
                patch.object(updater_module.wget, "download", side_effect=RuntimeError("network error")),
            ):
                ok, message = self.manager.download_release("v2.0.0", target_asset=asset)

            self.assertFalse(ok)
            self.assertIn("下载失败", message)
            self.assertFalse(download_dir.exists())

    def test_invalid_archive_does_not_write_version_or_create_target(self) -> None:
        with tempfile.TemporaryDirectory(prefix="updater-extract-") as directory:
            root = Path(directory)
            archive = root / "release.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                pass
            data_root = root / "data"
            plugin_root = root / "plugin"
            with (
                patch.object(updater_module, "get_paths", return_value=SimpleNamespace(data_root=data_root)),
                patch.object(updater_module, "Xiu_Plugin", plugin_root),
            ):
                updated, message = self.manager.extract_update(
                    archive, backup=False, release_tag="v2.0.0"
                )

            self.assertFalse(updated)
            self.assertIn("data目录", message)
            self.assertFalse(data_root.exists())
            self.assertFalse(plugin_root.exists())

    def test_version_marker_records_requested_tag(self) -> None:
        with tempfile.TemporaryDirectory(prefix="updater-version-") as directory:
            data_dir = Path(directory) / "data"
            with patch.object(updater_module, "get_paths", return_value=SimpleNamespace(data=data_dir)):
                self.manager.update_version_file("v2.0.0")

            self.assertEqual((data_dir / "version.txt").read_text(encoding="utf-8"), "v2.0.0")
            self.assertEqual(self.manager.current_version, "v2.0.0")

    def test_plugin_backup_sqlite_restore_stages_on_destination_filesystem(self) -> None:
        with tempfile.TemporaryDirectory(prefix="updater-sqlite-restore-") as directory:
            root = Path(directory)
            source = root / "staged" / "player.db"
            source.parent.mkdir()
            source.write_bytes(b"archive snapshot")
            target = root / "data" / "xiuxian" / "player.db"
            observations: list[Path] = []

            def snapshot(source_path: Path, clean_path: Path):
                observations.append(clean_path.parent.parent)
                clean_path.write_bytes(source_path.read_bytes())
                return True, ""

            self.manager._snapshot_sqlite_db = snapshot
            self.manager._backup_current_db_before_restore = Mock()
            self.manager._close_database_handles = Mock()
            self.manager._remove_sqlite_sidecars = Mock()

            self.manager.restore_plugin_backup_database(source, target, "player.db")

            self.assertEqual(observations, [target.parent])
            self.assertEqual(target.read_bytes(), b"archive snapshot")
            self.manager._backup_current_db_before_restore.assert_called_once_with(
                target, "player.db"
            )

    def test_plugin_backup_cloud_compatibility_methods_delegate_to_feature_owner(self) -> None:
        cloud_application = Mock()
        cloud_application.list_cloud_backups.return_value = (True, [])
        cloud_application.sync_cloud_backup.return_value = (True, Path("backup.zip"))
        cloud_application.delete_cloud_backup.return_value = (True, "deleted")

        with patch(
            "nonebot_plugin_xiuxian_2.features.plugin_backups.build_plugin_backup_cloud_application",
            return_value=cloud_application,
        ):
            self.assertEqual(self.manager.list_webdav_backups(), (True, []))
            self.assertEqual(
                self.manager.download_from_webdav("backup.zip"),
                (True, Path("backup.zip")),
            )
            self.assertEqual(self.manager.delete_webdav_backup("backup.zip"), (True, "deleted"))

        cloud_application.list_cloud_backups.assert_called_once_with()
        cloud_application.sync_cloud_backup.assert_called_once_with(
            "backup.zip", overwrite=True
        )
        cloud_application.delete_cloud_backup.assert_called_once_with("backup.zip")

    def test_plugin_backup_cloud_runtime_port_keeps_existing_webdav_configuration(self) -> None:
        paths = (True, "ok", {"plugin_rel": "backups/plugin"})
        self.manager._get_webdav_paths = Mock(return_value=paths)
        self.manager._webdav_join_url = Mock(return_value="https://dav.invalid/a.zip")
        self.manager._gmt_to_cst_str = Mock(return_value="2026-10-06 09:00:00")

        self.assertEqual(self.manager.plugin_backup_webdav_paths(), paths)
        self.assertEqual(
            self.manager.plugin_backup_webdav_join_url(
                "https://dav.invalid", "backups/plugin/a.zip"
            ),
            "https://dav.invalid/a.zip",
        )
        self.assertEqual(
            self.manager.plugin_backup_webdav_format_time("raw"),
            "2026-10-06 09:00:00",
        )
        self.manager._webdav_join_url.assert_called_once_with(
            "https://dav.invalid", "backups/plugin/a.zip"
        )
        self.manager._gmt_to_cst_str.assert_called_once_with("raw")


if __name__ == "__main__":
    unittest.main()
