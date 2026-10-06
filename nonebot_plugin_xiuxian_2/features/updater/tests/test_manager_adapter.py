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


if __name__ == "__main__":
    unittest.main()
