from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from ..application import UpdateApplication, is_valid_release_tag


class FakeUpdateProvider:
    def __init__(self, directory: str) -> None:
        self.archive = Path(directory) / "release.tar.gz"
        self.config_backup = Path(directory) / "config.json"
        self.calls: list[tuple[object, ...]] = []
        self.releases = [{"tag_name": "v2.0.0", "name": "Version 2"}]
        self.asset: dict[str, object] = {
            "name": "project.tar.gz",
            "browser_download_url": "https://releases.invalid/project.tar.gz",
        }
        self.prepare_result: tuple[bool, object] = (True, self.asset)
        self.download_result: tuple[bool, object] = (True, self.archive)
        self.extract_result: tuple[bool, str] = (True, "更新成功")
        self.restore_result: tuple[bool, str] = (True, "配置恢复成功")
        self.backup_results: dict[str, tuple[bool, object]] = {
            "plugin": (True, Path(directory) / "plugin.zip"),
            "database": (True, "db backup"),
            "config": (True, self.config_backup),
        }

    def get_current_version(self) -> str:
        self.calls.append(("current_version",))
        return "v1.0.0"

    def get_latest_releases(self, count: int):
        self.calls.append(("latest_releases", count))
        return self.releases[:count]

    def prepare_release_asset(self, tag: str):
        self.calls.append(("prepare", tag))
        return self.prepare_result

    def enhanced_backup_current_version(self):
        self.calls.append(("plugin_backup",))
        return self.backup_results["plugin"]

    def backup_db_files(self):
        self.calls.append(("database_backup",))
        return self.backup_results["database"]

    def backup_all_configs(self):
        self.calls.append(("config_backup",))
        return self.backup_results["config"]

    def download_release(self, tag: str, *, target_asset):
        self.calls.append(("download", tag, target_asset))
        return self.download_result

    def extract_update(self, archive: Path, backup: bool = False, *, release_tag: str):
        self.calls.append(("extract", archive, backup, release_tag))
        return self.extract_result

    def restore_config_from_backup(self, path: Path):
        self.calls.append(("restore_config", path))
        return self.restore_result

    def cleanup_download(self, path: Path) -> None:
        self.calls.append(("cleanup", path))


class UpdateApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="updater-application-")
        self.provider = FakeUpdateProvider(self.temp.name)
        self.application = UpdateApplication(self.provider)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_release_tag_validation(self) -> None:
        for valid in ("v1.2.3", "release_2026+1", "2.0.0-rc.1", "A"):
            with self.subTest(value=valid):
                self.assertTrue(is_valid_release_tag(valid))

        for invalid in (
            None,
            3,
            "",
            ".hidden",
            "a/b",
            "a\\b",
            "a b",
            "a" * 129,
        ):
            with self.subTest(value=invalid):
                self.assertFalse(is_valid_release_tag(invalid))

    def test_read_methods_delegate_and_check_update_decides(self) -> None:
        self.assertEqual(self.application.current_version(), "v1.0.0")
        self.assertEqual(self.application.latest_releases(2), self.provider.releases)
        release, message = self.application.check_update()
        self.assertEqual(release, self.provider.releases[0])
        self.assertIn("发现新版本 v2.0.0", message)
        self.assertEqual(
            [call for call in self.provider.calls if call[0] == "latest_releases"],
            [("latest_releases", 2), ("latest_releases", 1)],
        )

    def test_check_update_handles_empty_release_list_and_current_release(self) -> None:
        self.provider.releases = []
        self.assertEqual(self.application.check_update(), (None, "无法获取更新信息"))

        self.provider.releases = [{"tag_name": "v1.0.0"}]
        self.assertEqual(self.application.check_update(), (None, "当前已是最新版本"))

    def test_invalid_tag_does_not_preflight_or_create_backups(self) -> None:
        result = self.application.perform_update_with_backup("../v2.0.0")
        self.assertFalse(result[0])
        self.assertIn("无效", result[1])
        self.assertEqual(self.provider.calls, [])

    def test_failed_release_preflight_stops_before_backups(self) -> None:
        self.provider.prepare_result = (False, "release not found")
        result = self.application.perform_update_with_backup("v2.0.0")
        self.assertEqual(result, (False, "release not found"))
        self.assertEqual(self.provider.calls, [("prepare", "v2.0.0")])

    def test_wrong_asset_is_rejected_before_backups(self) -> None:
        self.provider.prepare_result = (True, {"name": "other.tar.gz"})
        result = self.application.perform_update_with_backup("v2.0.0")
        self.assertFalse(result[0])
        self.assertIn("project.tar.gz", result[1])
        self.assertEqual(self.provider.calls, [("prepare", "v2.0.0")])

    def test_backups_run_in_order_and_stop_at_first_failure(self) -> None:
        self.provider.backup_results["database"] = (False, "database unavailable")
        result = self.application.perform_update_with_backup("v2.0.0")
        self.assertEqual(result, (False, "数据库备份失败: database unavailable"))
        self.assertEqual(
            [call[0] for call in self.provider.calls],
            ["prepare", "plugin_backup", "database_backup"],
        )

    def test_update_passes_verified_asset_and_exact_tag_and_cleans_archive(self) -> None:
        result = self.application.perform_update_with_backup("v2.0.0")
        self.assertEqual(result, (True, "更新成功"))
        self.assertEqual(
            [call[0] for call in self.provider.calls],
            [
                "prepare",
                "plugin_backup",
                "database_backup",
                "config_backup",
                "download",
                "extract",
                "restore_config",
                "cleanup",
            ],
        )
        self.assertEqual(self.provider.calls[4], ("download", "v2.0.0", self.provider.asset))
        self.assertEqual(
            self.provider.calls[5],
            ("extract", self.provider.archive, False, "v2.0.0"),
        )
        self.assertEqual(self.provider.calls[-1], ("cleanup", self.provider.archive))

    def test_restore_failure_warns_but_keeps_update_success(self) -> None:
        self.provider.restore_result = (False, "config write failed")
        with self.assertLogs("nonebot_plugin_xiuxian_2.features.updater.application", level="WARNING"):
            result = self.application.perform_update_with_backup("v2.0.0")
        self.assertEqual(result, (True, "更新成功"))
        self.assertEqual(self.provider.calls[-1], ("cleanup", self.provider.archive))

    def test_extract_failure_still_cleans_archive(self) -> None:
        self.provider.extract_result = (False, "invalid archive")
        self.assertEqual(
            self.application.perform_update_with_backup("v2.0.0"),
            (False, "invalid archive"),
        )
        self.assertEqual(self.provider.calls[-1], ("cleanup", self.provider.archive))

    def test_lock_is_non_blocking_and_shared_across_instances(self) -> None:
        other_provider = FakeUpdateProvider(self.temp.name)
        other = UpdateApplication(other_provider)
        self.assertTrue(UpdateApplication._update_lock.acquire(blocking=False))
        try:
            result = other.perform_update_with_backup("v2.0.0")
        finally:
            UpdateApplication._update_lock.release()

        self.assertEqual(result, (False, "已有更新任务正在执行"))
        self.assertEqual(other_provider.calls, [])

    def test_provider_exception_releases_lock(self) -> None:
        self.provider.enhanced_backup_current_version = Mock(side_effect=RuntimeError("disk error"))
        result = self.application.perform_update_with_backup("v2.0.0")
        self.assertEqual(result, (False, "更新过程中出现错误: disk error"))
        self.assertTrue(UpdateApplication._update_lock.acquire(blocking=False))
        UpdateApplication._update_lock.release()


if __name__ == "__main__":
    unittest.main()
