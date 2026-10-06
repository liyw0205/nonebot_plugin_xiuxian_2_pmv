from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from ..application import ConfigBackupApplication
from ..repository import ConfigBackupRepository


class Runtime:
    def __init__(self) -> None:
        self.paths = {
            "base_url": "https://dav.invalid/root",
            "config_rel": "backups/config_backups",
            "config_url": "https://dav.invalid/root/backups/config_backups",
            "auth": ("user", "pass"),
        }
        self.values = {"cloud_backup_enabled": True, "webdav_pass": "secret", "debug": False}
        self.now = datetime(2026, 10, 7, 2, 3, 4, tzinfo=timezone.utc)
        self.cleanup_calls: list[str] = []
        self.saved_values: list[dict[str, object]] = []

    def configuration_backup_webdav_paths(self):
        return True, "ok", self.paths

    def configuration_backup_webdav_join_url(self, base_url, relative_path):
        return f"{base_url.rstrip('/')}/{relative_path}"

    def configuration_backup_webdav_make_directories(self, base_url, relative_path, auth):
        return True, "ok"

    def configuration_backup_format_time(self, value):
        return f"formatted:{value}"

    def configuration_backup_values(self):
        return dict(self.values)

    def configuration_backup_version(self):
        return "v2.0.0"

    def configuration_backup_now(self):
        return self.now

    def configuration_backup_cloud_enabled(self):
        return True

    def configuration_backup_keep_days(self):
        return 10

    def configuration_backup_cleanup_cloud(self):
        self.cleanup_calls.append("cloud")
        return True, "clean"

    def configuration_backup_save_values(self, values):
        self.saved_values.append(values)
        return True, "saved"


class ConfigBackupApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="config-backup-application-")
        self.addCleanup(self.temporary.cleanup)
        self.runtime = Runtime()
        self.directory = Path(self.temporary.name) / "backups" / "config_backups"
        self.repository = ConfigBackupRepository(self.directory, self.runtime)
        self.application = ConfigBackupApplication(self.repository, self.runtime)

    def test_export_and_local_backup_keep_allowlisted_fields_and_metadata(self) -> None:
        exported, export_name = self.application.export_config(["debug"])
        self.assertEqual(export_name, "xiuxian_config_export_20261007_020304.json")
        self.assertEqual(exported["debug"], False)
        self.assertNotIn("webdav_pass", {key: value for key, value in exported.items() if key != "_metadata"})
        self.assertEqual(exported["_metadata"]["backup_fields"], ["debug"])

        path = self.application.create_local_backup(["debug"])
        self.assertEqual(path.name, "config_backup_20261007_020304.json")
        data, metadata = self.repository.read_local_backup(path.name)
        self.assertEqual(data, {"debug": False})
        self.assertEqual(metadata["version"], "v2.0.0")
        self.assertEqual(metadata["backup_fields"], ["debug"])

    def test_import_and_restore_routes_only_stage_values_without_writing_config(self) -> None:
        from io import BytesIO

        self.assertEqual(
            self.application.import_config(
                "upload.json",
                BytesIO(b'{"debug":true,"_metadata":{"version":"v1"}}'),
            ),
            {"debug": True},
        )
        path = self.repository.create_local_backup(
            "config_backup_20261007_010101.json",
            {"debug": True, "_metadata": {"version": "v1"}},
        )
        restored, result = self.application.restore_local_backup(path.name)
        self.assertTrue(restored)
        self.assertEqual(result["data"], {"debug": True})
        self.assertEqual(result["metadata"], {"version": "v1"})
        self.assertEqual(self.runtime.saved_values, [])

    def test_full_backup_uploads_once_and_preserves_auto_cloud_cleanup(self) -> None:
        uploaded: list[str] = []
        self.repository.upload_cloud_backup = lambda filename: uploaded.append(filename) or (True, "ok")

        success, path = self.application.backup_all_configs()

        self.assertTrue(success)
        self.assertIsInstance(path, Path)
        self.assertEqual(uploaded, ["config_backup_20261007_020304.json"])
        self.assertEqual(self.runtime.cleanup_calls, ["cloud"])
        data, metadata = self.repository.read_local_backup(path.name)
        self.assertEqual(data, self.runtime.values)
        self.assertEqual(metadata["type"], "config_backup")
        self.assertEqual(metadata["backup_type"], "full")

    def test_full_backup_can_defer_cloud_cleanup_for_manual_orchestration(self) -> None:
        uploaded: list[str] = []
        self.repository.upload_cloud_backup = lambda filename: uploaded.append(filename) or (True, "ok")

        success, path, cloud_uploaded = self.application.backup_all_configs_with_details(
            defer_cloud_cleanup=True
        )

        self.assertTrue(success)
        self.assertIsInstance(path, Path)
        self.assertTrue(cloud_uploaded)
        self.assertEqual(uploaded, ["config_backup_20261007_020304.json"])
        self.assertEqual(self.runtime.cleanup_calls, [])

    def test_manual_cloud_backup_uploads_only_once_when_auto_cloud_is_enabled(self) -> None:
        uploaded: list[str] = []
        self.repository.upload_cloud_backup = lambda filename: uploaded.append(filename) or (True, "ok")

        success, path = self.application.backup_cloud_config()

        self.assertTrue(success)
        self.assertEqual(Path(path).name, "config_backup_20261007_020304.json")
        self.assertEqual(uploaded, ["config_backup_20261007_020304.json"])
        self.assertEqual(self.runtime.cleanup_calls, ["cloud"])

    def test_manual_cloud_backup_keeps_local_archive_when_upload_fails(self) -> None:
        self.repository.upload_cloud_backup = Mock(return_value=(False, "offline"))

        success, result = self.application.backup_cloud_config()

        self.assertFalse(success)
        self.assertEqual(result, "offline")
        self.assertTrue((self.directory / "config_backup_20261007_020304.json").is_file())
        self.assertEqual(self.runtime.cleanup_calls, [])

    def test_cloud_restore_prefers_local_backup_and_only_fetches_when_missing(self) -> None:
        filename = "config_backup_20261007_020304.json"
        local_path = self.repository.create_local_backup(
            filename, {"debug": True, "_metadata": {"source": "local"}}
        )
        download = Mock(side_effect=AssertionError("unexpected download"))
        self.repository.download_cloud_backup = download

        success, result = self.application.restore_cloud_backup(filename)

        self.assertTrue(success)
        self.assertEqual(result["data"], {"debug": True})
        self.assertEqual(result["metadata"], {"source": "local"})
        self.assertEqual(result["local_path"], str(local_path))
        download.assert_not_called()


if __name__ == "__main__":
    unittest.main()
