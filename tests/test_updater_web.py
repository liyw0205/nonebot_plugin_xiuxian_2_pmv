from __future__ import annotations

import unittest
from io import BytesIO
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.plugin_backups import (
    InvalidPluginBackupFile,
    PluginBackupFileNotFound,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app, backups, core, pages


class FakeUpdateApplication:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def current_version(self) -> str:
        return "v1.0.0"

    def check_update(self):
        self.calls.append(("check_update",))
        return (
            {"tag_name": "v2.0.0", "name": "Release", "published_at": "date", "body": "notes"},
            "new release",
        )

    def latest_releases(self, count: int):
        self.calls.append(("latest_releases", count))
        return [{"tag_name": "v2.0.0", "name": "Release"}]

    def perform_update_with_backup(self, release_tag: object):
        self.calls.append(("perform_update", release_tag))
        return True, "updated"


class FakeBackupCatalogApplication:
    def __init__(self) -> None:
        self.calls = 0

    def list_plugin_backups(self):
        self.calls += 1
        return [
            {
                "filename": "backup_20261006_010203_v2.0.0.zip",
                "timestamp": "20261006_010203",
                "version": "v2.0.0",
                "size": 3,
                "created_at": "2026-10-06T01:02:03",
            }
        ]


class FakePluginBackupFileApplication:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def open_plugin_backup(self, filename: str):
        self.calls.append(("open", filename))
        return BytesIO(b"zip-bytes")

    def delete_plugin_backup(self, filename: str) -> None:
        self.calls.append(("delete", filename))

    def delete_plugin_backups(self, filenames: list[object]):
        self.calls.append(("delete_many", filenames))
        return ["backup_20261006_010203_v2.0.0.zip"], [
            {"filename": "missing.zip", "reason": "文件不存在"}
        ]


class FakePluginBackupRestoreApplication:
    def __init__(self, local_exists: bool = True, result=(True, "restored")) -> None:
        self.local_exists = local_exists
        self.result = result
        self.calls: list[tuple[object, ...]] = []

    def local_backup_exists(self, filename: str) -> bool:
        self.calls.append(("exists", filename))
        return self.local_exists

    def restore_backup(self, filename: str):
        self.calls.append(("restore", filename))
        return self.result


class FakeWebDavUpdateManager:
    def __init__(self, result=(True, "downloaded")) -> None:
        self.result = result
        self.calls: list[tuple[object, ...]] = []

    def download_from_webdav(self, filename: str):
        self.calls.append(("download", filename))
        return self.result


class UpdaterWebRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        self.client = app.test_client()
        self.application = FakeUpdateApplication()

    def _login_session(self) -> None:
        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"

    def test_update_routes_require_admin_and_keep_update_permission(self) -> None:
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            response = self.client.get("/get_releases")
            self.assertEqual(response.status_code, 401)
            self._login_session()
            with patch.object(pages, "update_application", self.application):
                check = self.client.get("/check_update")
                releases = self.client.get("/get_releases")
                missing_csrf = self.client.post(
                    "/perform_update", json={"release_tag": "v2.0.0"}
                )
                update = self.client.post(
                    "/perform_update",
                    json={"release_tag": "v2.0.0"},
                    headers={"X-CSRF-Token": "csrf-token"},
                )

        self.assertEqual(check.status_code, 200)
        self.assertEqual(check.get_json()["latest_version"], "v2.0.0")
        self.assertEqual(releases.get_json()["releases"][0]["tag_name"], "v2.0.0")
        self.assertEqual(missing_csrf.status_code, 403)
        self.assertEqual(update.get_json(), {"success": True, "message": "updated"})
        self.assertEqual(
            self.application.calls,
            [("check_update",), ("latest_releases", 10), ("perform_update", "v2.0.0")],
        )

    def test_update_page_does_not_interpolate_release_metadata_as_html(self) -> None:
        with app.test_request_context("/update"):
            source = pages.render_template("update.html")
        self.assertNotIn("showChangelog('${data.latest_version}'", source)
        self.assertNotIn("onclick=\"performUpdate('${release.tag_name}')\"", source)
        self.assertIn("changelog.textContent = text", source)
        self.assertIn("button.addEventListener('click', () => performUpdate(release.tag_name))", source)

    def test_get_backups_route_requires_admin_and_keeps_legacy_payload(self) -> None:
        catalog = FakeBackupCatalogApplication()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            anonymous = self.client.get("/get_backups")
            self._login_session()
            with patch.object(pages, "backup_catalog_application", catalog):
                response = self.client.get("/get_backups")

        self.assertEqual(anonymous.status_code, 401)
        self.assertEqual(
            response.get_json(),
            {
                "success": True,
                "backups": [
                    {
                        "filename": "backup_20261006_010203_v2.0.0.zip",
                        "timestamp": "20261006_010203",
                        "version": "v2.0.0",
                        "size": 3,
                        "created_at": "2026-10-06T01:02:03",
                    }
                ],
            },
        )
        self.assertNotIn("path", response.get_json()["backups"][0])
        self.assertEqual(catalog.calls, 1)

    def test_plugin_backup_file_routes_keep_auth_csrf_and_response_contracts(self) -> None:
        application = FakePluginBackupFileApplication()
        archive_name = "backup_20261006_010203_v2.0.0.zip"
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            anonymous_download = self.client.get(f"/download_backup/{archive_name}")
            anonymous_delete = self.client.post(
                "/delete_backup", json={"backup_filename": archive_name}
            )
            anonymous_batch = self.client.post(
                "/batch_delete_backups", json={"filenames": [archive_name]}
            )
            self._login_session()
            with patch.object(backups, "plugin_backup_file_application", application):
                missing_csrf = self.client.post(
                    "/delete_backup", json={"backup_filename": archive_name}
                )
                missing_batch_csrf = self.client.post(
                    "/batch_delete_backups", json={"filenames": [archive_name]}
                )
                download = self.client.get(f"/download_backup/{archive_name}")
                deleted = self.client.post(
                    "/delete_backup",
                    json={"backup_filename": archive_name},
                    headers={"X-CSRF-Token": "csrf-token"},
                )
                batch = self.client.post(
                    "/batch_delete_backups",
                    json={"filenames": [archive_name, "missing.zip"]},
                    headers={"X-CSRF-Token": "csrf-token"},
                )

        self.assertEqual(anonymous_download.status_code, 401)
        self.assertEqual(anonymous_delete.status_code, 401)
        self.assertEqual(anonymous_batch.status_code, 401)
        self.assertEqual(missing_csrf.status_code, 403)
        self.assertEqual(missing_batch_csrf.status_code, 403)
        self.assertEqual(download.status_code, 200)
        self.assertEqual(download.data, b"zip-bytes")
        self.assertIn(archive_name, download.headers["Content-Disposition"])
        self.assertEqual(
            deleted.get_json(),
            {"success": True, "message": f"备份文件 {archive_name} 删除成功"},
        )
        self.assertEqual(
            batch.get_json(),
            {
                "success": True,
                "message": "批量删除完成，成功 1 个，失败 1 个",
                "deleted": [archive_name],
                "failed": [{"filename": "missing.zip", "reason": "文件不存在"}],
            },
        )
        self.assertEqual(
            application.calls,
            [
                ("open", archive_name),
                ("delete", archive_name),
                ("delete_many", [archive_name, "missing.zip"]),
            ],
        )

    def test_plugin_backup_download_rejects_invalid_and_missing_archives(self) -> None:
        class FileApplication:
            def open_plugin_backup(self, filename: str):
                if filename.startswith("backup_"):
                    raise PluginBackupFileNotFound(filename)
                raise InvalidPluginBackupFile(filename)

        self._login_session()
        with patch.object(backups, "plugin_backup_file_application", FileApplication()):
            missing = self.client.get(
                "/download_backup/backup_20261006_010203_v2.0.0.zip"
            )
            invalid = self.client.get("/download_backup/not-a-backup.zip")

        self.assertEqual(missing.status_code, 404)
        self.assertEqual(invalid.status_code, 400)

    def test_plugin_backup_restore_routes_keep_admin_csrf_and_local_contract(self) -> None:
        application = FakePluginBackupRestoreApplication()
        archive_name = "backup_20261006_010203_v2.0.0.zip"
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            anonymous = self.client.post(
                "/restore_backup", json={"backup_filename": archive_name}
            )
            self._login_session()
            with patch.object(backups, "plugin_backup_restore_application", application):
                missing_csrf = self.client.post(
                    "/restore_backup", json={"backup_filename": archive_name}
                )
                restored = self.client.post(
                    "/restore_backup",
                    json={"backup_filename": archive_name},
                    headers={"X-CSRF-Token": "csrf-token"},
                )

        self.assertEqual(anonymous.status_code, 401)
        self.assertEqual(missing_csrf.status_code, 403)
        self.assertEqual(restored.get_json(), {"success": True, "message": "restored"})
        self.assertEqual(application.calls, [("restore", archive_name)])

    def test_cloud_restore_reuses_local_archive_and_keeps_error_contract(self) -> None:
        application = FakePluginBackupRestoreApplication(
            local_exists=True, result=(False, "restore rejected")
        )
        webdav = FakeWebDavUpdateManager()
        archive_name = "cloud-export.zip"
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            self._login_session()
            with (
                patch.object(backups, "plugin_backup_restore_application", application),
                patch.object(backups, "update_manager", webdav),
            ):
                missing_csrf = self.client.post(
                    "/cloud_restore_backup", json={"filename": archive_name}
                )
                restored = self.client.post(
                    "/cloud_restore_backup",
                    json={"filename": archive_name},
                    headers={"X-CSRF-Token": "csrf-token"},
                )

        self.assertEqual(missing_csrf.status_code, 403)
        self.assertEqual(
            restored.get_json(),
            {"success": False, "error": "restore rejected"},
        )
        self.assertEqual(application.calls, [("exists", archive_name), ("restore", archive_name)])
        self.assertEqual(webdav.calls, [])

    def test_cloud_restore_downloads_only_when_local_archive_is_absent(self) -> None:
        application = FakePluginBackupRestoreApplication(local_exists=False)
        webdav = FakeWebDavUpdateManager()
        archive_name = "cloud-export.zip"
        self._login_session()
        with (
            patch.object(backups, "plugin_backup_restore_application", application),
            patch.object(backups, "update_manager", webdav),
        ):
            restored = self.client.post(
                "/cloud_restore_backup",
                json={"filename": archive_name},
                headers={"X-CSRF-Token": "csrf-token"},
            )

        self.assertEqual(restored.get_json(), {"success": True, "message": "restored"})
        self.assertEqual(application.calls, [("exists", archive_name), ("restore", archive_name)])
        self.assertEqual(webdav.calls, [("download", archive_name)])

    def test_cloud_restore_does_not_restore_after_download_failure(self) -> None:
        application = FakePluginBackupRestoreApplication(local_exists=False)
        webdav = FakeWebDavUpdateManager((False, "offline"))
        self._login_session()
        with (
            patch.object(backups, "plugin_backup_restore_application", application),
            patch.object(backups, "update_manager", webdav),
        ):
            response = self.client.post(
                "/cloud_restore_backup",
                json={"filename": "cloud-export.zip"},
                headers={"X-CSRF-Token": "csrf-token"},
            )

        self.assertEqual(
            response.get_json(),
            {"success": False, "error": "下载失败: offline"},
        )
        self.assertEqual(application.calls, [("exists", "cloud-export.zip")])
        self.assertEqual(webdav.calls, [("download", "cloud-export.zip")])


if __name__ == "__main__":
    unittest.main()
