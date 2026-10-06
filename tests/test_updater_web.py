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


class FakePluginBackupCloudApplication:
    def __init__(self, local_exists: bool = True) -> None:
        self.local_exists = local_exists
        self.list_result = (True, [{"filename": "backup_1_2_v1.zip", "size": 3}])
        self.sync_result = (True, "downloaded")
        self.sync_batch_result = (
            ["backup_1_2_ok.zip"],
            ["backup_1_2_exists.zip"],
            [{"filename": "backup_1_2_bad.zip", "reason": "offline"}],
        )
        self.delete_batch_result = (
            ["backup_1_2_ok.zip"],
            [{"filename": "backup_1_2_bad.zip", "reason": "remote failure"}],
        )
        self.calls: list[tuple[object, ...]] = []

    def list_cloud_backups(self):
        self.calls.append(("list",))
        return self.list_result

    def local_backup_exists(self, filename: str) -> bool:
        self.calls.append(("local_exists", filename))
        return self.local_exists

    def sync_cloud_backup(self, filename: str, *, overwrite: bool = False):
        self.calls.append(("sync", filename, overwrite))
        return self.sync_result

    def sync_cloud_backups(self, filenames: list[object], *, overwrite: bool = False):
        self.calls.append(("sync_many", filenames, overwrite))
        return self.sync_batch_result

    def delete_cloud_backups(self, filenames: list[object]):
        self.calls.append(("delete_many", filenames))
        return self.delete_batch_result


class FakeDatabaseBackupApplication:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def create_backup(self):
        self.calls.append(("create",))
        return True, "created"

    def list_local_backups(self):
        self.calls.append(("list_local",))
        return [{"filename": "db_backup_20261006_010203.zip", "size": 10}]

    def restore_local_backup(self, filename, selected):
        self.calls.append(("restore_local", filename, selected))
        return True, "restored"

    def list_cloud_backups(self):
        self.calls.append(("list_cloud",))
        return True, [{"filename": "db_backup_20261006_010203.zip", "size": 10}]

    def sync_cloud_backup(self, filename, *, overwrite=False):
        self.calls.append(("sync", filename, overwrite))
        return True, "downloaded"

    def restore_cloud_backup(self, filename, selected):
        self.calls.append(("restore_cloud", filename, selected))
        return True, "cloud restored"

    def delete_local_backups(self, filenames):
        self.calls.append(("delete_local", filenames))
        return ["db_backup_20261006_010203.zip"], []

    def sync_cloud_backups(self, filenames, *, overwrite=False):
        self.calls.append(("sync_many", filenames, overwrite))
        return ["db_backup_20261006_010203.zip"], [], []

    def delete_cloud_backups(self, filenames):
        self.calls.append(("delete_cloud_many", filenames))
        return ["db_backup_20261006_010203.zip"], []


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

    def test_cloud_plugin_backup_routes_keep_admin_csrf_and_batch_contracts(self) -> None:
        application = FakePluginBackupCloudApplication()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            anonymous_list = self.client.get("/get_cloud_backups")
            anonymous_sync = self.client.post(
                "/sync_cloud_backup", json={"filename": "backup_1_2_v1.zip"}
            )
            anonymous_batch = self.client.post(
                "/batch_sync_cloud_backups", json={"filenames": ["backup_1_2_v1.zip"]}
            )
            self._login_session()
            with patch.object(backups, "plugin_backup_cloud_application", application):
                missing_csrf = self.client.post(
                    "/sync_cloud_backup", json={"filename": "backup_1_2_v1.zip"}
                )
                missing_batch_csrf = self.client.post(
                    "/batch_delete_cloud_backups", json={"filenames": ["backup_1_2_v1.zip"]}
                )
                listing = self.client.get("/get_cloud_backups")
                synced = self.client.post(
                    "/sync_cloud_backup",
                    json={"filename": "backup_1_2_v1.zip", "overwrite": True},
                    headers={"X-CSRF-Token": "csrf-token"},
                )
                batch_sync = self.client.post(
                    "/batch_sync_cloud_backups",
                    json={"filenames": ["backup_1_2_ok.zip"], "overwrite": False},
                    headers={"X-CSRF-Token": "csrf-token"},
                )
                batch_delete = self.client.post(
                    "/batch_delete_cloud_backups",
                    json={"filenames": ["backup_1_2_ok.zip", "backup_1_2_bad.zip"]},
                    headers={"X-CSRF-Token": "csrf-token"},
                )

        self.assertEqual(anonymous_list.status_code, 401)
        self.assertEqual(anonymous_sync.status_code, 401)
        self.assertEqual(anonymous_batch.status_code, 401)
        self.assertEqual(missing_csrf.status_code, 403)
        self.assertEqual(missing_batch_csrf.status_code, 403)
        self.assertEqual(listing.get_json(), {"success": True, "backups": application.list_result[1]})
        self.assertEqual(
            synced.get_json(),
            {"success": True, "message": "已成功从云端同步: backup_1_2_v1.zip"},
        )
        self.assertEqual(
            batch_sync.get_json(),
            {
                "success": True,
                "message": "批量同步完成：成功 1，已存在 1，失败 1",
                "synced": ["backup_1_2_ok.zip"],
                "exists": ["backup_1_2_exists.zip"],
                "failed": [{"filename": "backup_1_2_bad.zip", "reason": "offline"}],
            },
        )
        self.assertEqual(
            batch_delete.get_json(),
            {
                "success": True,
                "message": "云端批量删除完成：成功 1，失败 1",
                "deleted": ["backup_1_2_ok.zip"],
                "failed": [{"filename": "backup_1_2_bad.zip", "reason": "remote failure"}],
            },
        )
        self.assertEqual(
            application.calls,
            [
                ("list",),
                ("sync", "backup_1_2_v1.zip", True),
                ("sync_many", ["backup_1_2_ok.zip"], False),
                ("delete_many", ["backup_1_2_ok.zip", "backup_1_2_bad.zip"]),
            ],
        )

    def test_cloud_restore_reuses_local_archive_and_keeps_error_contract(self) -> None:
        application = FakePluginBackupRestoreApplication(
            local_exists=True, result=(False, "restore rejected")
        )
        cloud = FakePluginBackupCloudApplication(local_exists=True)
        archive_name = "cloud-export.zip"
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            self._login_session()
            with (
                patch.object(backups, "plugin_backup_restore_application", application),
                patch.object(backups, "plugin_backup_cloud_application", cloud),
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
        self.assertEqual(application.calls, [("restore", archive_name)])
        self.assertEqual(cloud.calls, [("local_exists", archive_name)])

    def test_cloud_restore_downloads_only_when_local_archive_is_absent(self) -> None:
        application = FakePluginBackupRestoreApplication(local_exists=False)
        cloud = FakePluginBackupCloudApplication(local_exists=False)
        archive_name = "cloud-export.zip"
        self._login_session()
        with (
            patch.object(backups, "plugin_backup_restore_application", application),
            patch.object(backups, "plugin_backup_cloud_application", cloud),
        ):
            restored = self.client.post(
                "/cloud_restore_backup",
                json={"filename": archive_name},
                headers={"X-CSRF-Token": "csrf-token"},
            )

        self.assertEqual(restored.get_json(), {"success": True, "message": "restored"})
        self.assertEqual(application.calls, [("restore", archive_name)])
        self.assertEqual(
            cloud.calls,
            [("local_exists", archive_name), ("sync", archive_name, False)],
        )

    def test_cloud_restore_does_not_restore_after_download_failure(self) -> None:
        application = FakePluginBackupRestoreApplication(local_exists=False)
        cloud = FakePluginBackupCloudApplication(local_exists=False)
        cloud.sync_result = (False, "offline")
        self._login_session()
        with (
            patch.object(backups, "plugin_backup_restore_application", application),
            patch.object(backups, "plugin_backup_cloud_application", cloud),
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
        self.assertEqual(application.calls, [])
        self.assertEqual(
            cloud.calls,
            [("local_exists", "cloud-export.zip"), ("sync", "cloud-export.zip", False)],
        )

    def test_database_backup_routes_share_feature_application_and_http_contract(self) -> None:
        application = FakeDatabaseBackupApplication()
        filename = "db_backup_20261006_010203.zip"
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            anonymous = self.client.post("/manual_db_backup")
            self._login_session()
            with patch.object(backups, "database_backup_application", application):
                missing_csrf = self.client.post("/manual_db_backup")
                created = self.client.post(
                    "/manual_db_backup", headers={"X-CSRF-Token": "csrf-token"}
                )
                local_list = self.client.get("/get_db_backups")
                local_restore = self.client.post(
                    "/restore_db_backup",
                    json={"backup_filename": filename, "selected_dbs": ["player"]},
                    headers={"X-CSRF-Token": "csrf-token"},
                )
                cloud_list = self.client.get("/get_cloud_db_backups")
                cloud_sync = self.client.post(
                    "/sync_cloud_db_backup",
                    json={"filename": filename, "overwrite": True},
                    headers={"X-CSRF-Token": "csrf-token"},
                )
                cloud_restore = self.client.post(
                    "/cloud_restore_db_backup",
                    json={"filename": filename, "selected_dbs": ["xiuxian"]},
                    headers={"X-CSRF-Token": "csrf-token"},
                )
                local_delete = self.client.post(
                    "/batch_delete_db_backups",
                    json={"filenames": [filename]},
                    headers={"X-CSRF-Token": "csrf-token"},
                )
                cloud_sync_many = self.client.post(
                    "/batch_sync_cloud_db_backups",
                    json={"filenames": [filename], "overwrite": False},
                    headers={"X-CSRF-Token": "csrf-token"},
                )
                cloud_delete_many = self.client.post(
                    "/batch_delete_cloud_db_backups",
                    json={"filenames": [filename]},
                    headers={"X-CSRF-Token": "csrf-token"},
                )

        self.assertEqual(anonymous.status_code, 401)
        self.assertEqual(missing_csrf.status_code, 403)
        self.assertEqual(created.get_json(), {"success": True, "message": "created", "error": ""})
        self.assertEqual(local_list.get_json()["backups"][0]["filename"], filename)
        self.assertEqual(local_restore.get_json(), {"success": True, "message": "restored", "error": ""})
        self.assertEqual(cloud_list.get_json()["backups"][0]["filename"], filename)
        self.assertEqual(cloud_sync.get_json(), {"success": True, "message": f"同步成功: {filename}"})
        self.assertEqual(cloud_restore.get_json(), {"success": True, "message": "cloud restored", "error": ""})
        self.assertEqual(local_delete.get_json()["deleted"], [filename])
        self.assertEqual(cloud_sync_many.get_json()["synced"], [filename])
        self.assertEqual(cloud_delete_many.get_json()["deleted"], [filename])
        self.assertEqual(
            application.calls,
            [
                ("create",),
                ("list_local",),
                ("restore_local", filename, ["player.db"]),
                ("list_cloud",),
                ("sync", filename, True),
                ("restore_cloud", filename, ["xiuxian.db"]),
                ("delete_local", [filename]),
                ("sync_many", [filename], False),
                ("delete_cloud_many", [filename]),
            ],
        )


if __name__ == "__main__":
    unittest.main()
