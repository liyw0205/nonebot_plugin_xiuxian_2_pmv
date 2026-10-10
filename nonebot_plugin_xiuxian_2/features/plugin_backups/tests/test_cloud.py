from __future__ import annotations

import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from ..cloud_application import (
    PluginBackupCloudApplication,
)
from ..cloud_repository import (
    InvalidCloudPluginBackup,
    PluginBackupCloudRepository,
)


ARCHIVE_NAME = "backup_20261006_010203_v2.0.0.zip"


class FakeCloudRuntime:
    def plugin_backup_webdav_paths(self):
        return True, "ok", {
            "base_url": "https://dav.invalid",
            "plugin_rel": "backups/plugin",
            "plugin_url": "https://dav.invalid/backups/plugin",
            "auth": ("user", "password"),
        }

    def plugin_backup_webdav_join_url(self, base_url: str, relative_path: str) -> str:
        return f"{base_url.rstrip('/')}/{relative_path}"

    def plugin_backup_webdav_format_time(self, value: str) -> str:
        return value


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        chunks: tuple[bytes, ...] = (),
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.chunks = chunks
        self.headers = headers or {}
        self.closed = False

    def iter_content(self, chunk_size: int):
        yield from self.chunks

    def close(self) -> None:
        self.closed = True


def zip_bytes() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("data/example.json", "{}")
    return output.getvalue()


def dav_xml(*filenames: str) -> bytes:
    responses = [
        "<d:response><d:href>/backups/plugin/</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>"
    ]
    for filename in filenames:
        responses.append(
            "<d:response><d:href>/backups/plugin/"
            f"{filename}"
            "</d:href><d:propstat><d:prop><d:resourcetype/>"
            "<d:getcontentlength>12</d:getcontentlength>"
            "<d:getlastmodified>Tue, 06 Oct 2026 01:02:03 GMT</d:getlastmodified>"
            "</d:prop></d:propstat></d:response>"
        )
    return (
        '<d:multistatus xmlns:d="DAV:">'
        + "".join(responses)
        + "</d:multistatus>"
    ).encode()


class PluginBackupCloudRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="plugin-backup-cloud-")
        self.root = Path(self.temp.name)
        self.runtime = FakeCloudRuntime()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_list_cloud_backups_bounds_xml_and_keeps_plugin_archives_only(self) -> None:
        response = FakeResponse(207, (dav_xml(ARCHIVE_NAME, "readme.txt"),))
        repository = PluginBackupCloudRepository(
            self.root,
            self.runtime,
            request=lambda *args, **kwargs: response,
        )

        ok, result = repository.list_cloud_backups()

        self.assertTrue(ok)
        self.assertEqual(
            result,
            [{"filename": ARCHIVE_NAME, "size": 12, "modified": "Tue, 06 Oct 2026 01:02:03 GMT"}],
        )
        self.assertTrue(response.closed)

    def test_list_cloud_backups_rejects_oversized_and_entity_xml(self) -> None:
        oversized = FakeResponse(207, (b"12345",))
        repository = PluginBackupCloudRepository(
            self.root,
            self.runtime,
            request=lambda *args, **kwargs: oversized,
        )
        with patch(
            "nonebot_plugin_xiuxian_2.features.plugin_backups.cloud_repository.MAX_CLOUD_LIST_BYTES",
            4,
        ):
            ok, error = repository.list_cloud_backups()
        self.assertFalse(ok)
        self.assertIn("大小限制", error)
        self.assertTrue(oversized.closed)

        entity_response = FakeResponse(
            207,
            (b'<!DOCTYPE x [<!ENTITY e "boom">]><x xmlns="DAV:">&e;</x>',),
        )
        repository = PluginBackupCloudRepository(
            self.root,
            self.runtime,
            request=lambda *args, **kwargs: entity_response,
        )
        ok, error = repository.list_cloud_backups()
        self.assertFalse(ok)
        self.assertIn("实体", error)

    def test_list_cloud_backups_enforces_entry_limit(self) -> None:
        response = FakeResponse(207, (dav_xml(ARCHIVE_NAME, "backup_20261005_010203_v1.0.0.zip"),))
        repository = PluginBackupCloudRepository(
            self.root,
            self.runtime,
            request=lambda *args, **kwargs: response,
        )
        with patch(
            "nonebot_plugin_xiuxian_2.features.plugin_backups.cloud_repository.MAX_CLOUD_LIST_ENTRIES",
            1,
        ):
            ok, error = repository.list_cloud_backups()
        self.assertFalse(ok)
        self.assertIn("条目超过限制", error)

    def test_download_writes_valid_zip_atomically_and_closes_response(self) -> None:
        payload = zip_bytes()
        response = FakeResponse(200, (payload[:5], payload[5:]), {"content-length": str(len(payload))})
        repository = PluginBackupCloudRepository(
            self.root,
            self.runtime,
            get=lambda *args, **kwargs: response,
        )

        ok, result = repository.download_cloud_backup(ARCHIVE_NAME, overwrite=False)

        self.assertTrue(ok)
        self.assertEqual(Path(result).read_bytes(), payload)
        self.assertTrue(zipfile.is_zipfile(result))
        self.assertTrue(response.closed)
        self.assertEqual(sorted(item.name for item in self.root.iterdir()), [ARCHIVE_NAME])

    def test_bad_download_preserves_existing_file(self) -> None:
        target = self.root / ARCHIVE_NAME
        target.write_bytes(b"previous")
        response = FakeResponse(200, (b"not a zip",))
        repository = PluginBackupCloudRepository(
            self.root,
            self.runtime,
            get=lambda *args, **kwargs: response,
        )

        ok, error = repository.download_cloud_backup(ARCHIVE_NAME, overwrite=True)

        self.assertFalse(ok)
        self.assertIn("不是有效 zip", error)
        self.assertEqual(target.read_bytes(), b"previous")
        self.assertTrue(response.closed)
        self.assertEqual(sorted(item.name for item in self.root.iterdir()), [ARCHIVE_NAME])

    def test_download_rejects_bad_names_and_declared_oversize(self) -> None:
        repository = PluginBackupCloudRepository(self.root, self.runtime)
        with self.assertRaises(InvalidCloudPluginBackup):
            repository.local_backup_exists("../escape.zip")

        payload = zip_bytes()
        response = FakeResponse(200, (payload,), {"content-length": "11"})
        repository = PluginBackupCloudRepository(
            self.root,
            self.runtime,
            get=lambda *args, **kwargs: response,
        )
        with patch(
            "nonebot_plugin_xiuxian_2.features.plugin_backups.cloud_repository.MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES",
            10,
        ):
            ok, error = repository.download_cloud_backup(ARCHIVE_NAME, overwrite=True)
        self.assertFalse(ok)
        self.assertIn("大小限制", error)
        self.assertEqual(list(self.root.iterdir()), [])
        self.assertTrue(response.closed)

    def test_delete_uses_plugin_root_and_closes_response(self) -> None:
        response = FakeResponse(204)
        repository = PluginBackupCloudRepository(
            self.root,
            self.runtime,
            delete=lambda *args, **kwargs: response,
        )

        ok, message = repository.delete_cloud_backup(ARCHIVE_NAME)

        self.assertTrue(ok)
        self.assertEqual(message, f"已删除云端文件: {ARCHIVE_NAME}")
        self.assertTrue(response.closed)


class PluginBackupCloudApplicationTests(unittest.TestCase):
    def test_sync_conflict_and_restore_fetch_use_one_repository_boundary(self) -> None:
        class Repository:
            def __init__(self) -> None:
                self.downloads: list[tuple[str, bool]] = []
                self.exists = True

            def local_backup_exists(self, filename: str) -> bool:
                return self.exists

            def download_cloud_backup(self, filename: str, *, overwrite: bool):
                self.downloads.append((filename, overwrite))
                self.exists = True
                return True, Path(filename)

            def delete_cloud_backup(self, filename: str):
                return True, filename

            def list_cloud_backups(self):
                return True, []

        repository = Repository()
        application = PluginBackupCloudApplication(repository)  # type: ignore[arg-type]

        self.assertEqual(application.sync_cloud_backup(ARCHIVE_NAME), (False, "FILE_EXISTS"))
        self.assertEqual(repository.downloads, [])
        self.assertTrue(application.local_backup_exists(ARCHIVE_NAME))
        repository.exists = False
        self.assertFalse(application.local_backup_exists(ARCHIVE_NAME))
        self.assertEqual(
            application.sync_cloud_backup(ARCHIVE_NAME, overwrite=False),
            (True, Path(ARCHIVE_NAME)),
        )
        self.assertEqual(repository.downloads, [(ARCHIVE_NAME, False)])

    def test_batch_sync_and_delete_preserve_partial_results_and_bound_work(self) -> None:
        class Repository:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def local_backup_exists(self, filename: str) -> bool:
                return filename.endswith("exists.zip")

            def download_cloud_backup(self, filename: str, *, overwrite: bool):
                self.calls.append(("download", filename))
                return filename.endswith("ok.zip"), "offline"

            def delete_cloud_backup(self, filename: str):
                self.calls.append(("delete", filename))
                return filename.endswith("ok.zip"), "remote failure"

            def list_cloud_backups(self):
                return True, []

        repository = Repository()
        application = PluginBackupCloudApplication(repository)  # type: ignore[arg-type]
        with patch(
            "nonebot_plugin_xiuxian_2.features.plugin_backups.cloud_application.MAX_CLOUD_BACKUP_BATCH",
            2,
        ):
            synced, exists, failed = application.sync_cloud_backups(
                ["backup_1_2_ok.zip", "backup_1_2_exists.zip", "backup_1_2_extra.zip"]
            )
            deleted, delete_failed = application.delete_cloud_backups(
                ["backup_1_2_ok.zip", "backup_1_2_bad.zip", "backup_1_2_extra.zip"]
            )

        self.assertEqual(synced, ["backup_1_2_ok.zip"])
        self.assertEqual(exists, ["backup_1_2_exists.zip"])
        self.assertEqual(failed, [{"filename": "", "reason": "单次最多同步 2 个文件"}])
        self.assertEqual(deleted, ["backup_1_2_ok.zip"])
        self.assertEqual(
            delete_failed,
            [
                {"filename": "backup_1_2_bad.zip", "reason": "remote failure"},
                {"filename": "", "reason": "单次最多删除 2 个文件"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
