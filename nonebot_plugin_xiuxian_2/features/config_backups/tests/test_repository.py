from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ..repository import (
    ConfigBackupRepository,
    InvalidConfigBackup,
    MAX_CONFIG_BACKUP_BYTES,
)


class FakeRuntime:
    def __init__(self) -> None:
        self.paths = {
            "base_url": "https://dav.invalid/root",
            "config_rel": "backups/config_backups",
            "config_url": "https://dav.invalid/root/backups/config_backups",
            "auth": ("user", "pass"),
        }
        self.directory_calls: list[tuple[object, ...]] = []

    def configuration_backup_webdav_paths(self):
        return True, "ok", self.paths

    def configuration_backup_webdav_join_url(self, base_url, relative_path):
        return f"{base_url.rstrip('/')}/{relative_path}"

    def configuration_backup_webdav_make_directories(self, base_url, relative_path, auth):
        self.directory_calls.append((base_url, relative_path, auth))
        return True, "ok"

    def configuration_backup_format_time(self, value):
        return f"formatted:{value}"


class Response:
    def __init__(self, payload: bytes, status_code: int = 200) -> None:
        self.status_code = status_code
        self.payload = payload
        self.closed = False

    def iter_content(self, chunk_size: int):
        yield self.payload

    def close(self) -> None:
        self.closed = True


def webdav_response(name: str, size: int = 12) -> bytes:
    return (
        "<?xml version='1.0'?>"
        "<d:multistatus xmlns:d='DAV:'><d:response>"
        f"<d:href>/root/backups/config_backups/{name}</d:href>"
        "<d:propstat><d:prop>"
        f"<d:getcontentlength>{size}</d:getcontentlength>"
        "<d:getlastmodified>Tue, 06 Oct 2026 01:02:03 GMT</d:getlastmodified>"
        "<d:resourcetype/>"
        "</d:prop></d:propstat></d:response></d:multistatus>"
    ).encode()


class ConfigBackupRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="config-backup-repository-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "backups" / "config_backups"
        self.runtime = FakeRuntime()
        self.repository = ConfigBackupRepository(self.root, self.runtime)

    def test_local_backup_list_restore_and_delete_reject_symlinks(self) -> None:
        filename = "config_backup_20261006_010203.json"
        path = self.repository.create_local_backup(
            filename,
            {"webdav_pass": "secret", "_metadata": {"version": "v1"}},
        )
        self.assertEqual(self.repository.read_local_backup(filename), (
            {"webdav_pass": "secret"}, {"version": "v1"}
        ))
        listed = self.repository.list_local_backups()
        self.assertEqual([item["filename"] for item in listed], [filename])
        self.assertNotIn("path", listed[0])

        link = self.root / "config_backup_20261006_010204.json"
        link.symlink_to(path)
        self.assertEqual([item["filename"] for item in self.repository.list_local_backups()], [filename])
        with self.assertRaises(InvalidConfigBackup):
            self.repository.read_local_backup(link.name)
        with self.assertRaises(InvalidConfigBackup):
            self.repository.delete_local_backup(link.name)
        self.repository.delete_local_backup(filename)
        self.assertFalse(path.exists())

    def test_uploaded_json_is_bounded_and_preserves_non_object_json_contract(self) -> None:
        from io import BytesIO

        self.assertEqual(
            self.repository.parse_uploaded_config("config.json", BytesIO(b"[1,2]")),
            [1, 2],
        )
        with self.assertRaisesRegex(InvalidConfigBackup, "有效的JSON"):
            self.repository.parse_uploaded_config("config.json", BytesIO(b"{"))
        with self.assertRaisesRegex(InvalidConfigBackup, "大小限制"):
            self.repository.parse_uploaded_config(
                "config.json", BytesIO(b" " * (MAX_CONFIG_BACKUP_BYTES + 1))
            )
        with self.assertRaisesRegex(InvalidConfigBackup, "JSON格式"):
            self.repository.parse_uploaded_config("config.txt", BytesIO(b"{}"))

    def test_cloud_listing_is_bounded_and_returns_legacy_fields(self) -> None:
        filename = "config_backup_20261006_010203.json"
        response = Response(webdav_response(filename, 42), status_code=207)
        calls: list[tuple[object, ...]] = []

        def request(*args, **kwargs):
            calls.append((args, kwargs))
            return response

        repository = ConfigBackupRepository(self.root, self.runtime, request=request)
        success, backups = repository.list_cloud_backups()

        self.assertTrue(success)
        self.assertEqual(
            backups,
            [{"filename": filename, "size": 42, "modified": "formatted:Tue, 06 Oct 2026 01:02:03 GMT"}],
        )
        self.assertEqual(calls[0][1]["timeout"], 30)
        self.assertEqual(calls[0][1]["headers"], {"Depth": "1"})
        self.assertTrue(response.closed)

    def test_cloud_listing_rejects_entity_xml_and_closes_response(self) -> None:
        response = Response(b"<!DOCTYPE x [<!ENTITY y 'z'>]><x/>", status_code=207)
        repository = ConfigBackupRepository(
            self.root, self.runtime, request=lambda *_args, **_kwargs: response
        )

        success, message = repository.list_cloud_backups()

        self.assertFalse(success)
        self.assertIn("实体", message)
        self.assertTrue(response.closed)

    def test_cloud_download_installs_json_atomically_and_preserves_existing_on_error(self) -> None:
        filename = "config_backup_20261006_010203.json"
        existing = self.repository.create_local_backup(filename, {"old": True})
        bad_response = Response(b"not json")
        repository = ConfigBackupRepository(
            self.root, self.runtime, get=lambda *_args, **_kwargs: bad_response
        )

        failed, message = repository.download_cloud_backup(filename, overwrite=True)

        self.assertFalse(failed)
        self.assertIn("JSON", message)
        self.assertEqual(json.loads(existing.read_text(encoding="utf-8")), {"old": True})
        self.assertEqual(list(self.root.glob(".config-download-*.tmp")), [])
        self.assertTrue(bad_response.closed)

        good_response = Response(b'{"downloaded":true}')
        repository = ConfigBackupRepository(
            self.root, self.runtime, get=lambda *_args, **_kwargs: good_response
        )
        success, downloaded = repository.download_cloud_backup(filename, overwrite=True)
        self.assertTrue(success)
        self.assertEqual(downloaded, existing)
        self.assertEqual(json.loads(existing.read_text(encoding="utf-8")), {"downloaded": True})
        self.assertTrue(good_response.closed)

    def test_cloud_download_rejects_invalid_names_and_keeps_existing_without_overwrite(self) -> None:
        filename = "config_backup_20261006_010203.json"
        existing = self.repository.create_local_backup(filename, {"old": True})
        calls: list[str] = []

        def get(url, **_kwargs):
            calls.append(url)
            return Response(b"{}")

        repository = ConfigBackupRepository(self.root, self.runtime, get=get)
        success, result = repository.download_cloud_backup(filename)
        self.assertFalse(success)
        self.assertEqual(result, "FILE_EXISTS")
        self.assertEqual(calls, [])
        self.assertEqual(existing.read_text(encoding="utf-8"), '{\n  "old": true\n}')

        success, message = repository.download_cloud_backup("../escape.json", overwrite=True)
        self.assertFalse(success)
        self.assertIn("文件名", message)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
