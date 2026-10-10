from __future__ import annotations

import io
import zipfile
from pathlib import Path

from ..repository import (
    DatabaseBackupRepository,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: bytes) -> None:
        self.status_code = status_code
        self.headers = {"content-length": str(len(payload))}
        self.payload = payload
        self.closed = False

    def iter_content(self, chunk_size: int):
        yield self.payload

    def close(self) -> None:
        self.closed = True


class FakeRuntime:
    def database_backup_webdav_paths(self):
        paths = {
            "base_url": "https://dav.invalid",
            "auth": ("user", "secret"),
            "db_rel": "backups/db_backup",
            "db_url": "https://dav.invalid/backups/db_backup",
        }
        return True, "ok", paths

    def database_backup_webdav_join_url(self, base_url: str, relative_path: str) -> str:
        return f"{base_url.rstrip('/')}/{relative_path}"

    def database_backup_format_time(self, value: str) -> str:
        return value or "未知"


def _archive_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("player.db", b"sqlite snapshot")
    return buffer.getvalue()


def test_list_cloud_backups_parses_bounded_dav_response_and_closes() -> None:
    payload = b"""<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response><d:href>/backups/db_backup/db_backup_20261006_010203.zip</d:href>
    <d:propstat><d:prop><d:getcontentlength>123</d:getcontentlength>
      <d:getlastmodified>Mon, 06 Oct 2026 01:02:03 GMT</d:getlastmodified>
    </d:prop></d:propstat>
  </d:response>
</d:multistatus>"""
    response = FakeResponse(207, payload)
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return response

    repository = DatabaseBackupRepository(
        Path("unused"), Path("unused"), FakeRuntime(), request=request
    )

    ok, backups = repository.list_cloud_backups()

    assert ok is True
    assert backups == [
        {
            "filename": "db_backup_20261006_010203.zip",
            "size": 123,
            "modified": "Mon, 06 Oct 2026 01:02:03 GMT",
        }
    ]
    assert calls[0][0:2] == ("PROPFIND", "https://dav.invalid/backups/db_backup")
    assert calls[0][2]["timeout"] == 20
    assert response.closed is True


def test_download_installs_valid_archive_atomically_and_closes(tmp_path: Path) -> None:
    payload = _archive_bytes()
    response = FakeResponse(200, payload)
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return response

    backup_directory = tmp_path / "backups" / "db_backup"
    repository = DatabaseBackupRepository(
        backup_directory, tmp_path / "data", FakeRuntime(), get=get
    )
    filename = "db_backup_20261006_010203.zip"

    ok, result = repository.download_cloud_backup(filename, overwrite=False)

    assert ok is True
    assert Path(result).read_bytes() == payload
    assert calls[0][0] == "https://dav.invalid/backups/db_backup/" + filename
    assert calls[0][1]["timeout"] == 120
    assert response.closed is True
    assert list(backup_directory.iterdir()) == [backup_directory / filename]


def test_invalid_download_preserves_existing_archive_and_removes_temp_file(
    tmp_path: Path,
) -> None:
    response = FakeResponse(200, b"not a zip")
    backup_directory = tmp_path / "backups" / "db_backup"
    backup_directory.mkdir(parents=True)
    filename = "db_backup_20261006_010203.zip"
    existing = backup_directory / filename
    existing.write_bytes(b"existing backup")
    repository = DatabaseBackupRepository(
        backup_directory,
        tmp_path / "data",
        FakeRuntime(),
        get=lambda _url, **_kwargs: response,
    )

    ok, error = repository.download_cloud_backup(filename, overwrite=True)

    assert ok is False
    assert "不是有效 zip" in str(error)
    assert existing.read_bytes() == b"existing backup"
    assert list(backup_directory.iterdir()) == [existing]
    assert response.closed is True
