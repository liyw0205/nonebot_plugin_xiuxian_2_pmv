from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ..webdav_repository import (
    MAX_WEBDAV_BINDINGS_BYTES,
    MAX_WEBDAV_RESPONSE_ENTRIES,
    WebDavRepository,
    WebDavRepositoryError,
    WebDavTargetError,
)


class _Response:
    def __init__(self, status_code: int, content: bytes, *, content_length: str | None = None):
        self.status_code = status_code
        self.content = content
        self.headers = {}
        if content_length is not None:
            self.headers["content-length"] = content_length
        self.closed = False

    def iter_content(self, *, chunk_size: int):
        yield self.content

    def close(self):
        self.closed = True


class _HttpClient:
    def __init__(self, response: _Response):
        self.response = response
        self.calls = []

    def request(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.response


def _xml(*entries: str) -> bytes:
    return (
        '<multistatus xmlns="DAV:">' + "".join(entries) + "</multistatus>"
    ).encode()


def _entry(href: str, name: str, *, directory: bool = False) -> str:
    resource = "<resourcetype><collection /></resourcetype>" if directory else "<resourcetype />"
    return (
        f"<response><href>{href}</href><propstat><status>HTTP/1.1 200 OK</status>"
        f"<prop><displayname>{name}</displayname>{resource}"
        "<getcontentlength>12</getcontentlength><getlastmodified>today</getlastmodified>"
        "<getcontenttype>video/mp4</getcontenttype></prop></propstat></response>"
    )


class WebDavRepositoryTests(unittest.TestCase):
    def _write_binding(self, directory: str, rows=None) -> Path:
        path = Path(directory) / "bindings.json"
        path.write_text(
            json.dumps(rows or [{"label": "main", "dav_url": "https://dav.test/dav", "username": "u", "password": "p"}]),
            encoding="utf-8",
        )
        return path

    def test_missing_and_invalid_bindings_do_not_create_or_rewrite(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing.json"
            repository = WebDavRepository()
            self.assertEqual(repository.load_bindings(path), ())
            self.assertFalse(path.exists())

            path.write_text("not json", encoding="utf-8")
            original = path.read_text(encoding="utf-8")
            with self.assertRaises(WebDavRepositoryError):
                repository.load_bindings(path)
            self.assertEqual(path.read_text(encoding="utf-8"), original)

    def test_binding_file_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bindings.json"
            path.write_bytes(b"[" + b" " * MAX_WEBDAV_BINDINGS_BYTES)
            with self.assertRaises(WebDavRepositoryError):
                WebDavRepository().load_bindings(path)

    def test_propfind_parses_entries_and_closes_response(self):
        response = _Response(207, _xml(_entry("/dav/", "root", directory=True), _entry("/dav/a.mp4", "a.mp4")))
        client = _HttpClient(response)
        repository = WebDavRepository(http_client=client)
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_binding(directory)
            result = repository.propfind(path, "1 /", depth="1", need_path=False)

        self.assertEqual(result.binding.label, "main")
        self.assertEqual(result.path, "/")
        self.assertEqual([entry.name for entry in result.entries], ["root", "a.mp4"])
        self.assertTrue(response.closed)
        self.assertEqual(client.calls[0][0], "PROPFIND")
        self.assertEqual(client.calls[0][2]["auth"], ("u", "p"))
        self.assertTrue(client.calls[0][2]["stream"])

    def test_status_and_selector_errors_preserve_command_messages(self):
        client = _HttpClient(_Response(401, b""))
        repository = WebDavRepository(http_client=client)
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_binding(directory)
            with self.assertRaisesRegex(WebDavTargetError, "请填写路径"):
                repository.propfind(path, "1", depth="0", need_path=True)
            with self.assertRaisesRegex(WebDavRepositoryError, "认证失败"):
                repository.propfind(path, "1 /x", depth="0", need_path=True)

    def test_response_entry_count_is_bounded(self):
        response = _Response(207, _xml(*(_entry(f"/dav/{i}", str(i)) for i in range(MAX_WEBDAV_RESPONSE_ENTRIES + 1))))
        repository = WebDavRepository(http_client=_HttpClient(response))
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_binding(directory)
            with self.assertRaisesRegex(WebDavRepositoryError, "条目数量"):
                repository.propfind(path, "1 /", depth="1", need_path=False)


if __name__ == "__main__":
    unittest.main()
