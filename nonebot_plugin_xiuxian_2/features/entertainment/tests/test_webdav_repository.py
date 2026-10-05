from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

from ..webdav_repository import (
    MAX_WEBDAV_BINDINGS_BYTES,
    MAX_WEBDAV_RESPONSE_ENTRIES,
    MAX_OPENLIST_RESPONSE_BYTES,
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


class _QueueHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError("unexpected HTTP request")
        return self.responses.pop(0)


def _json_response(status: int, payload, *, content_length: str | None = None) -> _Response:
    return _Response(status, json.dumps(payload).encode("utf-8"), content_length=content_length)


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

    def test_bind_validates_before_atomic_write_and_delete_reindexes(self):
        response = _Response(207, b'<multistatus xmlns="DAV:" />')
        client = _HttpClient(response)
        repository = WebDavRepository(http_client=client)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "bindings.json"
            result = repository.bind(
                path,
                label="main",
                dav_url="https://dav.test/dav/",
                username="u",
                password="p",
            )
            self.assertEqual(result.status, "applied")
            self.assertEqual(repository.load_bindings(path)[0].dav_url, "https://dav.test/dav")
            calls_after_bind = len(client.calls)

            duplicate = repository.bind(
                path,
                label="other",
                dav_url="https://dav.test/dav",
                username="u",
                password="new",
            )
            self.assertEqual(duplicate.status, "rejected")
            self.assertEqual(len(client.calls), calls_after_bind)

            deleted = repository.delete(path, "1")
            self.assertEqual(deleted.status, "applied")
            self.assertEqual(deleted.removed[0].label, "main")
            self.assertEqual(repository.load_bindings(path), ())

    def test_bind_and_delete_leave_invalid_file_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bindings.json"
            path.write_text("not json", encoding="utf-8")
            original = path.read_bytes()
            repository = WebDavRepository(http_client=_HttpClient(_Response(207, b"")))
            with self.assertRaises(WebDavRepositoryError):
                repository.bind(
                    path,
                    label="main",
                    dav_url="https://dav.test/dav",
                    username="u",
                    password="p",
                )
            with self.assertRaises(WebDavRepositoryError):
                repository.delete(path, "all")
            self.assertEqual(path.read_bytes(), original)

    def test_download_link_direct_and_cache(self):
        client = _QueueHttpClient(
            [
                _json_response(200, {"code": 200, "data": {"token": "t"}}),
                _json_response(200, {"code": 200, "data": {"url": "/d/movie.mp4"}}),
            ]
        )
        repository = WebDavRepository(openlist_client=client)
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_binding(directory)
            result = repository.webdav_download_link(path, "1 /movie.mp4")
            cached = repository.webdav_download_link(path, "1 /movie.mp4")

        self.assertEqual(result.kind, "direct")
        self.assertEqual(result.url, "https://dav.test/d/movie.mp4")
        self.assertEqual(cached, result)
        self.assertEqual(len(client.calls), 2)

    def test_download_link_uses_signed_fallback_then_webdav(self):
        signed_client = _QueueHttpClient(
            [
                _json_response(200, {"code": 200, "data": {"token": "t"}}),
                _json_response(500, {"code": 500, "message": "link unavailable"}),
                _json_response(200, {"code": 200, "data": {"is_dir": False, "sign": "s"}}),
            ]
        )
        fallback_client = _QueueHttpClient(
            [
                _json_response(200, {"code": 200, "data": {"token": "t"}}),
                _json_response(500, {"code": 500}),
                _json_response(200, {"code": 200, "data": {"is_dir": True}}),
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_binding(directory)
            signed = WebDavRepository(openlist_client=signed_client).webdav_download_link(path, "1 /x y.mp4")
            fallback = WebDavRepository(openlist_client=fallback_client).webdav_download_link(path, "1 /x y.mp4")

        self.assertEqual(signed.kind, "direct")
        self.assertIn("?sign=s", signed.url)
        self.assertEqual(fallback.kind, "webdav")
        self.assertIn("x%20y.mp4", fallback.url)

    def test_download_link_refreshes_auth_once(self):
        client = _QueueHttpClient(
            [
                _json_response(200, {"code": 200, "data": {"token": "old"}}),
                _json_response(200, {"code": 401, "message": "expired"}),
                _json_response(200, {"code": 200, "data": {"token": "new"}}),
                _json_response(200, {"code": 200, "data": {"url": "/d/file"}}),
            ]
        )
        repository = WebDavRepository(openlist_client=client)
        with tempfile.TemporaryDirectory() as directory:
            result = repository.webdav_download_link(
                self._write_binding(directory), "1 /file"
            )
        self.assertEqual(result.kind, "direct")
        self.assertEqual(len(client.calls), 4)
        self.assertEqual(client.calls[1][2]["headers"]["Authorization"], "old")
        self.assertEqual(client.calls[3][2]["headers"]["Authorization"], "new")

    def test_openlist_json_response_is_bounded(self):
        client = _QueueHttpClient(
            [_json_response(200, {"code": 200}, content_length=str(MAX_OPENLIST_RESPONSE_BYTES + 1))]
        )
        repository = WebDavRepository(openlist_client=client)
        binding = self._write_binding(tempfile.mkdtemp())
        with self.assertRaisesRegex(WebDavRepositoryError, "超过大小限制"):
            repository._openlist_token(
                repository.load_bindings(binding)[0],
                deadline=time.monotonic() + 5,
            )


if __name__ == "__main__":
    unittest.main()
