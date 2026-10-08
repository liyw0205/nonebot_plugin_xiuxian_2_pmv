from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core, system


class CacheFileDownloadRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory(prefix="cache-download-route-")
        self.cache_root = Path(self.directory.name) / "cache"
        self.cache_root.mkdir()
        self.stack = ExitStack()
        self.stack.enter_context(patch.object(core, "ADMIN_IDS", {"admin-1"}))
        self.stack.enter_context(
            patch.object(
                system,
                "get_paths",
                return_value=SimpleNamespace(cache=self.cache_root),
            )
        )
        self.stack.enter_context(
            patch.dict(
                core.app.config,
                {"TESTING": True, "SECRET_KEY": "cache-download-route-test"},
            )
        )
        self.client = core.app.test_client()

    def tearDown(self) -> None:
        self.stack.close()
        self.directory.cleanup()

    def _login(self, admin_id: str = "admin-1") -> None:
        with self.client.session_transaction() as session:
            session["admin_id"] = admin_id
            session["_csrf_token"] = "cache-download-csrf"

    def test_download_keeps_read_permission_and_rejects_non_admin_sessions(self) -> None:
        self.assertEqual(
            core.resolve_endpoint_permission("download_file", "GET"),
            core.WebPermission.READ,
        )
        anonymous = self.client.get("/download/asset.bin")
        self.assertEqual(anonymous.status_code, 401)
        self.assertEqual(anonymous.get_json(), {"success": False, "error": "未登录"})

        self._login("not-an-admin")
        denied = self.client.get("/download/asset.bin")
        self.assertEqual(denied.status_code, 401)
        self.assertEqual(denied.get_json(), {"success": False, "error": "未登录"})

    def test_download_returns_file_bytes_for_an_admin(self) -> None:
        (self.cache_root / "nested").mkdir()
        (self.cache_root / "nested" / "asset.bin").write_bytes(b"cache-bytes")
        self._login()

        response = self.client.get("/download/nested/asset.bin")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"cache-bytes")

    def test_download_rejects_parent_traversal_and_symlink_escape(self) -> None:
        outside = self.cache_root.parent / "outside.bin"
        outside.write_bytes(b"outside")
        (self.cache_root / "escape.bin").symlink_to(outside)
        self._login()

        traversal = self.client.get("/download/../" + outside.name)
        symlink = self.client.get("/download/escape.bin")

        self.assertEqual(traversal.status_code, 403)
        self.assertEqual(symlink.status_code, 403)

    def test_download_preserves_missing_and_directory_status_codes(self) -> None:
        (self.cache_root / "folder").mkdir()
        self._login()

        missing = self.client.get("/download/missing.bin")
        directory = self.client.get("/download/folder")

        self.assertEqual(missing.status_code, 404)
        self.assertEqual(directory.status_code, 403)


if __name__ == "__main__":
    unittest.main()
