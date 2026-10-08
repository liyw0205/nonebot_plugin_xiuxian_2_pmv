from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import CacheFileApplication
from ..repository import (
    CacheFileNotFound,
    CacheFileNotRegular,
    CacheFileOutsideRoot,
)


class CacheFileApplicationTests(unittest.TestCase):
    def test_resolves_regular_files_under_cache_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="cache-files-") as directory:
            root = Path(directory)
            target = root / "nested" / "asset.bin"
            target.parent.mkdir()
            target.write_bytes(b"asset")

            resolved = CacheFileApplication().resolve_download(root, "nested/asset.bin")

            self.assertEqual(resolved, target.resolve())

    def test_rejects_parent_and_absolute_path_escape(self) -> None:
        with tempfile.TemporaryDirectory(prefix="cache-files-") as directory:
            root = Path(directory) / "cache"
            root.mkdir()
            outside = Path(directory) / "outside.bin"
            outside.write_bytes(b"outside")
            application = CacheFileApplication()

            with self.assertRaises(CacheFileOutsideRoot):
                application.resolve_download(root, "../outside.bin")
            with self.assertRaises(CacheFileOutsideRoot):
                application.resolve_download(root, str(outside))

    def test_rejects_symlink_escape_but_allows_in_root_symlink(self) -> None:
        with tempfile.TemporaryDirectory(prefix="cache-files-") as directory:
            base = Path(directory)
            root = base / "cache"
            root.mkdir()
            inside = root / "asset.bin"
            inside.write_bytes(b"inside")
            outside = base / "outside.bin"
            outside.write_bytes(b"outside")
            (root / "inside-link.bin").symlink_to(inside)
            (root / "outside-link.bin").symlink_to(outside)
            application = CacheFileApplication()

            self.assertEqual(
                application.resolve_download(root, "inside-link.bin"),
                inside.resolve(),
            )
            with self.assertRaises(CacheFileOutsideRoot):
                application.resolve_download(root, "outside-link.bin")

    def test_missing_file_raises_not_found(self) -> None:
        with tempfile.TemporaryDirectory(prefix="cache-files-") as directory:
            with self.assertRaises(CacheFileNotFound):
                CacheFileApplication().resolve_download(directory, "missing.bin")

    def test_directory_is_not_a_downloadable_file(self) -> None:
        with tempfile.TemporaryDirectory(prefix="cache-files-") as directory:
            Path(directory, "folder").mkdir()
            with self.assertRaises(CacheFileNotRegular):
                CacheFileApplication().resolve_download(directory, "folder")


if __name__ == "__main__":
    unittest.main()
