from __future__ import annotations

import tempfile
import unittest
from io import BytesIO
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info.changelog_command import (
    _delete_generated_image,
    _read_generated_image,
)


class ChangelogIoTests(unittest.TestCase):
    def test_read_generated_image_supports_memory_values(self) -> None:
        original = BytesIO(b"image")
        original.seek(3)

        buffer_value, path_value = _read_generated_image(original)
        self.assertIs(buffer_value, original)
        self.assertEqual(buffer_value.tell(), 0)
        self.assertIsNone(path_value)

        bytes_value, path_value = _read_generated_image(b"bytes")
        self.assertEqual(bytes_value.read(), b"bytes")
        self.assertIsNone(path_value)

    def test_read_and_delete_generated_image_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "changelog.png"
            path.write_bytes(b"png-data")

            buffer_value, path_value = _read_generated_image(str(path))
            self.assertEqual(buffer_value.read(), b"png-data")
            self.assertEqual(path_value, path)

            _delete_generated_image(path)
            self.assertFalse(path.exists())

    def test_read_generated_image_rejects_unsupported_type(self) -> None:
        with self.assertRaises(TypeError):
            _read_generated_image(object())


if __name__ == "__main__":
    unittest.main()
