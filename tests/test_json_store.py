from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.json_store import (
    load_json_file,
    save_json_file,
    update_json_file,
)


class JsonStoreTests(unittest.TestCase):
    def test_missing_and_invalid_files_recover_to_typed_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            self.assertEqual(load_json_file(path, [], list), [])
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), [])

            path.write_text('{"wrong": true}', encoding="utf-8")
            self.assertEqual(load_json_file(path, [], list), [])
            self.assertTrue(list(path.parent.glob("state.json.invalid.*.bak")))

    def test_atomic_save_leaves_no_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            save_json_file(path, {"value": 1})
            self.assertEqual(load_json_file(path, {}, dict), {"value": 1})
            self.assertEqual(list(path.parent.glob(".*.tmp")), [])

    def test_update_json_file_serializes_read_modify_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"

            def append(values):
                values.append("x")
                return values

            self.assertEqual(
                update_json_file(path, [], append, expected_type=list),
                ["x"],
            )
            self.assertEqual(load_json_file(path, [], list), ["x"])


if __name__ == "__main__":
    unittest.main()
