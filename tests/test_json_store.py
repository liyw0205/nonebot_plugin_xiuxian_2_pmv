from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.json_store import (
    delete_json_file,
    load_json_file,
    save_json_file,
    update_json_file,
    update_json_file_bounded,
    JsonStoreDataError,
    JsonStoreLimitError,
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

    def test_bounded_update_rejects_oversized_state_without_rewrite_or_backup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            original = b"[" + b" " * 128
            path.write_bytes(original)

            with self.assertRaises(JsonStoreLimitError):
                update_json_file_bounded(path, [], lambda rows: [*rows, "x"], max_bytes=64, expected_type=list)

            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(list(path.parent.glob("*.bak")), [])

    def test_bounded_update_preserves_invalid_json_without_backup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text("{broken", encoding="utf-8")

            with self.assertRaises(JsonStoreDataError):
                update_json_file_bounded(path, [], lambda rows: [*rows, "x"], max_bytes=64, expected_type=list)

            self.assertEqual(path.read_text(encoding="utf-8"), "{broken")
            self.assertEqual(list(path.parent.glob("*.bak")), [])

    def test_bounded_update_limits_output_and_writes_with_existing_atomic_writer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"

            self.assertEqual(
                update_json_file_bounded(path, [], lambda rows: [*rows, "ok"], max_bytes=32, expected_type=list),
                ["ok"],
            )
            original = path.read_bytes()
            with self.assertRaises(JsonStoreLimitError):
                update_json_file_bounded(
                    path,
                    [],
                    lambda rows: [*rows, "x" * 64],
                    max_bytes=32,
                    expected_type=list,
                )
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(list(path.parent.glob(".*.tmp")), [])

    def test_delete_json_file_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            save_json_file(path, {"value": 1})
            self.assertTrue(delete_json_file(path))
            self.assertFalse(delete_json_file(path))


if __name__ == "__main__":
    unittest.main()
