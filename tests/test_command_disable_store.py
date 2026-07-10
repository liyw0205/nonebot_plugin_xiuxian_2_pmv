from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian import command_disable


class CommandDisableStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "command_disable.json"
        self.path_patch = patch.object(command_disable, "COMMAND_DISABLE_FILE", self.path)
        self.path_patch.start()
        command_disable._COMMAND_ENTRIES = {}

    def tearDown(self) -> None:
        self.path_patch.stop()
        command_disable._COMMAND_ENTRIES = {}
        self.temp_dir.cleanup()

    def test_round_trip_preserves_normalized_entries(self) -> None:
        command_disable._COMMAND_ENTRIES = {
            "修仙签到": {"disabled": True, "module": "xiuxian_base"}
        }
        command_disable.save_command_disable_memory()
        command_disable._COMMAND_ENTRIES = {}

        loaded = command_disable.load_command_disable_memory()

        self.assertEqual(
            loaded["修仙签到"],
            {"disabled": True, "module": "xiuxian_base"},
        )
        self.assertEqual(list(self.path.parent.glob(".*.tmp")), [])

    def test_invalid_file_is_backed_up_and_reset(self) -> None:
        self.path.write_text("{broken", encoding="utf-8")

        loaded = command_disable.load_command_disable_memory()

        self.assertEqual(loaded, {})
        self.assertTrue(list(self.path.parent.glob("command_disable.json.invalid.*.bak")))

    def test_filesystem_error_is_not_silently_swallowed(self) -> None:
        command_disable._COMMAND_ENTRIES = {
            "修仙签到": {"disabled": True, "module": "xiuxian_base"}
        }
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.command_disable.save_json_file",
            side_effect=OSError("disk full"),
        ):
            with self.assertRaisesRegex(OSError, "disk full"):
                command_disable.save_command_disable_memory()


if __name__ == "__main__":
    unittest.main()
