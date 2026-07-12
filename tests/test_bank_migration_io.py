from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import _migrate_bank_data_sync


class BankMigrationIoTests(unittest.TestCase):
    def test_migration_reads_valid_files_and_counts_failures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            players = Path(directory)
            valid = players / "1001"
            invalid = players / "1002"
            empty = players / "1003"
            valid.mkdir()
            invalid.mkdir()
            empty.mkdir()
            (valid / "bankinfo.json").write_text(
                json.dumps(
                    {
                        "savestone": "120",
                        "savetime": "2026-07-12 12:00:00",
                        "banklevel": 3,
                    }
                ),
                encoding="utf-8",
            )
            (invalid / "bankinfo.json").write_text("{broken", encoding="utf-8")
            (empty / "bankinfo.json").write_text("", encoding="utf-8")

            with patch(
                "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.player_data_manager.update_or_write_data"
            ) as update:
                user_num, sync_num, fail_num = _migrate_bank_data_sync(players)

        self.assertEqual((user_num, sync_num, fail_num), (3, 1, 1))
        self.assertEqual(update.call_count, 3)
        update.assert_any_call(
            "1001", "bankinfo", "savestone", 120, data_type="INTEGER"
        )
        update.assert_any_call(
            "1001", "bankinfo", "banklevel", "3", data_type="TEXT"
        )

    def test_missing_players_directory_returns_zero_counts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing"
            self.assertEqual(_migrate_bank_data_sync(missing), (0, 0, 0))


if __name__ == "__main__":
    unittest.main()
