from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import (
    _migrate_statistics_data_sync,
)


class StatisticsMigrationIoTests(unittest.TestCase):
    def test_migration_writes_sorted_fields_and_isolates_invalid_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            players = Path(directory)
            valid = players / "1001"
            empty = players / "1002"
            broken = players / "1003"
            invalid_type = players / "1004"
            no_data = players / "1005"
            for user_dir in (valid, empty, broken, invalid_type, no_data):
                user_dir.mkdir()

            (valid / "statistics.json").write_text(
                json.dumps({"win": 3, "battle": 8}, ensure_ascii=False),
                encoding="utf-8",
            )
            (empty / "statistics.json").write_text("", encoding="utf-8")
            (broken / "statistics.json").write_text("{broken", encoding="utf-8")
            (invalid_type / "statistics.json").write_text("[]", encoding="utf-8")

            with patch(
                "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.player_data_manager.update_or_write_data"
            ) as update:
                result = _migrate_statistics_data_sync(players)

        self.assertEqual(result, (5, 1, 2))
        self.assertEqual(
            update.call_args_list,
            [
                unittest.mock.call("1001", "statistics", "battle", 8),
                unittest.mock.call("1001", "statistics", "win", 3),
            ],
        )

    def test_database_failure_is_counted_and_next_user_continues(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            players = Path(directory)
            for user_id in ("1001", "1002"):
                user_dir = players / user_id
                user_dir.mkdir()
                (user_dir / "statistics.json").write_text(
                    json.dumps({"battle": 1}), encoding="utf-8"
                )

            with patch(
                "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.player_data_manager.update_or_write_data",
                side_effect=[sqlite3.OperationalError("database unavailable"), None],
            ) as update:
                result = _migrate_statistics_data_sync(players)

        self.assertEqual(result, (2, 1, 1))
        self.assertEqual(update.call_count, 2)

    def test_missing_players_directory_returns_zero_counts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing"
            self.assertEqual(_migrate_statistics_data_sync(missing), (0, 0, 0))


if __name__ == "__main__":
    unittest.main()
