from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang import (
    _migrate_unseal_data_sync,
)


class UnsealMigrationIoTests(unittest.TestCase):
    def test_migration_converts_valid_data_and_isolates_invalid_users(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            players = root / "players"
            players.mkdir()
            valid = players / "1001"
            empty = players / "1002"
            broken = players / "1003"
            invalid_type = players / "1004"
            no_data = players / "1005"
            for user_dir in (valid, empty, broken, invalid_type, no_data):
                user_dir.mkdir()

            (valid / "unseal_data.json").write_text(
                json.dumps(
                    {
                        "unseal_info": {
                            "count": "2",
                            "total_cost": "300",
                            "profit": 40,
                            "loss": 5,
                        },
                        "sharing_info": {
                            "shared_profit": "6",
                            "shared_loss": 7,
                            "received_profit": 8,
                            "received_loss": 9,
                        },
                        "last_update": "2026-07-12 10:00:00",
                    }
                ),
                encoding="utf-8",
            )
            (empty / "unseal_data.json").write_text("", encoding="utf-8")
            (broken / "unseal_data.json").write_text("{broken", encoding="utf-8")
            (invalid_type / "unseal_data.json").write_text(
                json.dumps({"unseal_info": [], "sharing_info": {}}),
                encoding="utf-8",
            )
            sharing_path = root / "unseal_sharing.json"
            sharing_path.write_text(json.dumps({"users": ["1001", "1005"]}), encoding="utf-8")

            with (
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang.save_unseal_data"
                ) as save_data,
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang.save_sharing_users"
                ) as save_users,
            ):
                result = _migrate_unseal_data_sync(players, sharing_path)

        self.assertEqual(result, (5, 1, 2))
        save_data.assert_called_once()
        user_id, data = save_data.call_args.args
        self.assertEqual(user_id, "1001")
        self.assertEqual(data["unseal_info"], {"count": 2, "total_cost": 300, "profit": 40, "loss": 5})
        self.assertEqual(
            data["sharing_info"],
            {"shared_profit": 6, "shared_loss": 7, "received_profit": 8, "received_loss": 9},
        )
        save_users.assert_called_once_with(["1001", "1005"])

    def test_invalid_sharing_file_adds_failure_without_losing_user_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            players = root / "players"
            user_dir = players / "1001"
            user_dir.mkdir(parents=True)
            (user_dir / "unseal_data.json").write_text("{}", encoding="utf-8")
            sharing_path = root / "unseal_sharing.json"
            sharing_path.write_text(json.dumps({"users": "1001"}), encoding="utf-8")

            with (
                patch("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang.save_unseal_data"),
                patch("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang.save_sharing_users") as save_users,
            ):
                result = _migrate_unseal_data_sync(players, sharing_path)

        self.assertEqual(result, (1, 1, 1))
        save_users.assert_not_called()

    def test_missing_players_directory_returns_zero_counts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(
                _migrate_unseal_data_sync(root / "missing", root / "sharing.json"),
                (0, 0, 0),
            )


if __name__ == "__main__":
    unittest.main()
