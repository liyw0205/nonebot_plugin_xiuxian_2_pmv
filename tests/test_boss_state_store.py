from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.old_boss_info import (
    OLD_BOSS_INFO,
)


class BossStateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.path = root / "boss_info.json"
        self.player_db = root / "player.sqlite3"
        # isolate world_boss_state from the live host database
        self._paths_patch = patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.old_boss_info.get_paths",
            return_value=type("P", (), {"player_db": self.player_db})(),
        )
        self._paths_patch.start()

    def tearDown(self) -> None:
        self._paths_patch.stop()
        self.temp_dir.cleanup()

    def test_missing_state_is_initialized_and_round_trips(self) -> None:
        store = OLD_BOSS_INFO(self.path)
        # DB truth always exposes the global key (empty list when uninitialized).
        self.assertEqual(store.read_boss_info(), {"global": []})
        self.assertFalse(self.path.exists())
        self.assertTrue(store.save_boss({"global": [{"name": "测试Boss"}]}))
        self.assertEqual(
            OLD_BOSS_INFO(self.path).read_boss_info()["global"][0]["name"],
            "测试Boss",
        )

    def test_invalid_json_is_backed_up_before_reset(self) -> None:
        self.path.write_text("{broken", encoding="utf-8")

        store = OLD_BOSS_INFO(self.path)

        self.assertEqual(store.read_boss_info(), {"global": []})
        self.assertTrue(list(self.path.parent.glob("boss_info.json.invalid.*.bak")))

    def test_wrong_root_type_is_backed_up_before_reset(self) -> None:
        self.path.write_text("[]", encoding="utf-8")

        store = OLD_BOSS_INFO(self.path)

        self.assertEqual(store.read_boss_info(), {"global": []})
        self.assertTrue(list(self.path.parent.glob("boss_info.json.invalid.*.bak")))

    def test_two_instances_merge_against_latest_disk_state(self) -> None:
        first = OLD_BOSS_INFO(self.path)
        second = OLD_BOSS_INFO(self.path)
        first.save_boss({"global": [{"name": "Boss A"}]})

        second.save_boss({"group-1": [{"name": "Boss B"}]})

        stored = OLD_BOSS_INFO(self.path).read_boss_info()
        self.assertIn("global", stored)
        self.assertIn("group-1", stored)

    def test_filesystem_errors_are_not_silently_swallowed(self) -> None:
        store = OLD_BOSS_INFO(self.path)
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.old_boss_info.update_json_file",
            side_effect=OSError("disk unavailable"),
        ):
            with self.assertRaises(OSError):
                store.save_boss({"global": []})


if __name__ == "__main__":
    unittest.main()
