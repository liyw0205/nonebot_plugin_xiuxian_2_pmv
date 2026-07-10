from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_Illusion.IllusionData import (
    DEFAULT_QUESTIONS,
    IllusionData,
)


class IllusionDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_path = Path(self.temp_dir.name) / "illusion"
        self.stats_file = self.data_path / "illusion_stats.json"
        self.path_patch = patch.multiple(
            IllusionData,
            DATA_PATH=self.data_path,
            STATS_FILE=self.stats_file,
        )
        self.path_patch.start()

    def tearDown(self) -> None:
        self.path_patch.stop()
        self.temp_dir.cleanup()

    def test_legacy_user_file_is_completed_without_losing_fields(self) -> None:
        self.data_path.mkdir(parents=True)
        user_file = self.data_path / "user-1.json"
        user_file.write_text(
            json.dumps({"today_choice": "旧选择", "custom": 7}),
            encoding="utf-8",
        )

        data = IllusionData.get_or_create_user_illusion_info("user-1")

        self.assertEqual(data["today_choice"], "旧选择")
        self.assertEqual(data["custom"], 7)
        self.assertIn("last_participate", data)
        self.assertIsInstance(data["question_index"], int)

    def test_corrupt_user_file_is_backed_up_and_recreated(self) -> None:
        self.data_path.mkdir(parents=True)
        user_file = self.data_path / "user-2.json"
        user_file.write_text("{broken", encoding="utf-8")

        data = IllusionData.get_or_create_user_illusion_info("user-2")

        self.assertEqual(data["today_choice"], None)
        self.assertTrue(list(self.data_path.glob("user-2.json.invalid.*.bak")))
        self.assertIsInstance(json.loads(user_file.read_text(encoding="utf-8")), dict)

    def test_stats_update_is_atomic_under_concurrency(self) -> None:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(lambda _: IllusionData.update_question_stats(0, 0), range(40)))

        self.assertEqual(IllusionData.get_stats()["question_stats"][0][0], 40)

    def test_stats_shape_is_repaired_and_valid_rows_are_preserved(self) -> None:
        rows = [[0] * len(question["options"]) for question in DEFAULT_QUESTIONS]
        rows[0][1] = 9
        rows[1] = [1]
        self.data_path.mkdir(parents=True)
        self.stats_file.write_text(
            json.dumps({"question_stats": rows}),
            encoding="utf-8",
        )

        stats = IllusionData.get_stats()

        self.assertEqual(stats["question_stats"][0][1], 9)
        self.assertEqual(
            stats["question_stats"][1],
            [0] * len(DEFAULT_QUESTIONS[1]["options"]),
        )

    def test_player_reset_keeps_statistics_file(self) -> None:
        IllusionData.update_question_stats(0, 0)
        IllusionData.save_user_illusion_info("user-3", {"today_choice": "选择"})

        IllusionData.reset_player_data_only()

        self.assertTrue(self.stats_file.exists())
        self.assertFalse((self.data_path / "user-3.json").exists())
        self.assertEqual(IllusionData.get_stats()["question_stats"][0][0], 1)


if __name__ == "__main__":
    unittest.main()
