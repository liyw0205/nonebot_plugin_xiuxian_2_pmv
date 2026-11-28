from pathlib import Path
import unittest


class SeasonRankStorageLazyReaderTests(unittest.TestCase):
    def test_season_rank_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/season_rank_service.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertIn("_sql_message().", source)

    def test_season_preview_reuses_week_keyed_shared_sect_snapshot(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_base/__init__.py"
        ).read_text(encoding="utf-8")
        helper = source.split("def _get_sect_weekly_rank_fallback", 1)[1].split(
            "\n\ndef ", 1
        )[0]
        self.assertIn("sect_weekly_goal_manager.weekly_rank", helper)
        self.assertIn("week_key", helper)
        self.assertNotIn("FROM sect_weekly_goal", helper)


if __name__ == "__main__":
    unittest.main()
