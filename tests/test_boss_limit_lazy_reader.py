from pathlib import Path
import unittest


class BossLimitLazyReaderTests(unittest.TestCase):
    def test_boss_limit_defers_player_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/boss_limit.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().get_fields(", source)
        self.assertIn("_player_data_manager().update_or_write_data(", source)
        facade = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("boss_application.weekly_purchases(user_id)", facade)
        self.assertNotIn("boss_limit.get_weekly_purchases(", facade)


if __name__ == "__main__":
    unittest.main()
