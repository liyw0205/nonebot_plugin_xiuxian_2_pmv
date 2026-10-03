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

    def test_world_boss_daily_limit_handlers_use_feature_snapshot(self):
        facade = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertEqual(facade.count("boss_application.daily_limit_snapshot(user_id)"), 2)
        self.assertEqual(facade.count("today_battle_count = daily_limits.battle_count"), 2)
        self.assertEqual(facade.count("today_integral = daily_limits.integral"), 2)
        self.assertEqual(facade.count("today_stone = daily_limits.stone"), 2)
        self.assertNotIn("boss_limit.get_battle_count(", facade)
        self.assertNotIn("boss_limit.get_integral(", facade)
        self.assertNotIn("boss_limit.get_stone(", facade)


if __name__ == "__main__":
    unittest.main()
