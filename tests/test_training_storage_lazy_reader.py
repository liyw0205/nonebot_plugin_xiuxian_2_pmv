from pathlib import Path
import unittest


class TrainingStorageLazyReaderTests(unittest.TestCase):
    def test_training_defers_player_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_training/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("PlayerDataManager", source)
        self.assertIn("training_application.get_state(user_id)", source)
        self.assertIn("training_application.leaderboard(field, limit=50)", source)
        application = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/features/training/application.py"
        ).read_text(encoding="utf-8")
        self.assertIn("return self.repository.leaderboard(field, limit=limit)", application)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)


if __name__ == "__main__":
    unittest.main()
