from pathlib import Path
import unittest


class TrainingStorageLazyReaderTests(unittest.TestCase):
    def test_training_defers_player_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_training/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().get_all_field_data(", source)


if __name__ == "__main__":
    unittest.main()
