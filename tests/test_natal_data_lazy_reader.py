from pathlib import Path
import unittest


class NatalDataLazyReaderTests(unittest.TestCase):
    def test_natal_data_defers_player_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_natal_treasure/natal_data.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().get_fields(", source)
        self.assertIn("_player_data_manager().update_or_write_data(", source)
        self.assertIn("_player_data_manager().get_field_data(", source)


if __name__ == "__main__":
    unittest.main()
