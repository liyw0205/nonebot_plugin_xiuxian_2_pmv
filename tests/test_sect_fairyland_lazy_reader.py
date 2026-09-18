from pathlib import Path
import unittest


class SectFairylandLazyReaderTests(unittest.TestCase):
    def test_fairyland_defers_player_data_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_fairyland.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("_player_data_manager = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().get_fields(", source)
        self.assertIn("_player_data_manager().update_or_write_data(", source)


if __name__ == "__main__":
    unittest.main()
