from pathlib import Path
import unittest


class AccessoryStorageLazyReaderTests(unittest.TestCase):
    def test_accessory_modules_defer_player_manager_construction(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_back"
        accessory = (root / "accessory.py").read_text(encoding="utf-8")
        helpers = (root / "accessory_helpers.py").read_text(encoding="utf-8")
        for source in (accessory, helpers):
            self.assertIn("_player_data_manager_instance = None", source)
            self.assertIn("def _player_data_manager(", source)
            self.assertNotIn("player_data_manager = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().patch_doc(", accessory)
        self.assertIn("_player_data_manager().get_doc(", helpers)
        self.assertIn("_player_data_manager().save_doc(", helpers)


if __name__ == "__main__":
    unittest.main()
