from pathlib import Path
import unittest


class LunhuiStorageLazyReaderTests(unittest.TestCase):
    def test_lunhui_memory_reads_use_application_without_eager_player_manager(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_lunhui/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)
        self.assertIn("lunhui_application.get_reincarnation_memory(", source)
        self.assertNotIn("_player_data_manager().get_fields(", source)
        self.assertIn("_player_data_manager().update_or_write_data(", source)


if __name__ == "__main__":
    unittest.main()
