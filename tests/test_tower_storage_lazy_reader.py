from pathlib import Path
import unittest


class TowerStorageLazyReaderTests(unittest.TestCase):
    def test_tower_defers_player_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_tower/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("PlayerDataManager", source)
        limit = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_tower/tower_limit.py"
        ).read_text(encoding="utf-8")
        self.assertIn("tower_limit.ranking(", source)
        self.assertIn("return state_application.ranking(field, limit)", limit)
        self.assertIn("_state_application_instance = None", limit)
        self.assertIn("if _state_application_instance is None:", limit)
        self.assertIn("_state_application_instance = TowerStateApplication(", limit)
        self.assertIn("get_paths().player_db,", limit)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)


if __name__ == "__main__":
    unittest.main()
