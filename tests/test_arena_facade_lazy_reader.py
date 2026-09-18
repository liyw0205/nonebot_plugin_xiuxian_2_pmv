from pathlib import Path
import unittest


class ArenaFacadeLazyReaderTests(unittest.TestCase):
    def test_arena_facade_defers_player_manager_and_state_service(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_arena"
        facade = (root / "__init__.py").read_text(encoding="utf-8")
        limit = (root / "arena_limit.py").read_text(encoding="utf-8")

        self.assertIn("_player_data_manager_instance = None", facade)
        self.assertIn("def _player_data_manager(", facade)
        self.assertNotIn("player_data_manager = PlayerDataManager()", facade)
        self.assertIn("_player_data_manager().get_all_field_data(", facade)

        self.assertIn("_player_data_manager_instance = None", limit)
        self.assertIn("_state_service_instance = None", limit)
        self.assertIn("def _state_service(", limit)
        self.assertNotIn("player_data_manager = PlayerDataManager()", limit)
        self.assertNotIn("player_data_manager.lock", limit)


if __name__ == "__main__":
    unittest.main()
