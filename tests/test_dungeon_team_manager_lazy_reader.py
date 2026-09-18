from pathlib import Path
import unittest


class DungeonTeamManagerLazyReaderTests(unittest.TestCase):
    def test_team_manager_defers_player_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/team_manager.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().get_all_records(", source)
        self.assertIn("_player_data_manager().get_fields(", source)


if __name__ == "__main__":
    unittest.main()
