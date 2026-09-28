import unittest

from scripts.check_full_refactor_progress import _slice_status


class DungeonExploreProgressContractTests(unittest.TestCase):
    def test_explore_uses_application_and_isolates_legacy_service(self):
        dungeon = _slice_status()["dungeon_team"]
        self.assertTrue(dungeon["explore_settlement_application_owned"])
        self.assertTrue(dungeon["legacy_dungeon_explore_service_isolated"])
        self.assertTrue(dungeon["legacy_dungeon_explore_imports_explicit"])


if __name__ == "__main__":
    unittest.main()
