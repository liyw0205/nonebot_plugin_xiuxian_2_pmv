import unittest

from scripts.check_full_refactor_progress import _slice_status


class DungeonResetProgressContractTests(unittest.TestCase):
    def test_reset_manager_uses_feature_application(self):
        dungeon = _slice_status()["dungeon_team"]
        self.assertTrue(dungeon["reset_application_owned"])
        self.assertTrue(dungeon["legacy_dungeon_reset_service_isolated"])
        self.assertTrue(dungeon["legacy_dungeon_reset_construction_removed"])
