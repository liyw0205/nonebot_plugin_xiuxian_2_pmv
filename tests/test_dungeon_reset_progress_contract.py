import unittest

from scripts.check_full_refactor_progress import _slice_status


class DungeonResetProgressContractTests(unittest.TestCase):
    def test_reset_manager_uses_feature_application(self):
        dungeon = _slice_status()["dungeon_team"]
        self.assertTrue(dungeon["reset_application_owned"])
        self.assertTrue(dungeon["legacy_dungeon_reset_service_isolated"])
        self.assertTrue(dungeon["legacy_dungeon_reset_construction_removed"])

    def test_session_service_is_compatibility_only_and_exit_uses_feature_result(self):
        dungeon = _slice_status()["dungeon_team"]
        self.assertTrue(dungeon["legacy_dungeon_session_service_isolated"])
        self.assertTrue(dungeon["legacy_dungeon_session_imports_explicit"])
        self.assertTrue(dungeon["session_exit_application_owned"])
