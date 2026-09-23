import unittest

from scripts.check_full_refactor_progress import _slice_status


class DungeonResetProgressContractTests(unittest.TestCase):
    def test_reset_manager_uses_feature_application(self):
        self.assertTrue(_slice_status()["dungeon_team"]["reset_application_owned"])