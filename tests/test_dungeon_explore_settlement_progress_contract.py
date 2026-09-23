import unittest

from scripts.check_full_refactor_progress import _slice_status


class DungeonExploreSettlementProgressContractTests(unittest.TestCase):
    def test_explore_settlement_is_application_owned(self):
        self.assertTrue(_slice_status()["dungeon_team"]["explore_settlement_application_owned"])