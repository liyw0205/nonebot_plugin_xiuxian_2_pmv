import unittest

from scripts.check_full_refactor_progress import _slice_status


class DungeonRewardProgressContractTests(unittest.TestCase):
    def test_legacy_reward_service_is_isolated(self):
        dungeon = _slice_status()["dungeon_team"]
        self.assertTrue(dungeon["legacy_dungeon_reward_service_isolated"])


if __name__ == "__main__":
    unittest.main()
