import unittest

from scripts.check_full_refactor_progress import _slice_status


class DungeonPurchaseProgressContractTests(unittest.TestCase):
    def test_purchase_is_application_owned_and_legacy_service_isolated(self):
        dungeon = _slice_status()["dungeon_team"]
        self.assertTrue(dungeon["dungeon_purchase_application_owned"])
        self.assertTrue(dungeon["legacy_dungeon_purchase_service_isolated"])
        self.assertTrue(dungeon["legacy_dungeon_purchase_imports_explicit"])


if __name__ == "__main__":
    unittest.main()
