from __future__ import annotations

import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectInactiveOwnerProgressContractTests(unittest.TestCase):
    def test_scheduler_reads_sect_state_through_feature(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["inactive_owner_sect_state_application_owned"])
        self.assertTrue(sect["inactive_owner_sect_state_repository_owned"])
        self.assertTrue(sect["inactive_owner_sect_state_read_only"])
        self.assertTrue(sect["inactive_owner_profile_application_owned"])
        self.assertTrue(sect["inactive_owner_profile_repository_owned"])
        self.assertTrue(sect["inactive_owner_reads_fully_feature_owned"])


if __name__ == "__main__":
    unittest.main()
