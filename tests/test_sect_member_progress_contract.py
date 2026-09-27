from __future__ import annotations

import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectMemberProgressContractTests(unittest.TestCase):
    def test_all_sect_facade_member_lists_are_feature_owned(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["sect_member_list_application_owned"])
        self.assertTrue(sect["sect_member_list_repository_owned"])
        self.assertTrue(sect["sect_member_list_read_only"])
        self.assertTrue(sect["sect_member_utils_application_injected"])
        self.assertTrue(sect["sect_member_utils_info_reads_feature_owned"])
        self.assertTrue(sect["sect_member_utils_join_count_feature_owned"])


if __name__ == "__main__":
    unittest.main()
