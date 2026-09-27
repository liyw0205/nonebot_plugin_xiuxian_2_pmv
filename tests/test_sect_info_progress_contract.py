from __future__ import annotations

import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectInfoProgressContractTests(unittest.TestCase):
    def test_sect_info_reads_are_owned_by_feature(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["sect_info_application_owned"])
        self.assertTrue(sect["sect_info_repository_owned"])
        self.assertTrue(sect["sect_info_repository_read_only"])


if __name__ == "__main__":
    unittest.main()
