from __future__ import annotations

import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectDirectoryProgressContractTests(unittest.TestCase):
    def test_directory_reads_are_owned_by_sect_feature(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["sect_directory_application_owned"])
        self.assertTrue(sect["sect_directory_repository_owned"])
        self.assertTrue(sect["sect_directory_query_read_only"])


if __name__ == "__main__":
    unittest.main()
