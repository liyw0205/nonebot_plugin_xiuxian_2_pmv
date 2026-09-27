from __future__ import annotations

import unittest
from pathlib import Path

from scripts.check_full_refactor_progress import _slice_status


class SectDirectoryProgressContractTests(unittest.TestCase):
    def test_directory_reads_are_owned_by_sect_feature(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["sect_directory_application_owned"])
        self.assertTrue(sect["sect_directory_repository_owned"])
        self.assertTrue(sect["sect_directory_query_read_only"])
        self.assertTrue(sect["sect_active_names_repository_owned"])
        self.assertTrue(sect["sect_active_names_application_owned"])

    def test_rank_commands_use_directory_application(self):
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_sect"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("sect_application.list_sect_scale_rank()", source)
        self.assertIn("sect_application.list_sect_combat_power_rank()", source)
        self.assertNotIn("_sql_message().scale_top()", source)
        self.assertNotIn("_sql_message().combat_power_top()", source)


if __name__ == "__main__":
    unittest.main()
