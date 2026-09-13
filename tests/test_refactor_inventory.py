from __future__ import annotations

import json
import unittest

from scripts.export_refactor_inventory import ROOT, build_inventory


class RefactorInventoryTests(unittest.TestCase):
    def test_checked_in_inventory_matches_source(self) -> None:
        path = ROOT / "docs" / "refactor_inventory.json"
        checked_in = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(checked_in, build_inventory())

    def test_inventory_records_required_p0_surfaces(self) -> None:
        inventory = build_inventory()
        for section in (
            "commands",
            "routes",
            "jobs",
            "legacy_routes",
            "legacy_jobs",
            "database_tables",
            "json_files",
            "database_files",
        ):
            self.assertIn(section, inventory)
            self.assertIsInstance(inventory[section], list)
        self.assertTrue(inventory["features"])
        self.assertTrue(inventory["database_tables"])


if __name__ == "__main__":
    unittest.main()
