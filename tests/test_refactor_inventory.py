from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import export_refactor_inventory
from scripts.export_refactor_inventory import ROOT, build_inventory


class RefactorInventoryTests(unittest.TestCase):
    def test_table_inventory_ignores_python_imports_comments_and_docstrings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "tables.py").write_text(
                '"""Read from decoy_docs."""\n'
                'from weakref import WeakSet\n'
                '# Restore from decoy_comment.\n'
                'def query():\n'
                '    """Read from decoy_function_docs."""\n'
                '    sql = "CREATE TABLE IF NOT EXISTS actual_table (id INTEGER)"\n'
                '    error = "partner usage update did not persist exactly"\n'
                '    trigger_error = "join receipt failed"\n'
                '    qualified = "SELECT * FROM game_data.user_xiuxian AS profile JOIN player_data.partner AS partner ON 1=1"\n'
                '    update = "UPDATE player_data.actual_table SET value=1"\n'
                '    quoted_create = \'CREATE TABLE "quoted_create" (id INTEGER)\'\n'
                '    quoted_read = \'SELECT * FROM "quoted_read" WHERE id=1\'\n'
                '    quoted_insert = \'INSERT INTO main."quoted_insert" (id) VALUES(1)\'\n'
                '    quoted_update = \'UPDATE OR IGNORE main."quoted_update" SET id=1\'\n'
                '    return f"SELECT * FROM actual_table JOIN second_table ON 1=1 WHERE id={1}"\n',
                encoding="utf-8",
            )
            with patch.object(export_refactor_inventory, "PACKAGE", root):
                self.assertEqual(
                    ["actual_table", "partner", "quoted_create", "quoted_insert", "quoted_read", "quoted_update", "second_table", "user_xiuxian"],
                    export_refactor_inventory._database_tables(),
                )

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
