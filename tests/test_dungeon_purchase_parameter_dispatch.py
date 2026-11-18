import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class DungeonPurchaseParameterDispatchTests(unittest.TestCase):
    def test_write_rejections_are_visible_atomic_and_recoverable(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            cases = (
                "dungeon-purchase-stone-ignore-recovery",
                "dungeon-purchase-inventory-ignore-recovery",
                "dungeon-purchase-receipt-ignore-recovery",
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    *[argument for key in cases for argument in ("--case", key)],
                    "--report",
                    str(report),
                    "--base-sha",
                    "dungeon-purchase-write-recovery-red",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertEqual({"pass": 3}, evidence["summary"])
            reasons = {
                "dungeon-purchase-stone-ignore-recovery": "dungeon_purchase_stone_write_rejected",
                "dungeon-purchase-inventory-ignore-recovery": "dungeon_purchase_inventory_write_rejected",
                "dungeon-purchase-receipt-ignore-recovery": "dungeon_purchase_receipt_write_rejected",
            }
            for row in evidence["results"]:
                with self.subTest(case=row["key"]):
                    self.assertEqual("rejection", row["outcome"])
                    self.assertEqual(reasons[row["key"]], row["evidence"]["reason"])
                    self.assertTrue(row["evidence"]["rolled_back"])
                    self.assertTrue(row["evidence"]["recovered"])
                    self.assertTrue(row["evidence"]["replay_unchanged"])


if __name__ == "__main__":
    unittest.main()
