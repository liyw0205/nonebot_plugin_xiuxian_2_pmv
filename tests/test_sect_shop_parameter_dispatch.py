import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectShopParameterDispatchTests(unittest.TestCase):
    def test_shop_receipt_recovery_and_adapter_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--group",
                    "sect-shop-recovery",
                    "--report",
                    str(report),
                    "--base-sha",
                    "sect-shop-write-fixed",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual({"pass": 3}, evidence["summary"])
            receipt_case = next(row for row in evidence["results"] if row["key"] == "sect-shop-receipt-recovery")
            self.assertEqual(4, receipt_case["evidence"]["adapter_contracts"])


if __name__ == "__main__":
    unittest.main()
