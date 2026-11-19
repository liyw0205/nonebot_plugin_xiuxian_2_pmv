import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class AccessoryReadOnlyParameterDispatchTests(unittest.TestCase):
    def test_real_accessory_queries_use_seeded_data_without_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            cases = ("accessory-bag-page-real", "accessory-collection-real-set")
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    *[argument for key in cases for argument in ("--case", key)],
                    "--report",
                    str(report),
                    "--base-sha",
                    "364493130edffd46e031e8d57438940c54fd6673",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertEqual({"pass": 2}, evidence["summary"])
            rows = {row["key"]: row for row in evidence["results"]}
            self.assertEqual(17, rows["accessory-bag-page-real"]["evidence"]["actual_rows"])
            self.assertTrue(rows["accessory-bag-page-real"]["evidence"]["read_only"])
            self.assertEqual(4, rows["accessory-collection-real-set"]["evidence"]["actual_rows"])
            self.assertTrue(rows["accessory-collection-real-set"]["evidence"]["readonly"])


if __name__ == "__main__":
    unittest.main()
