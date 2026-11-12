from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest


class CurrencyParameterDispatchTests(unittest.TestCase):
    def test_exact_currency_settlement_and_refusal_dispatch(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.currency_parameter_cases", "--group", "currency-precision",
                 "--report", str(report), "--base-sha", "currency-exact-regression"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual({"pass": 12}, evidence["summary"], evidence["results"])
            self.assertEqual({"success": 7, "rejection": 5}, evidence["outcomes"])
            self.assertTrue(all(row["calls"] and row["evidence"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
