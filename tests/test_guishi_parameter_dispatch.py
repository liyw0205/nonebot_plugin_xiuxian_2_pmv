from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest


class GuishiParameterDispatchTests(unittest.TestCase):
    def test_orders_settlement_take_cancel_and_scheduled_return(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "market-guishi-parameters",
                 "--report", str(report), "--base-sha", "guishi-schedule-regression"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=120,
            )
            self.assertEqual(
                result.returncode, 0,
                result.stdout + result.stderr + (report.read_text(encoding="utf-8") if report.exists() else ""),
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual({"pass": 4}, evidence["summary"], evidence["results"])
            self.assertEqual({"success": 3, "rejection": 1}, evidence["outcomes"])
            self.assertTrue(all(row["calls"] and row["evidence"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
