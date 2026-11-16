from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest


class GuishiParameterDispatchTests(unittest.TestCase):
    def test_creation_replay_and_partial_cancellation_lifecycle(self):
        self.check_group("market-guishi-lifecycle", 5, {"success": 5})

    def test_orders_settlement_take_cancel_and_scheduled_return(self):
        self.check_group("market-guishi-parameters", 6, {"success": 5, "rejection": 1})

    def check_group(self, group, count, outcomes):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", group,
                 "--report", str(report), "--base-sha", "guishi-schedule-regression"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=120,
            )
            evidence = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
            failures = [row for row in evidence.get("results", []) if row["status"] != "pass"]
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False))
            self.assertEqual({"pass": count}, evidence["summary"], evidence["results"])
            self.assertEqual(outcomes, evidence["outcomes"])
            self.assertTrue(all(row["calls"] and row["evidence"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
