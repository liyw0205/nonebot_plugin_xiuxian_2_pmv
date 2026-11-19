import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectPositionParameterDispatchTests(unittest.TestCase):
    def test_position_replay_conflict_and_adapter_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "sect-position-parameters",
                 "--report", str(report), "--base-sha", "sect-position-parameters-red"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual({"pass": 1}, evidence["summary"])
            row = evidence["results"][0]
            self.assertEqual(row["evidence"]["adapter_contracts"], 4)
            self.assertIn("该操作号已用于不同内容", row["evidence"]["conflict"])

    def test_position_write_rejections_roll_back_and_recover(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "sect-position-recovery",
                 "--report", str(report), "--base-sha", "sect-position-recovery-red"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual({"pass": 2}, evidence["summary"])
            self.assertTrue(all(row["evidence"]["recovered"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
