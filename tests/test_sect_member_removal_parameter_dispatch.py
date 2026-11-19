import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectMemberRemovalParameterDispatchTests(unittest.TestCase):
    def _run_group(self, group):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", group,
                 "--report", str(report), "--base-sha", group + "-red"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return evidence

    def test_replay_and_leave_after_membership_is_cleared(self):
        evidence = self._run_group("sect-member-removal-parameters")
        self.assertEqual({"pass": 2}, evidence["summary"])
        kick = next(row for row in evidence["results"] if row["key"] == "sect-kick-replay")
        self.assertEqual(kick["evidence"]["adapter_contracts"], 4)

    def test_member_and_receipt_write_recovery(self):
        evidence = self._run_group("sect-member-removal-recovery")
        self.assertEqual({"pass": 2}, evidence["summary"])
        self.assertTrue(all(row["evidence"]["recovered"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
