import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectDonationParameterDispatchTests(unittest.TestCase):
    def _run_group(self, group, base_sha):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", group,
                 "--report", str(report), "--base-sha", base_sha],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return evidence

    def test_donation_parameters_replay_conflict_boundaries_and_adapters(self):
        evidence = self._run_group("sect-donation-parameters", "sect-donation-parameters-fixed")
        self.assertEqual({"pass": 7}, evidence["summary"])
        valid = next(row for row in evidence["results"] if row["key"] == "sect-donate-valid-amount")
        self.assertEqual(valid["status"], "pass", valid)
        self.assertEqual(valid["evidence"]["adapter_contracts"], 4)

    def test_donation_write_recovery_preserves_reason_and_assets(self):
        evidence = self._run_group("sect-donation-recovery", "sect-donation-recovery-fixed")
        self.assertEqual({"pass": 3}, evidence["summary"])
        self.assertTrue(all(row["evidence"]["complete_rollback"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
