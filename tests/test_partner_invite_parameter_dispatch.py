import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class PartnerInviteParameterDispatchTests(unittest.TestCase):
    def test_reject_and_accept_write_failures_recover_through_real_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group",
                 "partner-invite-parameters", "--report", str(report),
                 "--base-sha", "partner-invite-write-recovery-red"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
            failures = [row for row in evidence.get("results", []) if row["status"] != "pass"]
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False))
            self.assertEqual({"pass": 2}, evidence["summary"], evidence.get("results"))
            self.assertEqual({"rejection": 2}, evidence["outcomes"])
            self.assertTrue(all(row["calls"] and row["evidence"]["recovered"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
