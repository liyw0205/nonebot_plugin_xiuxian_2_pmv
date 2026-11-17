import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectOwnerTransferParameterDispatchTests(unittest.TestCase):
    def _run(self, group):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", group,
                 "--report", str(report), "--base-sha", "sect-owner-transfer-fixed"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return evidence

    def test_owner_transfer_replay_conflict_and_adapter_contract(self):
        evidence = self._run("sect-owner-transfer-parameters")
        self.assertEqual({"pass": 1}, evidence["summary"])
        row = evidence["results"][0]
        self.assertEqual(row["evidence"]["adapter_contracts"], 4)
        self.assertIn("该操作号已用于不同内容", row["evidence"]["conflict"])

    def test_owner_transfer_write_rejections_recover(self):
        evidence = self._run("sect-owner-transfer-recovery")
        self.assertEqual({"pass": 2}, evidence["summary"])
        self.assertTrue(all(row["evidence"]["recovered"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
