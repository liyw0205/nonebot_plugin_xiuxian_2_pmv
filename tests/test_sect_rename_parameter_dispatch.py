import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectRenameParameterDispatchTests(unittest.TestCase):
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

    def test_rename_replay_and_operation_conflict(self):
        evidence = self._run_group("sect-rename-parameters", "sect-rename-parameters-red")
        self.assertEqual({"pass": 2}, evidence["summary"])
        row = next(row for row in evidence["results"] if row["key"] == "sect-rename-replay-conflict")
        self.assertTrue(row["evidence"]["unchanged"])
        self.assertEqual(row["evidence"]["adapter_contracts"], 4)

    def test_rename_write_rejections_roll_back_and_recover(self):
        evidence = self._run_group("sect-rename-recovery", "sect-rename-recovery-red")
        self.assertEqual({"pass": 3}, evidence["summary"])
        self.assertTrue(all(row["evidence"]["complete_rollback"] and row["evidence"]["recovered"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
