import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectJoinStateParameterDispatchTests(unittest.TestCase):
    def _run(self, group):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", group,
                 "--report", str(report), "--base-sha", "sect-join-state-fixed"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return evidence

    def test_open_close_replay_conflict_and_adapter_contract(self):
        evidence = self._run("sect-join-state-parameters")
        self.assertEqual({"pass": 1}, evidence["summary"])
        self.assertEqual(evidence["results"][0]["evidence"]["adapter_contracts"], 4)

    def test_open_close_write_rejections_recover(self):
        evidence = self._run("sect-join-state-recovery")
        self.assertEqual({"pass": 2}, evidence["summary"])
        self.assertTrue(all(row["evidence"]["recovered"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
