import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectPracticeParameterDispatchTests(unittest.TestCase):
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

    def test_receipt_ignore_recovers_same_practice_command(self):
        evidence = self._run_group("sect-practice-handoff", "sect-practice-fixed")
        self.assertEqual({"pass": 2}, evidence["summary"])
        self.assertTrue(all(row["status"] == "pass" for row in evidence["results"]), evidence)
        self.assertTrue(all(row["evidence"]["rollback"] for row in evidence["results"]), evidence)

    def test_three_practice_commands_and_boundaries(self):
        evidence = self._run_group("sect-practice-parameters", "sect-practice-parameters-fixed")
        self.assertEqual({"pass": 3}, evidence["summary"])
        self.assertEqual(
            {"sect-practice-three-types-replay-conflict", "sect-practice-argument-boundaries",
             "sect-practice-resource-position-boundaries"},
            {row["key"] for row in evidence["results"]},
        )


if __name__ == "__main__":
    unittest.main()
