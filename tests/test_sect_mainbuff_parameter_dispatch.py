import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectMainbuffParameterDispatchTests(unittest.TestCase):
    def _run_group(self, group, base_sha):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--group",
                    group,
                    "--report",
                    str(report),
                    "--base-sha",
                    base_sha,
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=120,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return evidence

    def test_receipt_ignore_is_reproduced_through_real_command_path(self):
        evidence = self._run_group("sect-mainbuff-handoff", "sect-mainbuff-fixed")
        self.assertEqual({"pass": 1}, evidence["summary"])
        row = evidence["results"][0]
        self.assertEqual(row["status"], "pass", row)
        self.assertEqual(row["evidence"]["receipt_count"], [[0]])
        self.assertEqual(row["evidence"]["recovery"]["receipt_count"], 1)

    def test_mainbuff_parameter_matrix_uses_real_command_path(self):
        evidence = self._run_group("sect-mainbuff-parameters", "sect-mainbuff-parameters-fixed")
        self.assertEqual({"pass": 4}, evidence["summary"])
        self.assertEqual(
            {"sect-mainbuff-valid-replay", "sect-mainbuff-invalid-arguments",
             "sect-mainbuff-materials-insufficient", "sect-mainbuff-position-forbidden"},
            {row["key"] for row in evidence["results"]},
        )


if __name__ == "__main__":
    unittest.main()
