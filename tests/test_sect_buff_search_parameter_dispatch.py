import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectBuffSearchParameterDispatchTests(unittest.TestCase):
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

    def test_search_receipt_ignore_recovers_same_message(self):
        evidence = self._run_group("sect-buff-search-handoff", "sect-buff-search-fixed")
        self.assertEqual({"pass": 1}, evidence["summary"])
        row = evidence["results"][0]
        self.assertEqual(row["status"], "pass", row)
        self.assertTrue(row["evidence"]["rollback"])

    def test_both_search_commands_cover_parameters_and_boundaries(self):
        evidence = self._run_group("sect-buff-search-parameters", "sect-buff-search-parameters-fixed")
        self.assertEqual({"pass": 4}, evidence["summary"])
        self.assertEqual(
            {"sect-buff-search-main-replay", "sect-buff-search-secondary-replay",
             "sect-buff-search-resource-boundary", "sect-buff-search-owner-boundary"},
            {row["key"] for row in evidence["results"]},
        )


if __name__ == "__main__":
    unittest.main()
