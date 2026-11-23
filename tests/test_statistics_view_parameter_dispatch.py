from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class StatisticsViewParameterDispatchTests(unittest.TestCase):
    def test_real_statistics_queries_cover_empty_values_and_read_failure(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--group",
                    "statistics-view-parameters",
                    "--report",
                    str(report),
                    "--base-sha",
                    "5ea3de77daa3e7dda30bd5157fbca6f180cab702",
                ],
                cwd=root,
                text=True,
                capture_output=True,
                timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 3}, evidence["summary"], details)


if __name__ == "__main__":
    unittest.main()
