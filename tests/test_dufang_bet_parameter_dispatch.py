from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest


class DufangBetParameterDispatchTests(unittest.TestCase):
    def test_new_plan_preserves_parsed_cost_through_real_matcher_and_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--group",
                    "dufang-bet",
                    "--report",
                    str(report),
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
            failures = [row for row in evidence.get("results", []) if row["status"] != "pass"]
            self.assertEqual(
                result.returncode,
                0,
                result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False),
            )
            self.assertEqual(evidence.get("summary"), {"pass": 1}, evidence.get("results"))
            self.assertEqual(evidence.get("outcomes"), {"success": 1})
            self.assertTrue(evidence["results"][0]["evidence"])


if __name__ == "__main__":
    unittest.main()
