from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class ImpartParameterDiagnosticsTests(unittest.TestCase):
    def test_real_impart_parameter_dispatch_contracts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiuxian-impart-parameters-") as directory:
            report = Path(directory) / "report.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--group",
                    "impart-parameters",
                    "--report",
                    str(report),
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(report.read_text(encoding="utf-8"))
            statuses = {row["status"] for row in payload["results"]}
            self.assertEqual(payload["summary"], {"pass": 20}, result.stdout)
            self.assertEqual(statuses, {"pass"})
            self.assertEqual(payload["outcomes"], {"success": 8, "rejection": 12})


if __name__ == "__main__":
    unittest.main()
