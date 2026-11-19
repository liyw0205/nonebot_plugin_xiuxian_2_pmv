import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class AccessoryWashParameterDispatchTests(unittest.TestCase):
    def test_real_wash_command_rolls_back_rejected_writes_and_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--case",
                    "accessory-wash-write-recovery",
                    "--report",
                    str(report),
                    "--base-sha",
                    "a7ed8a8fe442a5efedf2a82b6ea2decc0bb94916",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 1}, evidence["summary"], details)


if __name__ == "__main__":
    unittest.main()
