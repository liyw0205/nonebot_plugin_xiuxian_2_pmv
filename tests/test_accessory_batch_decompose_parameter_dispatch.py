import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class AccessoryBatchDecomposeParameterDispatchTests(unittest.TestCase):
    def test_real_batch_decompose_writes_and_stable_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe",
                 "--case", "accessory-batch-decompose-write-recovery",
                 "--case", "accessory-batch-decompose-stable-replay",
                 "--report", str(report), "--base-sha", "3d7a59961d417b66444f5fc77f7f7ac22fec8fee"],
                cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 2}, evidence["summary"], details)


if __name__ == "__main__":
    unittest.main()
