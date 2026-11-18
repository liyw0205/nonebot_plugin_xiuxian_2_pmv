import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SectElixirRoomParameterDispatchTests(unittest.TestCase):
    def _run(self, group):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", group,
                 "--report", str(report), "--base-sha", "sect-elixir-room-fixed"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return evidence

    def test_receipt_recovery_and_adapters(self):
        evidence = self._run("sect-elixir-room-upgrade-recovery")
        self.assertEqual({"pass": 1}, evidence["summary"])
        self.assertEqual(4, evidence["results"][0]["evidence"]["adapter_contracts"])

    def test_replay_and_boundaries(self):
        evidence = self._run("sect-elixir-room-upgrade-parameters")
        self.assertEqual({"pass": 2}, evidence["summary"])


if __name__ == "__main__":
    unittest.main()
