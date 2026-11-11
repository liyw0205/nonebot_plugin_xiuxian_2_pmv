"""Keep real command registration and singleton state in a separate process."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import unittest


class RegisteredCommandProcessTests(unittest.TestCase):
    def test_registered_command_regressions(self):
        result = subprocess.run(
            [sys.executable, "-m", "tests.registered_command_cases", "-q"],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, (result.stdout + result.stderr)[-24000:])


if __name__ == "__main__":
    unittest.main()
