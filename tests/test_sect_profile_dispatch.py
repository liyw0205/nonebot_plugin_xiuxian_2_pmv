from pathlib import Path
import subprocess
import sys
import unittest


class SectProfileDispatchTests(unittest.TestCase):
    def test_profile_commands_only_read_sect_display_fields(self):
        result = subprocess.run(
            [sys.executable, "-m", "tests.sect_profile_probe"],
            cwd=Path(__file__).resolve().parents[1],
            text=True, capture_output=True, timeout=90,
        )
        self.assertEqual(result.returncode, 0, (result.stdout + result.stderr)[-16000:])
        self.assertIn("30 commands passed; strict financial refusals preserved", result.stdout)


if __name__ == "__main__":
    unittest.main()
