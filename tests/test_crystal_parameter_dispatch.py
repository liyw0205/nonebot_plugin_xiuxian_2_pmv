from pathlib import Path
import subprocess
import sys
import unittest


class CrystalParameterDispatchTests(unittest.TestCase):
    def test_migrated_exact_crystal_prayer_dispatch(self):
        result = subprocess.run(
            [sys.executable, "-m", "tests.crystal_parameter_probe"],
            cwd=Path(__file__).resolve().parents[1],
            text=True, capture_output=True, timeout=90,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("zero exceptions/blocked", result.stdout)


if __name__ == "__main__":
    unittest.main()
