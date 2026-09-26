import json
import subprocess
import sys
import unittest


class SignInProgressContractTests(unittest.TestCase):
    def test_lottery_default_is_feature_application(self):
        result = subprocess.run(
            [sys.executable, "scripts/check_full_refactor_progress.py", "--json"],
            check=True,
            capture_output=True,
            text=True,
        )
        data = json.loads(result.stdout)
        self.assertFalse(data["slices"]["sign_in"]["lottery_core_default_legacy"])
        self.assertFalse(data["slices"]["sign_in"]["lottery_compatibility_fallback"])
        self.assertTrue(data["slices"]["sign_in"]["lottery_scheduler_application_owned"])
        self.assertTrue(data["slices"]["sign_in"]["legacy_lottery_scheduler_disabled"])
