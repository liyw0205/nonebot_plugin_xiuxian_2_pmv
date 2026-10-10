import json
import subprocess
import sys
import unittest


class SignInProgressContractTests(unittest.TestCase):
    def test_sign_in_side_effects_are_feature_owned(self):
        result = subprocess.run(
            [sys.executable, "scripts/check_full_refactor_progress.py", "--json"],
            check=True,
            capture_output=True,
            text=True,
        )
        data = json.loads(result.stdout)
        sign_in = data["slices"]["sign_in"]
        self.assertTrue(sign_in["task_application_owned"])
        self.assertTrue(sign_in["lottery_application_owned"])
        self.assertTrue(sign_in["legacy_fallback_explicit_only"])
        self.assertTrue(sign_in["effects_application_owned"])
        self.assertTrue(sign_in["effects_outbox_reconcile_owned"])
        self.assertTrue(sign_in["lottery_service_isolated"])
        self.assertTrue(data["slices"]["sign_in"]["lottery_scheduler_application_owned"])
        self.assertTrue(data["slices"]["sign_in"]["legacy_lottery_scheduler_disabled"])
