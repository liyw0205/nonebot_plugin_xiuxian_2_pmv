import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.check_full_refactor_progress import _slice_status


class SectTaskSettlementProgressContractTests(unittest.TestCase):
    def test_task_settlement_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["task_settlement_application_owned"])

    def test_clock_gate_accepts_timezone_but_rejects_missing_clock(self):
        self.assertTrue(_slice_status()['sect']['task_settlement_clock_injected'])
        original_read = Path.read_text
        def without_clock(path, *args, **kwargs):
            source = original_read(path, *args, **kwargs)
            if path.as_posix().endswith('/features/sect/application.py'):
                return source.replace('clock=self.clock', 'clock=None')
            return source
        with patch.object(Path, 'read_text', without_clock):
            self.assertFalse(_slice_status.__wrapped__()['sect']['task_settlement_clock_injected'])
