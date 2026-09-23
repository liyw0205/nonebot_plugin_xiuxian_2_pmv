import unittest
from scripts.check_full_refactor_progress import _slice_status

class PastLifeProgressContractTests(unittest.TestCase):
    def test_start_and_choice_application_owned(self):
        slices=_slice_status()
        self.assertTrue(slices['past_life']['start_application_owned'])
        self.assertTrue(slices['past_life']['choice_application_owned'])
