import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectElixirRoomProgressContractTests(unittest.TestCase):
    def test_upgrade_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["elixir_room_upgrade_application_owned"])

    def test_elixir_claim_activity_timestamp_uses_feature_application(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["activity_timestamp_application_owned"])
        self.assertTrue(sect["activity_timestamp_repository_owned"])
        self.assertTrue(sect["activity_timestamp_clock_injected"])
        self.assertTrue(sect["activity_timestamp_legacy_format_preserved"])
        self.assertTrue(sect["activity_timestamp_order_preserved"])
