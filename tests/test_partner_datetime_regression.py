import unittest
from datetime import datetime, timezone

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.relation_utils import _parse_datetime


class _FixedClock:
    def __init__(self, value):
        self.value = value

    def now(self):
        return self.value


class PartnerDatetimeRegressionTests(unittest.TestCase):
    NOW = datetime(2026, 11, 16, 6, 51, 46, tzinfo=timezone.utc)

    def setUp(self):
        self._clock = partner.runtime_clock
        self._load_mentor = partner.load_mentor
        partner.runtime_clock = _FixedClock(self.NOW)

    def tearDown(self):
        partner.runtime_clock = self._clock
        partner.load_mentor = self._load_mentor

    def test_legacy_relation_text_is_utc_aware(self):
        parsed = _parse_datetime("2026-11-17 00:00:00")

        self.assertEqual(timezone.utc, parsed.tzinfo)
        self.assertEqual(
            datetime(2026, 11, 17, tzinfo=timezone.utc), parsed
        )

    def test_mentor_time_windows_compare_with_runtime_clock(self):
        values = {
            "mentor_cd_until": "2026-11-17 00:00:00",
            "apprentice_cd_until": "2026-11-17 00:00:00",
            "mentor_apply_time": "2026-11-16 00:00:00",
            "bind_time": "2026-11-16 00:00:00",
            "mentor_rebind_cd": '{"mentor": "2026-11-17 00:00:00"}',
        }
        partner.load_mentor = lambda _user_id: values

        expected_cooldown = int(
            (
                datetime(2026, 11, 17, tzinfo=timezone.utc)
                - self.NOW
            ).total_seconds()
        )
        expected_day_window = int(
            (
                datetime(2026, 11, 17, tzinfo=timezone.utc)
                - self.NOW
            ).total_seconds()
        )

        self.assertEqual(
            expected_cooldown,
            partner._cooldown_remaining("mentor", "mentor_cd_until"),
        )
        self.assertEqual(
            expected_day_window,
            partner._get_mentor_apply_remaining("apprentice")[0],
        )
        self.assertEqual(
            expected_cooldown,
            partner._get_pair_rebind_remaining("apprentice", "mentor"),
        )
        self.assertEqual(
            expected_day_window,
            partner._get_bind_wait_remaining("apprentice"),
        )

    def test_expired_cooldowns_are_released_without_frame_errors(self):
        partner.load_mentor = lambda _user_id: {
            "mentor_cd_until": "2026-11-16 06:51:45",
            "mentor_rebind_cd": '{"mentor": "2026-11-16 06:51:45"}',
        }

        self.assertEqual(0, partner._cooldown_remaining("mentor", "mentor_cd_until"))
        self.assertEqual(0, partner._get_pair_rebind_remaining("apprentice", "mentor"))


if __name__ == "__main__":
    unittest.main()
