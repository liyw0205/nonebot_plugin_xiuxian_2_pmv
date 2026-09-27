import unittest
from datetime import datetime

from ..presentation import (
    calculate_tianti_gain_rate,
    get_active_medicine_bath,
    get_sect_fairyland_bonus,
    parse_tianti_time,
)


class TiantiPresentationTests(unittest.TestCase):
    def test_time_parser_accepts_supported_storage_formats(self):
        expected = datetime(2026, 9, 14, 10, 30, 15, 250000)
        self.assertEqual(parse_tianti_time(expected), expected)
        self.assertEqual(parse_tianti_time("2026-09-14 10:30:15.250000"), expected)
        self.assertIsNone(parse_tianti_time("not-a-time"))

    def test_bath_is_active_through_its_end_time(self):
        data = {
            "medicine_end_time": "2026-09-14 10:30:00",
            "medicine_effect": "1.5",
            "medicine_name": "灵药浴",
        }
        bath = get_active_medicine_bath(data, datetime(2026, 9, 14, 10, 30))
        self.assertEqual(bath, {
            "name": "灵药浴",
            "effect": 1.5,
            "end_time": datetime(2026, 9, 14, 10, 30),
        })
        self.assertIsNone(get_active_medicine_bath(data, datetime(2026, 9, 14, 10, 31)))

    def test_gain_rate_matches_combined_training_bonuses(self):
        data = {
            "opened_qiaoxue_detail": [
                {"effect_type": "base_per_min_ratio", "effect_value": 0.1},
                {"effect_type": "hp_gain_pct", "effect_value": 0.2},
            ],
            "medicine_end_time": "2026-09-14 10:30:00",
            "medicine_effect": "1.5",
            "medicine_name": "灵药浴",
        }
        result = calculate_tianti_gain_rate(
            data,
            base_per_min=100,
            now=datetime(2026, 9, 14, 10),
            sect_fairyland_level=2,
            spirit_vein_multiplier=1.2,
        )
        self.assertEqual((result["base_ratio"], result["gain_pct"]), (0.1, 0.2))
        self.assertEqual((result["sect_bonus"], result["per_min"]), (0.1, 261))
        self.assertAlmostEqual(result["efficiency"], 2.61)
        self.assertEqual(result["bath"]["name"], "灵药浴")

    def test_sect_bonus_is_clamped_to_supported_levels(self):
        self.assertEqual(get_sect_fairyland_bonus(-2), 0.0)
        self.assertEqual(get_sect_fairyland_bonus(11), 0.5)
        self.assertEqual(get_sect_fairyland_bonus("bad"), 0.0)


if __name__ == "__main__":
    unittest.main()
