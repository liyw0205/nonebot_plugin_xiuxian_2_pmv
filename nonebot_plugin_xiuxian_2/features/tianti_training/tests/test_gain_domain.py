import unittest

from ..domain import decide_tianti_gain


class TiantiGainDomainTests(unittest.TestCase):
    def test_gain_matches_formula_and_cap(self):
        result = decide_tianti_gain(minutes=10, base_per_min=100, base_ratio=0.1, gain_pct=0.2, bath_effect=1.5, sect_bonus=0.1, spirit_vein_multiplier=1.05, old_hp=900, hp_cap=1000)
        self.assertEqual((result.gain, result.new_hp, result.real_gain), (2286, 1000, 100))

    def test_zero_minutes_is_noop(self):
        result = decide_tianti_gain(minutes=0, base_per_min=100, base_ratio=0, gain_pct=0, bath_effect=1, sect_bonus=0, spirit_vein_multiplier=1, old_hp=10, hp_cap=100)
        self.assertEqual((result.gain, result.new_hp, result.real_gain), (0, 10, 0))


if __name__ == "__main__":
    unittest.main()
