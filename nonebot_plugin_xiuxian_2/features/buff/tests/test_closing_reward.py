import unittest

from ..closing_reward import ClosingRewardCalculator


class ClosingRewardCalculatorTests(unittest.TestCase):
    def test_normal_closing_calculates_gain_and_recovery(self):
        result = ClosingRewardCalculator.calculate(
            exp_time=10,
            current_exp=100,
            current_stone=50,
            current_hp=20,
            current_mp=10,
            exp_cap=2000,
            closing_exp=100,
            level_rate=2,
            realm_rate=1.5,
            main_rate=0.1,
            closing_rate=0.2,
            blessed_rate=0.3,
            spirit_vein_multiplier=1.2,
            spirit_vein_message="，灵脉加持！",
        )

        self.assertEqual(
            (5148, 2000, 0, 10, 100, 50),
            (
                result.base_exp,
                result.exp_gain,
                result.stone_cost,
                result.exp_time,
                result.hp_gain,
                result.mp_gain,
            ),
        )
        self.assertEqual((2100, 120, 60, 210, 6300), (result.new_exp, result.new_hp, result.new_mp, result.new_atk, result.new_power))
        self.assertTrue(result.reached_limit)
        self.assertEqual(2.6, result.efficiency)
        self.assertEqual("，灵脉加持！", result.spirit_vein_message)

    def test_stone_closing_spends_available_stone_before_spirit_bonus(self):
        result = ClosingRewardCalculator.calculate(
            exp_time=1,
            current_exp=100,
            current_stone=70,
            current_hp=0,
            current_mp=0,
            exp_cap=250,
            closing_exp=100,
            level_rate=1,
            realm_rate=1,
            stone_exit=True,
            spirit_vein_multiplier=2,
        )

        self.assertEqual(100, result.base_exp)
        self.assertEqual(70, result.stone_cost)
        self.assertEqual(250, result.exp_gain)
        self.assertFalse(result.reached_limit)

    def test_cap_is_checked_before_stone_exit_and_zero_cap_is_stable(self):
        capped = ClosingRewardCalculator.calculate(
            exp_time=1,
            current_exp=100,
            current_stone=500,
            current_hp=0,
            current_mp=0,
            exp_cap=150,
            closing_exp=100,
            level_rate=1,
            realm_rate=1,
            stone_exit=True,
            spirit_vein_multiplier=2,
            spirit_vein_message="灵脉",
        )
        empty = ClosingRewardCalculator.calculate(
            exp_time=10,
            current_exp=100,
            current_stone=500,
            current_hp=10,
            current_mp=10,
            exp_cap=0,
            closing_exp=100,
            level_rate=1,
            realm_rate=1,
            stone_exit=True,
        )

        self.assertEqual((150, 0), (capped.exp_gain, capped.stone_cost))
        self.assertTrue(capped.reached_limit)
        self.assertEqual("灵脉", capped.spirit_vein_message)
        self.assertEqual((0, 100, 50), (empty.exp_gain, empty.new_exp, empty.new_hp))
        self.assertTrue(empty.reached_limit)


if __name__ == "__main__":
    unittest.main()
