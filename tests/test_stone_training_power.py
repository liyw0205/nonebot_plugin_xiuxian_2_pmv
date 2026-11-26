import sqlite3
import unittest

from nonebot_plugin_xiuxian_2.features.buff.stone_training_repository import _training_power


class StoneTrainingPowerTests(unittest.TestCase):
    def test_normal_range_preserves_sqlite_multiplication_and_half_rounding(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        for exp in (1, 3, 100, 832100, 2 ** 51, 2 ** 52 + 1):
            for rate in (0.0, .5, 1.005, 2.0, 2.5, 7.8, 1.3 * 6):
                if exp * rate > 2 ** 53:
                    continue
                with self.subTest(exp=exp, rate=rate):
                    previous = connection.execute("SELECT ROUND(?*?,0)", (exp, rate)).fetchone()[0]
                    self.assertEqual(_training_power(exp, rate), int(previous))

    def test_large_integer_uses_original_binary_multiplier_without_float_exp(self):
        for exp in (2 ** 63 + 11, 10 ** 80 + 1):
            self.assertEqual(_training_power(exp, 2.0), exp * 2)
            for rate in (7.8, 1.3 * 6):
                numerator, denominator = rate.as_integer_ratio()
                quotient, remainder = divmod(exp * numerator, denominator)
                self.assertEqual(_training_power(exp, rate), quotient + (remainder * 2 >= denominator))
        self.assertEqual(_training_power(2 ** 63 + 1, .5), 2 ** 62 + 1)

    def test_invalid_rates_are_rejected(self):
        for rate in (-1.0, float("inf"), float("nan")):
            with self.assertRaises(ValueError):
                _training_power(100, rate)
