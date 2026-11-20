from __future__ import annotations

import unittest

from ..domain import refine_output_counts


class RefineOutputCountTests(unittest.TestCase):
    def test_fractional_bonus_is_floored_to_the_actual_reward(self) -> None:
        self.assertEqual((2, 2), refine_output_counts(2.5, 0, 10))
        self.assertEqual((2, 1), refine_output_counts(2.5, 9, 10))

    def test_recipe_progress_never_exceeds_whole_rewards_or_remaining_cap(self) -> None:
        self.assertEqual((1, 0), refine_output_counts(1.5, 10, 10))
        self.assertEqual((0, 0), refine_output_counts(0.5, 0, 10))


if __name__ == "__main__":
    unittest.main()
