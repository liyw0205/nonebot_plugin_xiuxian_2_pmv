import unittest

from ..domain import decide_interactive_reward


class InteractiveRewardDecisionTests(unittest.TestCase):
    def test_low_roll_uses_stone_only_plan(self):
        result = decide_interactive_reward(0.05, "herb_low")
        self.assertEqual((('stone_low', 1, 1, 1.0),), result.plan)
        self.assertTrue(result.message)

    def test_lucky_roll_uses_expanded_plan(self):
        result = decide_interactive_reward(0.20, "herb_low")
        self.assertEqual("herb_low", result.plan[0][0])
        self.assertEqual("wash_stone_low", result.plan[-1][0])

    def test_normal_roll_uses_standard_plan(self):
        result = decide_interactive_reward(0.80, "herb_low")
        self.assertEqual((('herb_low', 1, 2, 1.0), ('stone_low', 1, 1, 0.55)), result.plan)
        self.assertEqual("", result.message)

    def test_invalid_roll_is_rejected(self):
        with self.assertRaises(ValueError):
            decide_interactive_reward(1.1, "herb_low")


if __name__ == "__main__":
    unittest.main()
