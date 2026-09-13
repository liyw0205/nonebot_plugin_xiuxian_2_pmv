import unittest

from ..domain import decide_stone_training


class StoneTrainingDomainTests(unittest.TestCase):
    def test_caps_gain_and_charges_only_real_gain(self):
        decision = decide_stone_training(old_hp=95, requested_stone=100, hp_cap=100)
        self.assertEqual(decision.status, "trained")
        self.assertEqual(decision.hp_gain, 5)
        self.assertEqual(decision.stone_cost, 50)
        self.assertEqual(decision.new_hp, 100)

    def test_at_cap_does_not_charge(self):
        decision = decide_stone_training(old_hp=100, requested_stone=100, hp_cap=100)
        self.assertEqual(decision.status, "at_cap")
        self.assertEqual(decision.stone_cost, 0)
        self.assertEqual(decision.new_hp, 100)


if __name__ == "__main__":
    unittest.main()
