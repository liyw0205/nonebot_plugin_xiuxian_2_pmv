import unittest

from ..domain import decide_breakthrough


class BreakthroughDomainTests(unittest.TestCase):
    def test_success_consumes_hp_and_advances(self):
        decision = decide_breakthrough(old_level="one", next_level="two", cultivation_rank=1, required_rank=2, old_hp=100, required_hp=80, roll_success=True)
        self.assertEqual((decision.status, decision.new_level, decision.hp_cost, decision.new_hp), ("completed", "two", 5, 95))

    def test_failed_roll_consumes_hp_without_advancing(self):
        decision = decide_breakthrough(old_level="one", next_level="two", cultivation_rank=1, required_rank=2, old_hp=100, required_hp=80, roll_success=False)
        self.assertEqual((decision.status, decision.new_level, decision.hp_cost, decision.new_hp), ("completed", "one", 5, 95))

    def test_rejections_do_not_consume_hp(self):
        for kwargs, status in (
            ({"next_level": None, "cultivation_rank": 1, "required_rank": 2, "old_hp": 100, "required_hp": 80}, "max_level"),
            ({"next_level": "two", "cultivation_rank": 3, "required_rank": 2, "old_hp": 100, "required_hp": 80}, "cultivation_insufficient"),
            ({"next_level": "two", "cultivation_rank": 1, "required_rank": 2, "old_hp": 50, "required_hp": 80}, "hp_insufficient"),
        ):
            decision = decide_breakthrough(old_level="one", roll_success=True, **kwargs)
            self.assertEqual(decision.status, status)
            self.assertEqual((decision.hp_cost, decision.new_hp), (0, kwargs["old_hp"]))


if __name__ == "__main__":
    unittest.main()
