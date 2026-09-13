from __future__ import annotations

import unittest

from nonebot_plugin_xiuxian_2.features.package_reward.resolver import PackageRewardResolver


class Random:
    def choice(self, values):
        return values[0]


class PackageRewardResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.resolver = PackageRewardResolver(Random())

    def test_roll_pool_uses_injected_random(self) -> None:
        result = self.resolver.resolve_once({"name": "礼包", "roll": 1, "roll_pool": [{"buff": 7, "name": "丹药", "type": "丹药", "amount": 2}]})
        self.assertEqual(result.rewards[0].item_id, 7)
        self.assertEqual(result.rewards[0].quantity, 2)

    def test_fixed_rewards_are_normalized(self) -> None:
        result = self.resolver.resolve_once({"name_1": "灵石", "buff_1": None, "amount_1": 100})
        self.assertEqual(result.rewards[0].name, "灵石")
        self.assertEqual(result.rewards[0].item_id, None)

    def test_empty_roll_pool_is_rejected_without_reward(self) -> None:
        result = self.resolver.resolve_once({"name": "礼包", "roll": 1, "roll_pool": []})
        self.assertEqual(result.rewards, ())
        self.assertTrue(result.errors)


if __name__ == "__main__":
    unittest.main()
