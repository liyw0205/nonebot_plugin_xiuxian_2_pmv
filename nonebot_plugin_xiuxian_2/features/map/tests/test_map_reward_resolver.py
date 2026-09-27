import unittest

from ..rewards import MapRewardResolver


class FixedRandom:
    def random(self):
        return 0.0

    def randint(self, start, end):
        return start

    def choice(self, values):
        return values[0]


class ItemCatalog:
    def __init__(self):
        self.rank_request = None

    def get_data_by_item_id(self, item_id):
        rows = {
            "20023": {"name": "洗髓石", "type": "材料"},
            "20024": {"name": "竞技令", "type": "材料"},
            "501": {"name": "玄阶回春经", "type": "技能", "level": "玄阶"},
        }
        return rows.get(str(item_id))

    def get_random_id_list_by_rank_and_item_type(self, rank, item_type):
        self.rank_request = (rank, item_type)
        return ["501"]


class MapRewardResolverTests(unittest.TestCase):
    def setUp(self):
        self.catalog = ItemCatalog()
        self.rank_requests = []
        self.resolver = MapRewardResolver(
            reward_pools={
                "wash_stone_low": [20023],
                "arena_ticket_low": [20024],
                "stone_high": ["LS_5000000"],
                "acc_pack_low": [501],
            },
            item_catalog=self.catalog,
            rank_for_level=lambda level, cap: self.rank_requests.append((level, cap)) or 4,
            format_number=lambda value: f"N{value}",
            skill_equip_types=["功法", "法器"],
            arena_ticket_drop_ratio=0.30,
        )
        self.random = FixedRandom()

    def test_wash_stone_expands_arena_ticket_drop(self):
        rewards, stone, items = self.resolver.roll_rewards(
            [("wash_stone_low", 1, 1, 1.0)],
            random_source=self.random,
        )

        self.assertEqual((0, ["洗髓石x1", "竞技令x1"]), (stone, rewards))
        self.assertEqual([20023, 20024], [item["id"] for item in items])

    def test_stone_decay_and_mission_reward_shape_are_preserved(self):
        rewards, stone, _ = self.resolver.roll_rewards(
            [("stone_high", 1, 1, 1.0)],
            0.5,
            random_source=self.random,
        )
        mission_rewards, meta = self.resolver.roll_mission_reward(
            random_source=self.random,
        )

        self.assertEqual(("灵石xN2500000", 2500000), (rewards[0], stone))
        self.assertEqual(["灵石xN5000000", "玄阶回春经x1"], mission_rewards)
        self.assertEqual(5000000, meta["stone_delta"])
        self.assertEqual(501, meta["item_delta"][0]["id"])

    def test_skill_equip_drop_uses_injected_rank_and_item_providers(self):
        text, item = self.resolver.roll_skill_equip_drop(
            {"level": "化神境"},
            random_source=self.random,
        )

        self.assertEqual(("玄阶:玄阶回春经x1", 501), (text, item["id"]))
        self.assertEqual([("化神境", 5)], self.rank_requests)
        self.assertEqual((4, "功法"), self.catalog.rank_request)

    def test_unknown_dongfu_node_needs_no_random_draw_or_catalog_read(self):
        rewards = self.resolver.roll_dongfu_material(
            "未知节点",
            random_source=self.random,
        )

        self.assertEqual(([], 0, []), rewards)
        self.assertIsNone(self.catalog.rank_request)


if __name__ == "__main__":
    unittest.main()
