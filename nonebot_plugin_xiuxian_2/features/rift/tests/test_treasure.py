from __future__ import annotations

import unittest

from ..application import RiftApplication
from ..domain import RiftTreasureResolver


class FixedRandom:
    def __init__(self, draw=1):
        self.draw = draw

    def randint(self, _start, _end):
        return self.draw

    def choice(self, values):
        return values[0]


class RiftTreasureTests(unittest.IsolatedAsyncioTestCase):
    def _resolver(self, calls):
        def weapon_provider(user_info, rank, *, random_source):
            calls.append((user_info, rank, random_source))
            return 1001, {"name": "玄铁剑", "type": "法器"}

        return RiftTreasureResolver(
            treasure_config={"type_rate": 70, "法器": {"type_rate": 1}},
            messages={"法器": ["道友获得了{}"]},
            weapon_provider=weapon_provider,
            armor_provider=lambda *_args, **_kwargs: (0, {}),
            main_provider=lambda *_args, **_kwargs: (False, 0),
            secondary_provider=lambda *_args, **_kwargs: (False, 0),
            sub_provider=lambda *_args, **_kwargs: (False, 0),
            item_lookup=lambda _item_id: None,
            format_number=str,
        )

    async def test_roll_returns_inventory_item_without_persistence(self):
        calls = []
        random_source = FixedRandom()
        event = self._resolver(calls).roll(
            {"user_id": "u", "level": "练气境"},
            2,
            random_source=random_source,
        )
        self.assertEqual("玄铁剑", event.item_name)
        self.assertEqual("道友获得了玄铁剑!", event.message)
        self.assertEqual(
            [{"id": 1001, "name": "玄铁剑", "type": "法器", "amount": 1}],
            event.outcome["items"],
        )
        self.assertIs(random_source, calls[0][2])

    async def test_application_uses_feature_treasure_resolver(self):
        calls = []
        app = RiftApplication(
            "/tmp/rift-treasure-game.db",
            "/tmp/rift-treasure-player.db",
            treasure_resolver=self._resolver(calls),
        )
        item_name, message, outcome = app.roll_treasure(
            {"user_id": "u", "level": "练气境"},
            2,
            random_source=FixedRandom(),
        )
        self.assertEqual("玄铁剑", item_name)
        self.assertEqual("道友获得了玄铁剑!", message)
        self.assertEqual(1001, outcome["items"][0]["id"])
        self.assertEqual(2, calls[0][1])


if __name__ == "__main__":
    unittest.main()
