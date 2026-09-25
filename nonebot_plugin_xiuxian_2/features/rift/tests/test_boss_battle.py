from __future__ import annotations

import unittest

from ..application import RiftApplication
from ..domain import RiftBossBattleResolver


class FixedRandom:
    def choice(self, values):
        return values[0]


class RiftBossBattleTests(unittest.IsolatedAsyncioTestCase):
    def _resolver(self, calls, *, with_assets=False):
        async def battle_runner(*args, **kwargs):
            calls.append((args, kwargs))
            return ["battle"], "群友赢了", args[1], [{"player": {"user_id": "u", "hp": 80, "mp": 35}}]

        resolver_kwargs = {}
        if with_assets:
            resolver_kwargs["player_asset_provider"] = lambda user_id: {
                "属性": {"user_id": user_id},
                "其他": {},
            }
        return RiftBossBattleResolver(
            boss_config={
                "Boss数据": {"name": ["墨蛟"], "hp": [1.0], "mp": 2, "atk": [0.1]},
                "success": {"desc": ["击败{}"], "give": {"exp": [0.1], "stone": 100}},
                "fail": {"desc": ["败给{}"]},
            },
            battle_runner=battle_runner,
            rank_score=lambda level: {"练气境圆满": 10, "练气境": 8}[level],
            level_power=lambda level: {"练气境初期": 1000}[level],
            max_exp_factor=1.0,
            exp_reward=lambda *_args, **_kwargs: 50,
            format_number=str,
            **resolver_kwargs,
        )

    async def test_roll_returns_reward_and_combat_delta_without_persistence(self):
        calls = []
        event = await self._resolver(calls).roll(
            {"user_id": "u", "exp": 100, "hp": 100, "mp": 50, "level": "练气境"},
            2,
            "bot",
            random_source=FixedRandom(),
        )
        self.assertTrue(event.victory)
        self.assertEqual({"hp": -20, "mp": -15, "exp": 50, "stone": 400}, event.outcome["delta"])
        self.assertEqual("击败墨蛟获得了修为：50点，灵石：400枚！", event.message)
        self.assertEqual(0, calls[0][1]["type_in"])

    async def test_asset_provider_is_passed_as_a_snapshot_to_the_runner(self):
        calls = []
        await self._resolver(calls, with_assets=True).roll(
            {"user_id": "u", "exp": 100, "hp": 100, "mp": 50, "level": "练气境"},
            2,
            "bot",
            random_source=FixedRandom(),
        )
        self.assertEqual("u", calls[0][1]["player_data"]["属性"]["user_id"])

    async def test_boss_engine_providers_are_passed_to_the_runner(self):
        calls = []
        providers = {
            "boss_attribute_provider": lambda boss, bot_id: {"属性": {"user_id": bot_id}},
            "boss_buff_provider": lambda boss: [],
            "boss_skill_provider": lambda enemy, skills: None,
            "boss_status_updater": lambda boss, status: None,
        }
        resolver = self._resolver(calls)
        resolver.boss_attribute_provider = providers["boss_attribute_provider"]
        resolver.boss_buff_provider = providers["boss_buff_provider"]
        resolver.boss_skill_provider = providers["boss_skill_provider"]
        resolver.boss_status_updater = providers["boss_status_updater"]

        random_source = FixedRandom()
        await resolver.roll(
            {"user_id": "u", "exp": 100, "hp": 100, "mp": 50, "level": "练气境"},
            2,
            "bot",
            random_source=random_source,
        )

        for name, provider in providers.items():
            self.assertIs(provider, calls[0][1][name])
        self.assertIs(random_source, calls[0][1]["boss_buff_random_source"])

    async def test_application_uses_the_feature_resolver(self):
        calls = []
        app = RiftApplication(
            "/tmp/rift-boss-game.db",
            "/tmp/rift-boss-player.db",
            boss_battle_resolver=self._resolver(calls),
        )
        battle_result, message, outcome = await app.roll_boss_battle(
            {"user_id": "u", "exp": 100, "hp": 100, "mp": 50, "level": "练气境"},
            2,
            "bot",
            random_source=FixedRandom(),
        )
        self.assertEqual(["battle"], battle_result)
        self.assertEqual("击败墨蛟获得了修为：50点，灵石：400枚！", message)
        self.assertEqual(400, outcome["delta"]["stone"])
        self.assertEqual(0, calls[0][1]["type_in"])


if __name__ == "__main__":
    unittest.main()
