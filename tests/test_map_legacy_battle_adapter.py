import asyncio
import unittest

from nonebot_plugin_xiuxian_2.compatibility.legacy_map_battle import (
    LegacyMapBattleRunner,
)


class LegacyMapBattleRunnerTests(unittest.TestCase):
    def test_runner_resolves_all_assets_and_passes_them_to_engine(self):
        calls = {}
        player_data = {"属性": {"nickname": "player"}}
        providers = {
            "boss_attribute_provider": lambda *args: None,
            "boss_buff_provider": lambda *args: [],
            "boss_skill_provider": lambda *args: None,
            "boss_status_updater": lambda *args: None,
            "player_status_updater": lambda *args: None,
        }

        async def battle_engine(user_id, boss, **kwargs):
            calls["engine"] = (user_id, boss, kwargs)
            return ["battle log"], "群友赢了", boss

        def player_data_provider(user_id):
            calls["player"] = user_id
            return player_data

        runner = LegacyMapBattleRunner(
            battle_engine=battle_engine,
            player_data_provider=player_data_provider,
            **providers,
        )
        boss = {"name": "守关石傀"}

        result = asyncio.run(runner("u", boss, bot_id="bot-1"))

        self.assertEqual("u", calls["player"])
        user_id, passed_boss, kwargs = calls["engine"]
        self.assertEqual(("u", boss, "bot-1", player_data), (
            user_id,
            passed_boss,
            kwargs["bot_id"],
            kwargs["player_data"],
        ))
        for name, provider in providers.items():
            self.assertIs(provider, kwargs[name])
        self.assertEqual((["battle log"], "群友赢了", boss), result)

    def test_runner_propagates_engine_failure(self):
        async def fail(*args, **kwargs):
            raise LookupError("legacy battle failed")

        runner = LegacyMapBattleRunner(
            battle_engine=fail,
            player_data_provider=lambda user_id: {},
            boss_attribute_provider=lambda *args: None,
            boss_buff_provider=lambda *args: [],
            boss_skill_provider=lambda *args: None,
            boss_status_updater=lambda *args: None,
            player_status_updater=lambda *args: None,
        )

        with self.assertRaisesRegex(LookupError, "legacy battle failed"):
            asyncio.run(runner("u", {"name": "守关石傀"}, bot_id="bot-1"))
