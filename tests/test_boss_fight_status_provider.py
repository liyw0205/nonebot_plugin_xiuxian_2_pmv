import asyncio
import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import player_fight


class FakeEntity:
    def __init__(self, attributes, team_id, is_boss=False):
        self.attributes = attributes
        self.team_id = team_id
        self.is_boss = is_boss
        self.start_skills = []
        self.skills = []


class FakeBattleSystem:
    status = [{"u": {"hp": 75, "mp": 12}}]

    def __init__(self, team_a, team_b, bot_id):
        self.team_a = team_a
        self.team_b = team_b
        self.bot_id = bot_id

    def run_battle(self):
        return ["battle log"], 0, self.status


class BossFightStatusProviderTests(unittest.TestCase):
    def test_injected_player_updater_receives_battle_status(self):
        player_data = {"属性": {"nickname": "player"}, "本命法宝": None}
        boss = {"name": "守关石傀", "jj": "感气境"}
        calls = {}

        def boss_attributes(boss_data, bot_id):
            calls["boss_attributes"] = (boss_data, bot_id)
            return {"属性": {"nickname": boss_data["name"]}}

        def boss_buffs(boss_data):
            calls["boss_buffs"] = boss_data
            return ["buff"]

        def boss_skills(enemy, skills):
            calls["boss_skills"] = (enemy, skills)

        def boss_status(boss_data, statuses):
            calls["boss_status"] = (boss_data, statuses)

        def player_status(statuses, bot_id):
            calls["player_status"] = (statuses, bot_id)

        with (
            patch.object(player_fight, "Entity", FakeEntity),
            patch.object(player_fight, "BattleSystem", FakeBattleSystem),
            patch.object(player_fight, "apply_player_buffs"),
        ):
            result = asyncio.run(
                player_fight.Boss_fight(
                    "u",
                    boss,
                    bot_id="bot-1",
                    return_status=True,
                    player_data=player_data,
                    boss_attribute_provider=boss_attributes,
                    boss_buff_provider=boss_buffs,
                    boss_skill_provider=boss_skills,
                    boss_status_updater=boss_status,
                    player_status_updater=player_status,
                )
            )

        self.assertEqual("群友赢了", result[1])
        self.assertEqual(
            (["battle log"], "群友赢了", boss, FakeBattleSystem.status),
            result,
        )
        self.assertEqual((FakeBattleSystem.status, "bot-1"), calls["player_status"])
        self.assertEqual((boss, FakeBattleSystem.status), calls["boss_status"])
        self.assertEqual("bot-1", calls["boss_attributes"][1])
        self.assertEqual(boss, calls["boss_buffs"])
        self.assertEqual([14501, 14502], calls["boss_skills"][1])


if __name__ == "__main__":
    unittest.main()
