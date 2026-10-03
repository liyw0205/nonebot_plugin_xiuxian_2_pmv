from __future__ import annotations

import asyncio
import random
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon import (
    _dungeon_explore_random_source,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.dungeon_manager import (
    DungeonEvent,
    DungeonManager,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import player_fight


class DungeonOperationRandomTests(unittest.TestCase):
    def test_event_plan_uses_a_repeatable_operation_scoped_source(self):
        dungeon = SimpleNamespace(
            type="explore",
            events=[
                DungeonEvent(
                    {
                        "event_id": "spirit_stone",
                        "reward": {"spirit_stone": [0.2, 0.6]},
                    }
                )
            ],
        )
        dungeon.get_event_map = lambda: {event.event_id: event for event in dungeon.events}
        manager = object.__new__(DungeonManager)
        manager.current_dungeon = dungeon
        manager.sync_current_dungeon = lambda: None

        global_state = random.getstate()
        first = manager.trigger_event(
            "感气境", 1000,
            random_source=_dungeon_explore_random_source("event-1", "encounter"),
        )
        replay = manager.trigger_event(
            "感气境", 1000,
            random_source=_dungeon_explore_random_source("event-1", "encounter"),
        )
        other_operation = manager.trigger_event(
            "感气境", 1000,
            random_source=_dungeon_explore_random_source("event-2", "encounter"),
        )

        self.assertEqual(first, replay)
        self.assertNotEqual(first["stones"], other_operation["stones"])
        self.assertNotEqual(
            _dungeon_explore_random_source("event-1", "encounter").random(),
            _dungeon_explore_random_source("event-1", "battle").random(),
        )
        self.assertEqual(global_state, random.getstate())
        self.assertEqual("spirit_stone", other_operation["type"])

    def test_pve_fight_uses_the_injected_source_without_global_random_state(self):
        class FakeEntity:
            def __init__(self, attributes, team_id, is_boss=False):
                self.attributes = attributes
                self.team_id = team_id
                self.is_boss = is_boss
                self.start_skills = []
                self.skills = []

        class FakeBattleSystem:
            def __init__(self, team_a, team_b, bot_id):
                pass

            def run_battle(self):
                return [player_fight.random.random() for _ in range(3)], 0, []

        async def run(seed):
            with (
                patch.object(
                    player_fight,
                    "get_players_attributes",
                    return_value={"属性": {}, "本命法宝": None},
                ),
                patch.object(
                    player_fight,
                    "get_boss_attributes",
                    return_value={"属性": {}},
                ),
                patch.object(player_fight, "Entity", FakeEntity),
                patch.object(player_fight, "BattleSystem", FakeBattleSystem),
                patch.object(player_fight, "apply_player_buffs"),
                patch.object(player_fight, "generate_boss_buff", return_value=[]),
                patch.object(player_fight, "generate_boss_skill"),
            ):
                return await player_fight.pve_fight(
                    ["u"], [{}], type_in=0,
                    random_source=random.Random(seed),
                )

        global_state = random.getstate()
        first = asyncio.run(run("operation-1"))
        replay = asyncio.run(run("operation-1"))
        other = asyncio.run(run("operation-2"))

        self.assertEqual(first, replay)
        self.assertNotEqual(first[0], other[0])
        self.assertEqual(global_state, random.getstate())


if __name__ == "__main__":
    unittest.main()
