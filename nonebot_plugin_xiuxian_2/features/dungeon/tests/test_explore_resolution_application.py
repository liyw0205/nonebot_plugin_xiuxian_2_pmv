from __future__ import annotations

import asyncio
import unittest

from ..explore_resolution import DungeonExplorePlanApplication, base_member_plan


class _Encounter:
    def __init__(self, event=None, boss=None):
        self.event = event or {"type": "nothing", "description": "静谧无事"}
        self.boss = boss or [{"name": "守关者", "monster_type": "boss"}]

    def trigger_event(self, user_level, user_exp, *, random_source):
        random_source.random()
        return dict(self.event)

    def get_boss_data(self, user_level, user_exp, *, random_source):
        random_source.random()
        return list(self.boss)


def _intent(*, event=None, layer=0):
    member = base_member_plan(
        {
            "user_id": "leader",
            "user_name": "道友",
            "level": "筑基境初期",
            "hp": 100,
            "mp": 50,
            "stone": 900,
            "exp": 1000,
        },
        0,
    )
    return {
        "seed_version": "dungeon-explore-rng-v1",
        "seeds": {name: f"fixed:{name}" for name in ("encounter", "battle", "reward")},
        "expected_status": {
            "dungeon_id": "d1",
            "dungeon_status": "not_started",
            "current_layer": layer,
            "total_layers": 2,
        },
        "team": None,
        "dungeon": {"dungeon_id": "d1", "events": ["frozen"]},
        "battle_inputs": {"players": {"leader": {"attributes": {"属性": {"attack": 20}}}}},
        "level_ratios": {},
        "attack_buffs": {},
        "mentor_buff_message": "",
        "inventory": {"leader": {"7": {"goods_num": 3, "bind_num": 2}}},
        "members": [member],
    }


class DungeonExplorePlanApplicationTests(unittest.TestCase):
    def planner(self, encounter, *, winner=0, calls=None):
        calls = calls if calls is not None else {}

        async def battle(user_ids, monsters, **kwargs):
            calls["battle"] = (user_ids, monsters, kwargs)
            return ["frozen battle log"], winner, [{"leader": {"hp": 75, "mp": 30}}]

        def final_statuses(statuses, ratios):
            calls["final_statuses"] = (statuses, ratios)
            return {"leader": {"hp": 75, "mp": 30}}

        def rewards(user, members, monsters, statuses, operation_id, **kwargs):
            calls["reward"] = (user, members, monsters, kwargs)
            return "\n副本奖励：灵石100", [
                {"user_id": "leader", "stone": 100, "exp": 10, "items": []}
            ]

        return DungeonExplorePlanApplication(
            encounter_factory=lambda snapshot: self._record_encounter(calls, snapshot, encounter),
            battle=battle,
            final_statuses=final_statuses,
            reward_builder=rewards,
            item_lookup=lambda item_id: {"name": "灵草", "type": "药材"} if item_id == 7 else None,
            format_number=str,
        )

    @staticmethod
    def _record_encounter(calls, snapshot, encounter):
        calls["dungeon"] = dict(snapshot)
        return encounter

    def test_non_battle_events_use_frozen_inventory_and_keep_existing_mappings(self):
        events = (
            ({"type": "trap", "description": "落石", "damage": 0.1}, 0, 0, 50, "气血减少：50"),
            ({"type": "treasure", "description": "宝箱", "drop_items": 7}, 0, 1, 100, "灵草"),
            ({"type": "spirit_stone", "description": "灵石堆", "stones": 120}, 120, 0, 100, "获得120灵石"),
            ({"type": "nothing", "description": "风平浪静"}, 0, 0, 100, "风平浪静"),
        )
        for event, stone_delta, item_count, final_hp, message in events:
            with self.subTest(event=event["type"]):
                encounter = _Encounter(event=event)
                planner = self.planner(encounter)
                plan = asyncio.run(planner.build("op", "leader", _intent()))
                member = plan["members"][0]
                self.assertEqual(member["stone_delta"], stone_delta)
                self.assertEqual(member["final_hp"], final_hp)
                self.assertEqual(len(member["items"]), item_count)
                self.assertIn(message, plan["response"]["message"])
                self.assertEqual(plan["resolved"]["kind"], event["type"])
                if item_count:
                    self.assertEqual(member["items"][0]["expected_num"], 3)
                    self.assertEqual(member["items"][0]["expected_bind_num"], 2)

    def test_boss_win_and_loss_are_mapped_without_replaying_the_battle(self):
        for winner, complete, expected_reward in ((0, True, True), (1, False, False)):
            with self.subTest(winner=winner):
                calls = {}
                planner = self.planner(_Encounter(), winner=winner, calls=calls)
                intent = _intent(layer=1)
                plan = asyncio.run(planner.build("op", "leader", intent))
                self.assertEqual(plan["complete"], complete)
                self.assertEqual(plan["resolved"]["winner"], winner)
                self.assertEqual("reward" in calls, expected_reward)
                self.assertIn("frozen battle log", plan["response"]["battle_messages"])
                self.assertEqual(calls["battle"][0], ["leader"])
                self.assertEqual(
                    calls["battle"][2]["player_data_by_id"],
                    {"leader": {"属性": {"attack": 20}}},
                )
                if winner == 0:
                    self.assertEqual(plan["members"][0]["stone_delta"], 100)
                else:
                    self.assertFalse(plan["advance"])
                    self.assertEqual(plan["members"][0]["stone_delta"], 0)

    def test_seeded_resolution_is_repeatable_and_uses_snapshot_template(self):
        calls = {}
        planner = self.planner(_Encounter(event={"type": "spirit_stone", "stones": 15}), calls=calls)
        intent = _intent()
        first = asyncio.run(planner.build("op", "leader", intent))
        second = asyncio.run(planner.build("op", "leader", intent))
        self.assertEqual(first, second)
        self.assertEqual(calls["dungeon"], intent["dungeon"])


if __name__ == "__main__":
    unittest.main()
