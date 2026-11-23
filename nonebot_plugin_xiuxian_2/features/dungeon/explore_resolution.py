from __future__ import annotations

import copy
import random
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Protocol


DUNGEON_EXPLORE_RNG_VERSION = "dungeon-explore-rng-v1"


class DungeonExploreEncounterPort(Protocol):
    def get_boss_data(self, user_level: str, user_exp: int, *, random_source: Any) -> list[dict[str, Any]]: ...
    def trigger_event(self, user_level: str, user_exp: int, *, random_source: Any) -> dict[str, Any]: ...


class InvalidDungeonExploreIntent(ValueError):
    """A persisted exploration intent is incomplete or not supported."""


def dungeon_explore_rng(intent: Mapping[str, Any], purpose: str) -> random.Random:
    if intent.get("seed_version") != DUNGEON_EXPLORE_RNG_VERSION:
        raise InvalidDungeonExploreIntent("unsupported dungeon explore seed version")
    seeds = intent.get("seeds")
    seed = seeds.get(purpose) if isinstance(seeds, Mapping) else None
    if not isinstance(seed, str) or not seed:
        raise InvalidDungeonExploreIntent(f"missing dungeon explore {purpose} seed")
    return random.Random(seed)


def dungeon_explore_random_source(
    operation_id: str, purpose: str, *, seed: str | None = None
) -> random.Random:
    return random.Random(
        seed if seed is not None else f"{DUNGEON_EXPLORE_RNG_VERSION}:{purpose}:{operation_id}"
    )


def explore_response(message: str, battle_messages: Sequence[Any] | None = None) -> dict[str, Any]:
    return {"battle_messages": list(battle_messages or []), "message": str(message)}


def progress_message(current_layer: int, total_layers: int, *, advance: bool, complete: bool) -> str:
    if complete:
        final_layer = int(total_layers)
    elif advance:
        final_layer = min(int(current_layer) + 1, int(total_layers))
    else:
        final_layer = int(current_layer)
    if final_layer >= int(total_layers):
        return "恭喜你已完成今日副本！"
    return f"当前：第{final_layer + 1}层\n使用「探索副本」进入下一层！"


def base_member_plan(member: Mapping[str, Any], cd_type: int) -> dict[str, Any]:
    user_id = str(member["user_id"])
    hp, mp = int(member["hp"]), int(member["mp"])
    return {
        "user_id": user_id,
        "user_name": str(member.get("user_name", user_id)),
        "level": str(member.get("level", "")),
        "expected": {
            "hp": hp,
            "mp": mp,
            "stone": int(member["stone"]),
            "exp": int(member["exp"]),
            "cd_type": int(cd_type),
        },
        "final_hp": hp,
        "final_mp": mp,
        "stone_delta": 0,
        "exp_delta": 0,
        "items": [],
    }


def _frozen_member_info(member_plan: Mapping[str, Any]) -> dict[str, Any]:
    expected = member_plan.get("expected")
    user_id = str(member_plan.get("user_id", ""))
    level = str(member_plan.get("level", ""))
    if not user_id or not level or not isinstance(expected, Mapping):
        raise InvalidDungeonExploreIntent("frozen member profile is missing")
    try:
        return {
            "user_id": user_id,
            "user_name": str(member_plan.get("user_name", user_id)),
            "level": level,
            "hp": int(expected["hp"]),
            "mp": int(expected["mp"]),
            "stone": int(expected["stone"]),
            "exp": int(expected["exp"]),
        }
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise InvalidDungeonExploreIntent("frozen member resources are invalid") from exc


class DungeonExplorePlanApplication:
    """Turn a durable intent into a deterministic settlement plan via explicit ports."""

    def __init__(
        self,
        encounter_factory: Callable[[Mapping[str, Any]], DungeonExploreEncounterPort],
        battle: Callable[..., Awaitable[tuple[list[Any], int, list[dict[str, Any]]]]],
        final_statuses: Callable[[list[dict[str, Any]], Mapping[str, float]], Mapping[str, Mapping[str, Any]]],
        reward_builder: Callable[..., tuple[str, list[dict[str, Any]]]],
        item_lookup: Callable[[int], Mapping[str, Any] | None],
        format_number: Callable[[int], str],
    ) -> None:
        self.encounter_factory = encounter_factory
        self.battle = battle
        self.final_statuses = final_statuses
        self.reward_builder = reward_builder
        self.item_lookup = item_lookup
        self.format_number = format_number

    async def build(self, operation_id: str, user_id: str, intent: Mapping[str, Any]) -> dict[str, Any]:
        try:
            member_plans = copy.deepcopy(intent.get("members"))
            expected_status = copy.deepcopy(intent.get("expected_status"))
            team = copy.deepcopy(intent.get("team"))
            dungeon = copy.deepcopy(intent.get("dungeon"))
            battle_inputs = intent.get("battle_inputs")
            raw_ratios = intent.get("level_ratios")
            raw_attack_buffs = intent.get("attack_buffs")
            frozen_inventory = copy.deepcopy(intent.get("inventory"))

            if not isinstance(member_plans, list) or not member_plans:
                raise InvalidDungeonExploreIntent("frozen members are missing")
            if any(not isinstance(member, Mapping) for member in member_plans):
                raise InvalidDungeonExploreIntent("frozen member is invalid")
            members = [_frozen_member_info(member) for member in member_plans]
            member_ids = [member["user_id"] for member in members]
            if member_ids[0] != str(user_id) or len(member_ids) != len(set(member_ids)):
                raise InvalidDungeonExploreIntent("frozen member ids are invalid")
            if not isinstance(expected_status, dict) or not isinstance(dungeon, dict):
                raise InvalidDungeonExploreIntent("frozen dungeon input is missing")
            if not isinstance(battle_inputs, Mapping) or not isinstance(battle_inputs.get("players"), Mapping):
                raise InvalidDungeonExploreIntent("frozen battle inputs are missing")
            if not isinstance(raw_ratios, Mapping) or not isinstance(raw_attack_buffs, Mapping):
                raise InvalidDungeonExploreIntent("frozen battle modifiers are missing")
            if not isinstance(frozen_inventory, Mapping):
                raise InvalidDungeonExploreIntent("frozen inventory is missing")

            current_layer = int(expected_status["current_layer"])
            total_layers = int(expected_status["total_layers"])
            if total_layers <= 0 or current_layer < 0 or current_layer >= total_layers:
                raise InvalidDungeonExploreIntent("frozen dungeon progress is invalid")
            if not isinstance(team, (dict, type(None))):
                raise InvalidDungeonExploreIntent("frozen team is invalid")

            ratios = {str(key): float(value) for key, value in raw_ratios.items()}
            level_ratios = ratios or None
            attack_buffs = {str(key): float(value) for key, value in raw_attack_buffs.items()}
            mentor_message = str(intent.get("mentor_buff_message", ""))
            battle_data: dict[str, Mapping[str, Any]] = {}
            for member_id in member_ids:
                entry = battle_inputs["players"].get(member_id)
                attributes = entry.get("attributes") if isinstance(entry, Mapping) else None
                if not isinstance(attributes, Mapping) or not isinstance(attributes.get("属性"), Mapping) or not attributes["属性"]:
                    raise InvalidDungeonExploreIntent("frozen player battle input is invalid")
                battle_data[member_id] = attributes
                inventory = frozen_inventory.get(member_id)
                if not isinstance(inventory, Mapping):
                    raise InvalidDungeonExploreIntent("frozen inventory member is missing")
                for counts in inventory.values():
                    if not isinstance(counts, Mapping):
                        raise InvalidDungeonExploreIntent("frozen inventory entry is invalid")
                    goods_num = int(counts.get("goods_num", 0) or 0)
                    bind_num = int(counts.get("bind_num", 0) or 0)
                    if goods_num < 0 or bind_num < 0 or bind_num > goods_num:
                        raise InvalidDungeonExploreIntent("frozen inventory counts are invalid")
        except InvalidDungeonExploreIntent:
            raise
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise InvalidDungeonExploreIntent("frozen exploration input is invalid") from exc

        leader = members[0]
        leader_level, leader_exp = str(leader["level"]), int(leader["exp"])
        encounter_rng = dungeon_explore_rng(intent, "encounter")
        battle_rng = dungeon_explore_rng(intent, "battle")
        reward_rng = dungeon_explore_rng(intent, "reward")
        engine = self.encounter_factory(dungeon)
        member_plan_by_id = {member["user_id"]: member for member in member_plans}
        advance, complete = True, False
        resolved: dict[str, Any] = {}

        if current_layer == total_layers - 1:
            monsters = engine.get_boss_data(leader_level, leader_exp, random_source=encounter_rng)
            battle_messages, winner, statuses = await self.battle(
                member_ids,
                monsters,
                level_ratios=level_ratios,
                attack_buffs=attack_buffs,
                random_source=battle_rng,
                player_data_by_id=battle_data,
            )
            for member_id, final in self.final_statuses(statuses, ratios).items():
                if member_id in member_plan_by_id:
                    member_plan_by_id[member_id]["final_hp"] = int(final["hp"])
                    member_plan_by_id[member_id]["final_mp"] = int(final["mp"])
            if winner == 0:
                reward_message, rewards = self.reward_builder(
                    leader,
                    members,
                    monsters,
                    statuses,
                    operation_id,
                    inventory_snapshot=frozen_inventory,
                    random_source=reward_rng,
                )
                _apply_rewards(member_plan_by_id, rewards)
                complete = True
                summary = (
                    f"恭喜道友击败【{monsters[0]['name']}】！{mentor_message}{reward_message}\n"
                    f"{progress_message(current_layer, total_layers, advance=True, complete=True)}"
                )
            else:
                advance = False
                summary = (
                    f"道友不敌【{monsters[0]['name']}】，重伤逃遁。\n"
                    f"{progress_message(current_layer, total_layers, advance=False, complete=False)}"
                )
            response = explore_response(summary, battle_messages)
            resolved = {"kind": "boss", "winner": int(winner), "monsters": monsters}
        else:
            event = engine.trigger_event(leader_level, leader_exp, random_source=encounter_rng)
            event_type = str(event.get("type", "nothing"))
            battle_messages: list[Any] = []
            if event_type == "trap":
                parts = [str(event.get("description", ""))]
                damage = float(event.get("damage", 0.1))
                for member in members:
                    member_id = member["user_id"]
                    cost_hp = int((int(member["exp"]) / 2) * damage)
                    member_plan_by_id[member_id]["final_hp"] = max(1, int(member["hp"]) - cost_hp)
                    parts.append(f"{member['user_name']}气血减少：{self.format_number(cost_hp)}")
                summary = "，".join(parts)
            elif event_type == "monster":
                monsters = event["monster_data"]
                battle_messages, winner, statuses = await self.battle(
                    member_ids,
                    monsters,
                    level_ratios=level_ratios,
                    attack_buffs=attack_buffs,
                    random_source=battle_rng,
                    player_data_by_id=battle_data,
                )
                for member_id, final in self.final_statuses(statuses, ratios).items():
                    if member_id in member_plan_by_id:
                        member_plan_by_id[member_id]["final_hp"] = int(final["hp"])
                        member_plan_by_id[member_id]["final_mp"] = int(final["mp"])
                summary = f"{event.get('description', '')}！"
                if winner == 0:
                    reward_message, rewards = self.reward_builder(
                        leader,
                        members,
                        monsters,
                        statuses,
                        operation_id,
                        inventory_snapshot=frozen_inventory,
                        random_source=reward_rng,
                    )
                    _apply_rewards(member_plan_by_id, rewards)
                    summary += f"\n恭喜道友击败敌人。{mentor_message}{reward_message}"
                else:
                    summary += "\n道友不敌，重伤逃遁。"
                resolved = {"kind": "monster", "winner": int(winner), "monsters": monsters}
            elif event_type == "treasure":
                item_id = int(event.get("drop_items", 0) or 0)
                item = self.item_lookup(item_id) if item_id else None
                if item:
                    leader_inventory = frozen_inventory.get(str(user_id))
                    if not isinstance(leader_inventory, Mapping):
                        raise InvalidDungeonExploreIntent("frozen treasure inventory is invalid")
                    counts = leader_inventory.get(str(item_id), {"goods_num": 0, "bind_num": 0})
                    if not isinstance(counts, Mapping):
                        raise InvalidDungeonExploreIntent("frozen treasure inventory is invalid")
                    member_plan_by_id[str(user_id)]["items"] = [{
                        "id": item_id,
                        "name": str(item["name"]),
                        "type": str(item["type"]),
                        "amount": 1,
                        "expected_num": int(counts.get("goods_num", 0) or 0),
                        "expected_bind_num": int(counts.get("bind_num", 0) or 0),
                    }]
                    summary = f"{event.get('description', '')}，凑近一看居然是{item['name']}"
                else:
                    summary = f"{event.get('description', '')}，宝箱中空空如也"
            elif event_type == "spirit_stone":
                stones = int(event.get("stones", 0))
                member_plan_by_id[str(user_id)]["stone_delta"] = stones
                summary = f"{event.get('description', '')}，获得{self.format_number(stones)}灵石"
            else:
                event_type = "nothing"
                summary = str(event.get("description", "无事发生"))

            summary += "！\n" + progress_message(
                current_layer, total_layers, advance=advance, complete=complete
            )
            response = explore_response(summary, battle_messages)
            if not resolved:
                resolved = {"kind": event_type, "event": event}

        return {
            "expected_status": expected_status,
            "team": team,
            "members": member_plans,
            "advance": bool(advance),
            "complete": bool(complete),
            "resolved": resolved,
            "response": response,
        }


def _apply_rewards(member_plans: Mapping[str, dict[str, Any]], rewards: Sequence[Mapping[str, Any]]) -> None:
    for reward in rewards:
        member = member_plans.get(str(reward.get("user_id", "")))
        if member is None:
            continue
        member["stone_delta"] = int(reward.get("stone", 0) or 0)
        member["exp_delta"] = int(reward.get("exp", 0) or 0)
        member["items"] = list(reward.get("items", []))


__all__ = [
    "DUNGEON_EXPLORE_RNG_VERSION",
    "DungeonExplorePlanApplication",
    "InvalidDungeonExploreIntent",
    "base_member_plan",
    "dungeon_explore_rng",
    "dungeon_explore_random_source",
    "explore_response",
    "progress_message",
]
