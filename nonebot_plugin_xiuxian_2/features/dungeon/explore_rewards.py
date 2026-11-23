from __future__ import annotations

import random
from fractions import Fraction
from typing import Any, Callable, Mapping


def calc_team_distribution(
    team_member_ids: list[str], leader_id: str, damage_by_user: Mapping[str, int]
) -> dict[str, Fraction]:
    member_ids = [str(user_id) for user_id in team_member_ids]
    count = len(member_ids)
    if count <= 0:
        return {}
    if count == 1:
        return {member_ids[0]: Fraction(1, 2)}

    fixed: dict[str, Fraction] = {}
    fixed_pool = Fraction(0)
    for user_id in member_ids:
        fixed[user_id] = Fraction(1, 5) if user_id == str(leader_id) else Fraction(1, 10)
        fixed_pool += fixed[user_id]

    remaining = max(Fraction(0), Fraction(1) - fixed_pool)
    contributions = {
        user_id: max(0, int(damage_by_user.get(user_id, 0)))
        for user_id in member_ids
    }
    total_contribution = sum(contributions.values())
    distribution = dict(fixed)
    if remaining > 0:
        if total_contribution <= 0:
            share = remaining / count
            for user_id in member_ids:
                distribution[user_id] += share
        else:
            for user_id in member_ids:
                distribution[user_id] += remaining * Fraction(
                    contributions[user_id], total_contribution
                )

    cap = Fraction(1, 2)
    for user_id in member_ids:
        distribution[user_id] = min(distribution[user_id], cap)
    return distribution


def level_initial_exp(user_level: str, levels: Mapping[str, Any]) -> int:
    if user_level in {"江湖好手", "至高"}:
        return int(levels.get(user_level, {}).get("power", 100))
    if user_level.endswith(("初期", "中期", "圆满")):
        main = user_level[:-2]
    else:
        main = user_level
    return int(levels.get(f"{main}初期", {}).get("power", 100))


def reward_caps(user_level: str, is_boss: bool, levels: Mapping[str, Any]) -> tuple[int, int]:
    exp_cap = int(level_initial_exp(user_level, levels) * (0.03 if is_boss else 0.01))
    return (10_000_000 if is_boss else 5_000_000), exp_cap


def build_battle_rewards(
    user_info: Mapping[str, Any],
    members_info: list[Mapping[str, Any]],
    monsters: list[Mapping[str, Any]],
    battle_status: list[Mapping[str, Any]],
    operation_id: str,
    *,
    inventory_snapshot: Mapping[str, Any],
    random_source: Any,
    levels: Mapping[str, Any],
    item_lookup: Callable[[int], Mapping[str, Any] | None],
    format_number: Callable[[int], str],
) -> tuple[str, list[dict[str, Any]]]:
    is_boss = any(monster.get("monster_type") == "boss" for monster in monsters)
    stone_pool = sum(int(monster.get("stone", 0)) for monster in monsters)
    exp_pool = sum(int(monster.get("experience", 0)) for monster in monsters)

    damage_by_user: dict[str, int] = {}
    for group in battle_status:
        for stats in group.values():
            if stats.get("team_id") == 0 and str(stats.get("user_id", 0)) != 0:
                user_id = str(stats["user_id"])
                damage_by_user[user_id] = max(0, int(stats.get("total_dmg", 0)))

    member_by_id = {
        str(member["user_id"]): member
        for member in members_info
        if str(member["user_id"]) in damage_by_user
    }
    recipients = list(member_by_id)
    if not recipients:
        return "\n副本奖励：无，本次战斗无人获得奖励。", []

    leader_id = str(user_info["user_id"])
    distribution = calc_team_distribution(recipients, leader_id, damage_by_user)
    item_ids = [int(monster.get("item_id", 0) or 0) for monster in monsters]
    item_ids = [item_id for item_id in item_ids if item_id]
    result_lines = ["\n副本奖励："]
    rewards: list[dict[str, Any]] = []

    for user_id in recipients:
        member = member_by_id[user_id]
        ratio = distribution.get(user_id, Fraction(0))
        stone_cap, exp_cap = reward_caps(str(member["level"]), is_boss, levels)
        stone = min(int(stone_pool * ratio), stone_cap)
        exp = min(int(exp_pool * ratio), exp_cap)
        reward_text: list[str] = []
        if stone > 0:
            reward_text.append(f"灵石{format_number(stone)}")
        if exp > 0:
            reward_text.append(f"修为{format_number(exp)}")

        reward_items: list[dict[str, Any]] = []
        if user_id == leader_id and item_ids:
            item_id = random_source.choice(item_ids)
            item = item_lookup(item_id)
            if item:
                reward_item = {
                    "id": item_id,
                    "name": str(item["name"]),
                    "type": str(item["type"]),
                    "amount": 1,
                }
                frozen_user = inventory_snapshot.get(user_id)
                if not isinstance(frozen_user, Mapping):
                    raise ValueError("frozen inventory snapshot is missing")
                inventory = frozen_user.get(str(item_id), {"goods_num": 0, "bind_num": 0})
                if not isinstance(inventory, Mapping):
                    raise ValueError("frozen inventory entry is invalid")
                reward_item["expected_num"] = int(inventory.get("goods_num", 0) or 0)
                reward_item["expected_bind_num"] = int(inventory.get("bind_num", 0) or 0)
                reward_items.append(reward_item)
                reward_text.append(str(item["name"]))

        rewards.append({
            "user_id": user_id,
            "expected_stone": int(member["stone"]),
            "expected_exp": int(member["exp"]),
            "stone": stone,
            "exp": exp,
            "items": reward_items,
        })
        total_damage = sum(damage_by_user.values()) or 1
        contribution = damage_by_user.get(user_id, 0) / total_damage * 100
        allocation = float(ratio * 100)
        received = "无" if not reward_text else "、".join(reward_text)
        result_lines.append(
            f"\n{member['user_name']}（贡献{contribution:.2f}% / 分配{allocation:.2f}%）获得：{received}"
        )

    return "".join(result_lines), rewards


__all__ = ["build_battle_rewards", "calc_team_distribution", "level_initial_exp", "reward_caps"]
