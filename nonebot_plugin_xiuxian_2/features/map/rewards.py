from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Callable, Protocol


class MapRewardRandomSource(Protocol):
    def random(self) -> float: ...

    def randint(self, start: int, end: int) -> int: ...

    def choice(self, values: Sequence[Any]) -> Any: ...


class MapItemCatalog(Protocol):
    def get_data_by_item_id(self, item_id: Any) -> dict[str, Any] | None: ...

    def get_random_id_list_by_rank_and_item_type(
        self, rank: int, item_type: str
    ) -> Sequence[Any]: ...


class MapRewardResolver:
    """Resolve map reward decisions before any asset settlement transaction."""

    def __init__(
        self,
        *,
        reward_pools: Mapping[str, Sequence[Any]],
        item_catalog: MapItemCatalog,
        rank_for_level: Callable[[str, int], int],
        format_number: Callable[[Any], str],
        skill_equip_types: Sequence[str],
        arena_ticket_drop_ratio: float,
    ) -> None:
        self.reward_pools = reward_pools
        self.item_catalog = item_catalog
        self.rank_for_level = rank_for_level
        self.format_number = format_number
        self.skill_equip_types = skill_equip_types
        self.arena_ticket_drop_ratio = float(arena_ticket_drop_ratio)

    def _expand_plan(
        self,
        reward_plan: Sequence[tuple[str, int, int, float]],
    ) -> list[tuple[str, int, int, float]]:
        expanded = []
        for pool_key, cmin, cmax, chance in reward_plan:
            if pool_key == "arena_ticket_low":
                continue
            expanded.append((pool_key, cmin, cmax, chance))
            if pool_key == "wash_stone_low":
                expanded.append((
                    "arena_ticket_low",
                    1,
                    1,
                    round(float(chance) * self.arena_ticket_drop_ratio, 4),
                ))
            if pool_key == "acc_pack_low":
                expanded.append(("pet_resource_low", cmin, cmax, chance))
        return expanded

    def roll_rewards(
        self,
        reward_plan: Sequence[tuple[str, int, int, float]],
        decay_ratio: float = 1.0,
        *,
        random_source: MapRewardRandomSource,
    ) -> tuple[list[str], int, list[dict[str, Any]]]:
        rewards = []
        stone = 0
        items_to_add = []
        for pool_key, cmin, cmax, chance in self._expand_plan(reward_plan):
            if random_source.random() > chance:
                continue
            pool_ids = self.reward_pools.get(pool_key, ())
            if not pool_ids:
                continue
            count = max(1, int(round(random_source.randint(cmin, cmax) * decay_ratio)))
            for _ in range(count):
                reward_id = random_source.choice(pool_ids)
                if isinstance(reward_id, str) and reward_id.startswith("LS_"):
                    amount = max(1, int(round(int(reward_id.split("_")[1]) * decay_ratio)))
                    stone += amount
                    rewards.append(f"灵石x{self.format_number(amount)}")
                    continue
                info = self.item_catalog.get_data_by_item_id(str(reward_id))
                if not info:
                    continue
                items_to_add.append({
                    "id": int(reward_id),
                    "name": info["name"],
                    "type": info.get("type", "材料"),
                    "amount": 1,
                })
                rewards.append(f"{info['name']}x1")
        return rewards, stone, items_to_add

    def roll_dongfu_material(
        self,
        node_type: str,
        chance_multiplier: float = 1.0,
        *,
        random_source: MapRewardRandomSource,
    ) -> tuple[list[str], int, list[dict[str, Any]]]:
        plan = {
            "水域": ("dongfu_water", 0.16),
            "灵林": ("dongfu_soil", 0.16),
            "仙山": ("dongfu_soil", 0.22),
            "矿脉": ("dongfu_array", 0.16),
        }
        if node_type not in plan:
            return [], 0, []
        pool_key, chance = plan[node_type]
        if random_source.random() > min(0.80, chance * chance_multiplier):
            return [], 0, []
        return self.roll_rewards(
            [(pool_key, 1, 1, 1.0)],
            random_source=random_source,
        )

    def roll_skill_equip_drop(
        self,
        user_info: Mapping[str, Any],
        drop_rate: float = 0.1,
        *,
        random_source: MapRewardRandomSource,
    ) -> tuple[str | None, dict[str, Any] | None]:
        if random_source.random() > drop_rate:
            return None, None
        item_type = random_source.choice(self.skill_equip_types)
        user_level = user_info.get("level", "江湖好手")
        max_rank = 16 if item_type in ["法器", "防具", "辅修功法", "身法", "瞳术"] else 5
        rank = self.rank_for_level(user_level, max_rank)
        item_ids = self.item_catalog.get_random_id_list_by_rank_and_item_type(
            rank,
            item_type,
        )
        if not item_ids:
            return None, None
        item_id = random_source.choice(item_ids)
        info = self.item_catalog.get_data_by_item_id(item_id)
        if not info:
            return None, None
        return f"{info.get('level', '未知品级')}:{info['name']}x1", {
            "id": int(item_id),
            "name": info["name"],
            "type": info.get("type", item_type),
            "amount": 1,
        }

    def roll_mission_reward(
        self,
        *,
        random_source: MapRewardRandomSource,
    ) -> tuple[list[str], dict[str, Any]]:
        rewards = []
        reward_meta = {"stone_delta": 0, "item_delta": []}
        stone_pool = self.reward_pools.get("stone_high", ())
        if stone_pool:
            stone_pick = random_source.choice(stone_pool)
            if isinstance(stone_pick, str) and stone_pick.startswith("LS_"):
                stone_num = int(stone_pick.split("_")[1])
                rewards.append(f"灵石x{self.format_number(stone_num)}")
                reward_meta["stone_delta"] += stone_num
        extra_pool_key = random_source.choice(["acc_pack_low", "god_frag", "token_rare"])
        extra_pool = self.reward_pools.get(extra_pool_key, ())
        if extra_pool:
            item_id = random_source.choice(extra_pool)
            info = self.item_catalog.get_data_by_item_id(str(item_id))
            if info:
                rewards.append(f"{info['name']}x1")
                reward_meta["item_delta"].append({
                    "id": int(item_id),
                    "name": info["name"],
                    "type": info.get("type", "材料"),
                    "amount": 1,
                })
        return rewards, reward_meta


__all__ = ["MapItemCatalog", "MapRewardRandomSource", "MapRewardResolver"]
