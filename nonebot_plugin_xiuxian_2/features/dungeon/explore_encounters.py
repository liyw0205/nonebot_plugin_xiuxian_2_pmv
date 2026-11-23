from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any


DEFAULT_BOSS_SKILLS = [14501, 14502, 14503, 14504]
DEFAULT_MINION_SKILLS = [14503, 14504]

TYPE_EVENT_WEIGHTS = {
    "explore": {"trap": 10, "monster": 40, "treasure": 20, "spirit_stone": 20, "nothing": 10},
    "challenge": {"trap": 10, "monster": 65, "treasure": 10, "spirit_stone": 10, "nothing": 5},
    "puzzle": {"trap": 30, "monster": 25, "treasure": 20, "spirit_stone": 15, "nothing": 10},
}

TYPE_DIFFICULTY_MULT = {
    "explore": (0.90, 1.10),
    "challenge": (1.20, 1.60),
    "puzzle": (1.00, 1.25),
}
TYPE_REWARD_MULT = {"explore": 1.00, "challenge": 1.60, "puzzle": 1.25}
TYPE_NON_BATTLE_REWARD_MULT = {"explore": 1.00, "challenge": 1.50, "puzzle": 1.20}
JINJIE_LIST = [
    "江湖好手", "感气境", "练气境", "筑基境", "结丹境", "金丹境", "元神境", "化神境",
    "炼神境", "返虚境", "大乘境", "虚道境", "斩我境", "遁一境", "至尊境", "微光境",
    "星芒境", "月华境", "耀日境", "祭道境", "自在境", "破虚境", "无界境", "混元境",
    "造化境", "永恒境", "至高",
]


def dungeon_type(template: Mapping[str, Any]) -> str:
    value = str(template.get("dungeon_type", template.get("type", "explore")) or "explore").lower()
    return value if value in TYPE_EVENT_WEIGHTS else "explore"


def type_multipliers(template: Mapping[str, Any]) -> tuple[float, float, float, float]:
    kind = dungeon_type(template)
    low, high = TYPE_DIFFICULTY_MULT.get(kind, (1.0, 1.0))
    return low, high, TYPE_REWARD_MULT.get(kind, 1.0), TYPE_NON_BATTLE_REWARD_MULT.get(kind, 1.0)


def choose_main_realm(
    user_level: str,
    *,
    prefer_player_level_for_boss: bool,
    random_source: Any,
    convert_rank: Callable[[str], tuple[Any, Any]],
) -> str:
    player_rank, _ = convert_rank(user_level)
    if player_rank is None:
        player_rank = convert_rank("江湖好手")[0] or 0
    player_main = next((realm for realm in JINJIE_LIST if user_level.startswith(realm)), "感气境")
    if prefer_player_level_for_boss:
        return player_main
    available = []
    for realm in JINJIE_LIST:
        if realm in {"江湖好手", "至高"}:
            continue
        rank, _ = convert_rank(f"{realm}初期")
        if rank is not None and rank <= player_rank + 5:
            available.append((realm, rank))
    if not available:
        return "感气境"
    weighted = []
    for realm, rank in available:
        difference = abs(player_rank - rank)
        weight = 6 if difference <= 2 else 3 if difference <= 5 else 1
        weighted.extend([realm] * weight)
    return random_source.choice(weighted) if weighted else random_source.choice([realm for realm, _ in available])


def generate_drop_item(
    user_level: str,
    drop_items: Mapping[str, Any],
    *,
    random_source: Any,
    convert_rank: Callable[[str], tuple[Any, Any]],
    item_ids_for_rank: Callable[[int, str], list[int]],
) -> int:
    if not drop_items:
        return 0
    item_type = random_source.choices(list(drop_items.keys()), weights=list(drop_items.values()), k=1)[0]
    player_rank, _ = convert_rank(user_level)
    if player_rank is None:
        player_rank = convert_rank("江湖好手")[0] or 0
    max_rank = convert_rank("江湖好手")[0] or 55
    base_rank = max(player_rank - random_source.randint(15, 20), 5)
    item_rank = random_source.randint(base_rank, min(base_rank + random_source.randint(5, 10), max_rank))
    if item_rank <= 10 and random_source.random() < 0.2:
        item_rank = random_source.randint(11, 20)
    item_ids = item_ids_for_rank(item_rank, item_type)
    return random_source.choice(item_ids) if item_ids else 0


def create_monster(
    template: Mapping[str, Any],
    user_level: str,
    user_exp: int,
    monster_info: Mapping[str, Any],
    monster_type: str,
    *,
    random_source: Any,
    safe_level_power: Callable[[str], int],
    convert_rank: Callable[[str], tuple[Any, Any]],
    generate_drop: Callable[..., int],
) -> dict[str, Any]:
    kind = dungeon_type(template)
    diff_low, diff_high, reward_mult, _ = type_multipliers(template)
    main_realm = choose_main_realm(
        user_level,
        prefer_player_level_for_boss=monster_type == "boss",
        random_source=random_source,
        convert_rank=convert_rank,
    )
    realm_key = main_realm if main_realm in {"江湖好手", "至高"} else f"{main_realm}初期"
    base_power = safe_level_power(realm_key)
    player_power = safe_level_power(user_level)
    exp_factor = max(0.8, min(1.3, (user_exp / max(1, player_power)) ** 0.2))
    difficulty = random_source.uniform(diff_low, diff_high)
    if monster_type == "boss":
        boss_bonus = 1.25 if kind == "challenge" else 1.10
        power_anchor = int(max(base_power, user_exp) * exp_factor * boss_bonus)
    else:
        power_anchor = int(base_power * exp_factor * difficulty)
    power_anchor = max(100, power_anchor)

    hp = max(100, int(power_anchor * monster_info.get("hp_base_multiplier", 60) * random_source.uniform(0.92, 1.08)))
    mp = max(10, int(power_anchor * monster_info.get("mp_base_multiplier", 1.5) * random_source.uniform(0.92, 1.08)))
    attack = max(20, int(power_anchor * monster_info.get("attack_base_multiplier", 0.15) * random_source.uniform(0.92, 1.08)))

    reward = monster_info.get("reward", {})
    stone_range = reward.get("spirit_stone", [0.2, 0.6])
    if not isinstance(stone_range, list) or len(stone_range) != 2:
        stone_range = [0.2, 0.6]
    stone_factor = random_source.uniform(stone_range[0], stone_range[1])
    stone_base = 12000 if monster_type == "boss" else 8000
    stone = int(stone_base * stone_factor * reward_mult * random_source.uniform(90, 150))
    exp_ratio = float(reward.get("experience", 0.002))
    experience = power_anchor * 180 * exp_ratio * reward_mult * random_source.uniform(1.0, 1.8)
    if monster_type == "boss":
        experience *= 1.6
    experience = int(experience)

    drop_chance = float(reward.get("drop_chance", 0.01))
    if kind == "challenge":
        drop_chance *= 1.20
    elif kind == "puzzle":
        drop_chance *= 1.10
    drop_chance = min(drop_chance, 0.95)
    item_id = 0
    if random_source.random() < drop_chance:
        drop_items = reward.get("drop_items", {})
        item_id = generate_drop(user_level, drop_items, random_source=random_source)

    skills = monster_info.get("skills", [])
    if not isinstance(skills, list):
        skills = []
    if not skills:
        skills = DEFAULT_BOSS_SKILLS[:2] if monster_type == "boss" else DEFAULT_MINION_SKILLS[:1]
    return {
        "name": monster_info.get("name", "怪物"),
        "jj": main_realm,
        "气血": hp,
        "总血量": hp,
        "真元": mp,
        "攻击": attack,
        "skills": skills,
        "experience": max(1, experience),
        "stone": max(1, stone),
        "item_id": item_id,
        "monster_type": monster_type,
    }


def select_event(template: Mapping[str, Any], random_source: Any) -> dict[str, Any]:
    events = template.get("events", [])
    if not isinstance(events, list):
        events = []
    event_map = {
        str(event.get("event_id")): event
        for event in events
        if isinstance(event, Mapping)
    }
    weights = TYPE_EVENT_WEIGHTS[dungeon_type(template)]
    candidates = [event_map[event_id] for event_id in weights if event_id in event_map]
    candidate_weights = [weights[event_id] for event_id in weights if event_id in event_map]
    if candidates:
        return dict(random_source.choices(candidates, weights=candidate_weights, k=1)[0])
    if not events:
        return {"event_id": "nothing", "description": "无事发生", "weight": 1}
    valid_events = [event for event in events if isinstance(event, Mapping)]
    if not valid_events:
        return {"event_id": "nothing", "description": "无事发生", "weight": 1}
    fallback_weights = [max(1, int(event.get("weight", 0) or 0)) for event in valid_events]
    return dict(random_source.choices(valid_events, weights=fallback_weights, k=1)[0])


def minion_info(template: Mapping[str, Any], template_type: str, random_source: Any) -> dict[str, Any]:
    templates = template.get("monster_templates", {})
    templates = templates if isinstance(templates, Mapping) else {}
    if template_type not in templates:
        template_type = "common"
    data = templates.get(template_type, {})
    data = data if isinstance(data, Mapping) else {}
    name_prefixes = data.get("name_prefix", [""])
    base_names = data.get("base_names", ["怪物"])
    prefix = random_source.choice(name_prefixes)
    base_name = random_source.choice(base_names)
    hp_range = data.get("hp_range", [50, 100])
    mp_range = data.get("mp_range", [1, 2])
    attack_range = data.get("attack_range", [0.1, 0.5])
    skill_pool = data.get("skill_pool", [])
    if not isinstance(skill_pool, list):
        skill_pool = []
    if skill_pool:
        pick_n = 1 if template_type == "common" else min(2, len(skill_pool))
        skills = random_source.sample(skill_pool, pick_n)
    else:
        skills = DEFAULT_MINION_SKILLS[:1]
    return {
        "name": f"{prefix}·{base_name}" if prefix else base_name,
        "hp_base_multiplier": random_source.uniform(hp_range[0], hp_range[1]),
        "mp_base_multiplier": random_source.uniform(mp_range[0], mp_range[1]),
        "attack_base_multiplier": random_source.uniform(attack_range[0], attack_range[1]),
        "skills": skills,
        "reward": data.get("reward", {}),
    }


def boss_info(template: Mapping[str, Any], random_source: Any) -> dict[str, Any]:
    config = template.get("boss", {})
    config = config if isinstance(config, Mapping) else {}
    hp_range = config.get("hp_range", [100, 200])
    mp_range = config.get("mp_range", [1, 2])
    attack_range = config.get("attack_range", [0.3, 0.5])
    skills = config.get("skills", [])
    if not isinstance(skills, list) or not skills:
        skills = DEFAULT_BOSS_SKILLS[:2]
    return {
        "name": config.get("name", "副本BOSS"),
        "hp_base_multiplier": random_source.uniform(hp_range[0], hp_range[1]),
        "mp_base_multiplier": random_source.uniform(mp_range[0], mp_range[1]),
        "attack_base_multiplier": random_source.uniform(attack_range[0], attack_range[1]),
        "skills": skills,
        "reward": config.get("reward", {}),
    }


def build_boss_encounter(
    template: Mapping[str, Any],
    user_level: str,
    user_exp: int,
    *,
    random_source: Any,
    create_monster: Callable[..., dict[str, Any]],
) -> list[dict[str, Any]]:
    kind = dungeon_type(template)
    monsters = [
        create_monster(
            user_level,
            user_exp,
            boss_info(template, random_source),
            monster_type="boss",
            random_source=random_source,
        )
    ]
    if kind == "challenge":
        minion_count = random_source.randint(1, 3)
        minion_type = "elite" if random_source.random() < 0.6 else "common"
    else:
        minion_count = 1
        minion_type = "common"
    for _ in range(minion_count):
        monsters.append(
            create_monster(
                user_level,
                user_exp,
                minion_info(template, minion_type, random_source),
                monster_type="minion",
                random_source=random_source,
            )
        )
    return monsters


def build_event_encounter(
    template: Mapping[str, Any],
    user_level: str,
    user_exp: int,
    *,
    random_source: Any,
    create_monster: Callable[..., dict[str, Any]],
    generate_drop_item: Callable[..., int],
    item_lookup: Callable[[int], Mapping[str, Any] | None],
    convert_rank: Callable[[str], tuple[Any, Any]],
) -> dict[str, Any]:
    event = select_event(template, random_source)
    event_id = str(event.get("event_id", "nothing"))
    kind = dungeon_type(template)
    _, _, _, non_battle_reward_mult = type_multipliers(template)
    result: dict[str, Any] = {"type": event_id, "description": event.get("description", "")}

    if event_id == "trap":
        low, high = event.get("damage", [0.1, 0.2])
        if kind == "puzzle":
            low *= 1.10
            high *= 1.20
        elif kind == "challenge":
            low *= 1.05
            high *= 1.10
        result["damage"] = random_source.uniform(low, high)
    elif event_id == "monster":
        battle = event.get("battle", {}) or {}
        template_types = battle.get("monster_templates", ["common"])
        elite_chance = float(battle.get("elite_chance", 0.2))
        if kind == "challenge":
            minion_count = random_source.randint(1, 3)
            elite_chance = min(0.5, elite_chance + 0.15)
        else:
            minion_count = 1
            if kind == "puzzle":
                elite_chance = min(0.35, elite_chance + 0.05)
        enemy_data = []
        for _ in range(minion_count):
            actual_type = "common"
            if random_source.random() < elite_chance and "elite" in template_types:
                actual_type = "elite"
            elif len(template_types) > 1:
                pool = [value for value in template_types if value != "elite"]
                actual_type = random_source.choice(pool) if pool else "common"
            enemy_data.append(
                create_monster(
                    user_level,
                    user_exp,
                    minion_info(template, actual_type, random_source),
                    monster_type="minion",
                    random_source=random_source,
                )
            )
        result["monster_data"] = enemy_data
    elif event_id == "treasure":
        reward = event.get("reward", {}) or {}
        drop_items = reward.get("drop_items", {}) or {}
        roll_times = 2 if kind == "puzzle" else 3 if kind == "challenge" else 1
        best_item, best_rank = 0, -1
        for _ in range(roll_times):
            item_id = generate_drop_item(user_level, drop_items, random_source=random_source)
            if item_id == 0:
                continue
            item = item_lookup(item_id)
            rank = int(item.get("rank", 0)) if item else 0
            if rank > best_rank:
                best_rank, best_item = rank, item_id
        result["drop_items"] = best_item
    elif event_id == "spirit_stone":
        stone_range = (event.get("reward", {}) or {}).get("spirit_stone", [1, 2])
        if not isinstance(stone_range, list) or len(stone_range) != 2:
            stone_range = [1, 2]
        rand_mul = random_source.uniform(stone_range[0], stone_range[1])
        rank, _ = convert_rank(user_level)
        if rank is None:
            rank = convert_rank("江湖好手")[0] or 0
        base_rank = convert_rank("江湖好手")[0] or 0
        rank_diff = max(0, rank - base_rank)
        amount = int(12000 * rand_mul * (1.18 ** rank_diff) * non_battle_reward_mult * random_source.uniform(90, 140))
        result["stones"] = max(1, amount)
    return result


__all__ = [
    "DEFAULT_BOSS_SKILLS",
    "DEFAULT_MINION_SKILLS",
    "JINJIE_LIST",
    "TYPE_DIFFICULTY_MULT",
    "TYPE_EVENT_WEIGHTS",
    "TYPE_NON_BATTLE_REWARD_MULT",
    "TYPE_REWARD_MULT",
    "boss_info",
    "build_boss_encounter",
    "build_event_encounter",
    "choose_main_realm",
    "create_monster",
    "dungeon_type",
    "generate_drop_item",
    "minion_info",
    "select_event",
    "type_multipliers",
]
