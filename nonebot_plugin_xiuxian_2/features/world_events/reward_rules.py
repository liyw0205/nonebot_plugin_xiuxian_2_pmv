"""魔修入侵领奖奖励规则（纯函数，无 I/O）。

这些常量与算法原先内联在兼容命令模块 `xiuxian/xiuxian_world_events/__init__.py` 里，
与领奖写入、消息文案混在同一文件，无法单独锁定回归。现在由 feature 域拥有：
贡献口径、灵石上限、灵签宝箓档位、随机奖励池筛选与「无上仅 1% 保留」都是纯函数，
命令适配器只负责取 `world_event_state` 快照、调用本模块并发送回复。

`world_event_state` 是历史 JSON 投影（嵌套 dict，字段可能缺失或为文本），因此所有
读取都按「缺/坏字段回退默认值」处理，不抛错、不改写输入。
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

# 与旧实现完全一致的可调数值，迁移不得顺手调整。
DEMON_STONE_REWARD_CAP = 50_000_000
DEMON_EXP_REWARD_CAP_RATE = 0.05
DEMON_MIN_REWARD_CONTRIBUTION = 0.01
DEMON_TALISMAN_ITEM_ID = 20010
DEMON_TALISMAN_CONTRIBUTION_STEP = 0.10
DEMON_TALISMAN_REWARD_CAP = 3
DEMON_FULL_RANDOM_POOL_CONTRIBUTION = 0.50

# 随机奖励只从这些物品类型里取（与旧实现一致）。
RANDOM_REWARD_SKILL_TYPES = ("功法", "辅修功法", "神通")
RANDOM_REWARD_GEAR_TYPES = ("法器", "防具")
RANDOM_REWARD_MOBILITY_TYPES = ("身法", "瞳术")
RANDOM_REWARD_ALWAYS_TYPES = ("特殊物品",)


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def clamp(value: Any, lower: float, upper: float, *, default: float = 0.0) -> float:
    return min(max(to_float(value, default), lower), upper)


def reward_contribution(contribution: Any) -> float:
    """把原始贡献压进 ``[DEMON_MIN_REWARD_CONTRIBUTION, 1.0]``。"""
    return clamp(contribution, DEMON_MIN_REWARD_CONTRIBUTION, 1.0)


def boss_reward_multiplier(boss_info: Mapping[str, Any] | None) -> float:
    return max(to_float((boss_info or {}).get("reward_multiplier"), 1.0), 1.0)


def record_reward_multiplier(record: Mapping[str, Any] | None) -> float:
    return max(to_float((record or {}).get("reward_multiplier"), 1.0), 1.0)


def format_contribution_percent(contribution: Any, reward_multiplier: float = 1.0) -> str:
    raw = clamp(contribution, 0.0, 1.0)
    settled = clamp(raw * max(to_float(reward_multiplier, 1.0), 1.0), 0.0, 1.0)
    if abs(settled - raw) > 1e-12:
        return f"{raw * 100:.2f}%，结算{settled * 100:.2f}%"
    return f"{raw * 100:.2f}%"


def stone_reward(contribution: Any) -> int:
    """灵石奖励：贡献 × 上限，且不超过上限。"""
    normalized = clamp(contribution, 0.0, 1.0)
    return min(int(DEMON_STONE_REWARD_CAP * normalized), DEMON_STONE_REWARD_CAP)


def exp_reward_rate(contribution: Any) -> float:
    """修为奖励比例（调用方再用 ``percent_exp_reward`` 做 gap 锚定与境界压制）。"""
    return DEMON_EXP_REWARD_CAP_RATE * clamp(contribution, 0.0, 1.0)


def talisman_reward_count(contribution: Any) -> int:
    """灵签宝箓：每 ``DEMON_TALISMAN_CONTRIBUTION_STEP`` 贡献 1 个，最多上限。"""
    normalized = to_float(contribution)
    count = int((normalized + 1e-12) / DEMON_TALISMAN_CONTRIBUTION_STEP)
    return min(max(count, 0), DEMON_TALISMAN_REWARD_CAP)


def is_wushang_reward_item(item: Mapping[str, Any] | None) -> bool:
    """无上品技能/装备（技能加载后 level 常为「无上」）。"""
    if not isinstance(item, Mapping):
        return False
    if str(item.get("level") or "") == "无上":
        return True
    if str(item.get("rank") or "") == "无上":
        return True
    if item.get("item_type") in RANDOM_REWARD_GEAR_TYPES and to_int(item.get("rank"), -1) == 5:
        return True
    return False


def is_random_reward_item(item: Mapping[str, Any] | None, contribution: Any) -> bool:
    """该物品是否可以进入魔修随机奖励池。"""
    if not isinstance(item, Mapping):
        return False
    contribution = clamp(contribution, 0.0, 1.0)
    item_type = item.get("item_type")
    if item_type in RANDOM_REWARD_ALWAYS_TYPES:
        return True
    if item_type in RANDOM_REWARD_SKILL_TYPES:
        return to_int(item.get("rank"), -1) > 0
    if item_type in RANDOM_REWARD_MOBILITY_TYPES:
        return contribution >= DEMON_FULL_RANDOM_POOL_CONTRIBUTION or to_int(item.get("rank"), -1) > 0
    if item_type in RANDOM_REWARD_GEAR_TYPES:
        if "fusion" in item:
            return False
        return contribution >= DEMON_FULL_RANDOM_POOL_CONTRIBUTION or to_int(item.get("rank"), -1) > 0
    return False


def random_reward_pool(
    items: Mapping[Any, Mapping[str, Any]] | Iterable[tuple[Any, Mapping[str, Any]]],
    contribution: Any,
) -> list[tuple[int, Mapping[str, Any]]]:
    """按贡献筛选可抽取的物品，返回 ``[(item_id, item)]``，ID 非数字则跳过。"""
    source: Iterable[tuple[Any, Mapping[str, Any]]]
    source = items.items() if isinstance(items, Mapping) else items
    pool: list[tuple[int, Mapping[str, Any]]] = []
    for entry in source:
        item_id, item = entry
        if not is_random_reward_item(item, contribution):
            continue
        try:
            pool.append((int(item_id), item))
        except (TypeError, ValueError):
            continue
    return pool


def pick_random_reward(
    pool: Sequence[tuple[int, Mapping[str, Any]]],
    rng: Any,
) -> tuple[int, Mapping[str, Any]] | None:
    """从池中抽一件；抽中无上技能/装备时仅 1% 保留，否则改抽非无上。"""
    if not pool:
        return None
    pick: tuple[int, Mapping[str, Any]] = rng.choice(list(pool))
    item = pick[1]
    if item is not None and is_wushang_reward_item(item):
        if rng.randint(1, 100) != 100:
            normal_pool = [row for row in pool if not is_wushang_reward_item(row[1])]
            if normal_pool:
                return rng.choice(normal_pool)
            return None
    return pick


def describe_random_reward(item_id: Any, item: Mapping[str, Any] | None) -> str | None:
    """随机奖励的展示文案；未获得返回 ``None``。"""
    if item is None:
        return None
    name = item.get("name", f"未知物品{item_id}")
    level = item.get("level")
    return f"{level}:{name}" if level else str(name)


def record_wave(record: Mapping[str, Any] | None) -> int:
    """记录所属波次（缺/坏字段按第 1 波）。"""
    return max(to_int((record or {}).get("wave"), 1), 1)


def record_reward_base_hp(
    state: Mapping[str, Any] | None,
    record: Mapping[str, Any] | None,
    realm: str,
    wave: int,
) -> int:
    """贡献分母：记录自身基准、当前 BOSS 满血与已造成伤害三者的较大值。"""
    if not record:
        return 1
    boss_info = ((state or {}).get("bosses") or {}).get(realm, {})
    current_boss_base = 0
    if boss_info and max(to_int(boss_info.get("wave"), 1), 1) == wave:
        current_boss_base = to_int(boss_info.get("boss_max_hp"), 0)
    return max(
        to_int(record.get("reward_base_hp"), 0),
        current_boss_base,
        to_int(record.get("damage"), 0),
        1,
    )


def record_reward_contribution(
    state: Mapping[str, Any] | None,
    record: Mapping[str, Any] | None,
    realm: str,
    wave: int,
) -> float:
    """单条记录的贡献：优先用已冻结的 ``reward_contribution``，否则按伤害/分母 × 倍率。"""
    if not record:
        return 0.0
    if "reward_contribution" in record:
        return clamp(record.get("reward_contribution"), 0.0, 1.0)
    reward_base_hp = record_reward_base_hp(state, record, realm, wave)
    base_contribution = clamp(to_int(record.get("damage"), 0) / reward_base_hp, 0.0, 1.0)
    return min(base_contribution * record_reward_multiplier(record), 1.0)


def sum_reward_contribution(
    state: Mapping[str, Any] | None,
    records: Iterable[tuple[str, Mapping[str, Any]]],
) -> tuple[int, float, int, int]:
    """汇总多条记录：``(总伤害, 总贡献(≤1), 首波, 末波)``。"""
    total_damage = 0
    total_contribution = 0.0
    first_wave = 0
    last_wave = 0
    for _record_key, record in records:
        wave = record_wave(record)
        if first_wave <= 0 or wave < first_wave:
            first_wave = wave
        if wave > last_wave:
            last_wave = wave
        realm = record.get("realm") or ""
        total_damage += to_int(record.get("damage"), 0)
        total_contribution += record_reward_contribution(state, record, realm, wave)
    return total_damage, clamp(total_contribution, 0.0, 1.0), first_wave, last_wave


__all__ = [
    "DEMON_EXP_REWARD_CAP_RATE",
    "DEMON_FULL_RANDOM_POOL_CONTRIBUTION",
    "DEMON_MIN_REWARD_CONTRIBUTION",
    "DEMON_STONE_REWARD_CAP",
    "DEMON_TALISMAN_CONTRIBUTION_STEP",
    "DEMON_TALISMAN_ITEM_ID",
    "DEMON_TALISMAN_REWARD_CAP",
    "boss_reward_multiplier",
    "clamp",
    "describe_random_reward",
    "exp_reward_rate",
    "format_contribution_percent",
    "is_random_reward_item",
    "is_wushang_reward_item",
    "pick_random_reward",
    "random_reward_pool",
    "record_reward_base_hp",
    "record_reward_contribution",
    "record_reward_multiplier",
    "record_wave",
    "reward_contribution",
    "stone_reward",
    "sum_reward_contribution",
    "talisman_reward_count",
    "to_float",
    "to_int",
]
