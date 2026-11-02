"""魔修入侵领奖的结算组装（纯函数，无 I/O、无 NoneBot）。

`领取魔修奖励` 的奖励原本在兼容命令模块里逐行拼装：贡献压档、灵石、修为比例、
灵签宝箓数量、随机奖励抽取与奖励物品列表都混在 handler 中。现在这些组装集中到本模块，
由 `features/world_events/reward_rules.py` 提供数值规则，命令适配器只提供
`world_event_state` 快照、物品目录与随机源，然后发送消息。

修为奖励仍由调用方使用共享的 `percent_exp_reward`（gap 锚定 + 境界压制）结算，
本模块只产出**比例**，不复制该数值绑定。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from .reward_rules import (
    DEMON_TALISMAN_ITEM_ID,
    describe_random_reward,
    exp_reward_rate,
    pick_random_reward,
    random_reward_pool,
    reward_contribution,
    stone_reward,
    talisman_reward_count,
    to_int,
)

NO_RANDOM_REWARD_TEXT = "未获得"


@dataclass(frozen=True)
class DemonRewardPlan:
    """一次领奖的结算明细；全部字段只读且可直接渲染。"""

    raw_contribution: float
    contribution: float
    stone: int
    exp_rate: float
    talisman_count: int
    random_reward_id: int | None = None
    random_reward: Mapping[str, Any] | None = None
    random_reward_text: str = NO_RANDOM_REWARD_TEXT
    items: tuple[Mapping[str, Any], ...] = field(default_factory=tuple)

    @property
    def talisman_reward(self) -> int:
        return self.talisman_count

    @property
    def reward_items(self) -> list[Mapping[str, Any]]:
        return list(self.items)


def _talisman_item(talisman_item: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if not isinstance(talisman_item, Mapping):
        return None
    name = talisman_item.get("name")
    item_type = talisman_item.get("type")
    if not name:
        return None
    return {
        "id": DEMON_TALISMAN_ITEM_ID,
        "name": name,
        "type": item_type,
    }


def build_demon_reward_plan(
    *,
    raw_contribution: Any,
    talisman_item: Mapping[str, Any] | None = None,
    item_catalog: Mapping[Any, Mapping[str, Any]] | Iterable[tuple[Any, Mapping[str, Any]]] = (),
    rng: Any,
) -> DemonRewardPlan:
    """按贡献组装一次领奖的完整奖励明细。

    ``rng`` 必须提供 ``choice`` / ``random`` / ``randint``（与 ``SystemRandom`` 同形）。
    随机奖励只在 ``rng.random() <= 贡献`` 时抽取，与历史行为一致；无上技能/装备
    仅 1% 保留由 ``reward_rules.pick_random_reward`` 保证。
    """
    raw = float(raw_contribution or 0.0)
    contribution = reward_contribution(raw)
    stone = stone_reward(contribution)
    rate = exp_reward_rate(contribution)
    talisman_count = talisman_reward_count(contribution)

    items: list[Mapping[str, Any]] = []
    talisman = _talisman_item(talisman_item)
    if talisman is not None and talisman_count > 0:
        items.append({**talisman, "amount": talisman_count})

    random_reward_id: int | None = None
    random_reward: Mapping[str, Any] | None = None
    random_reward_text = NO_RANDOM_REWARD_TEXT
    if rng.random() <= contribution:
        pool = random_reward_pool(item_catalog, contribution)
        pick = pick_random_reward(pool, rng)
        if pick is not None:
            random_reward_id, random_reward = pick
            random_reward_text = describe_random_reward(random_reward_id, random_reward) or NO_RANDOM_REWARD_TEXT
            items.append(
                {
                    "id": random_reward_id,
                    "name": random_reward.get("name", f"未知物品{random_reward_id}"),
                    "type": random_reward.get("type"),
                    "amount": 1,
                }
            )

    return DemonRewardPlan(
        raw_contribution=raw,
        contribution=contribution,
        stone=stone,
        exp_rate=rate,
        talisman_count=talisman_count,
        random_reward_id=random_reward_id,
        random_reward=random_reward,
        random_reward_text=random_reward_text,
        items=tuple(items),
    )


def contribution_matches_raw(raw_contribution: Any, contribution: Any, *, tolerance: float = 1e-12) -> bool:
    """领奖文案用：结算贡献是否与原始贡献一致（用于决定是否标注「按 X% 结算」）。"""
    return abs(float(contribution or 0.0) - float(raw_contribution or 0.0)) <= tolerance


def wave_text(first_wave: Any, last_wave: Any) -> str:
    """领奖文案用：把波次区间渲染成历史格式（未知/单波/区间）。"""
    first = to_int(first_wave, 0)
    last = to_int(last_wave, 0)
    if first <= 0:
        return "未知"
    if first == last:
        return f"第{first}波"
    return f"第{first}-{last}波"


__all__ = [
    "DemonRewardPlan",
    "NO_RANDOM_REWARD_TEXT",
    "build_demon_reward_plan",
    "contribution_matches_raw",
    "wave_text",
]
