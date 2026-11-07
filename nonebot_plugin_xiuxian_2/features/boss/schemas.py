"""Transport DTOs are represented by the validated domain requests.

同一模块还承载世界BOSS伤害上限的**纯渲染**说明（`boss_damage_note`）：
`actual_damage = min(total_damage, boss_all_hp*0.2, boss_old_hp)` 一直生效，
但回执只写「受天道限制，实际削减…」，玩家会把单次 20% 上限读成 bug。
说明函数只读渲染，不改伤害与奖励数学。
"""

from __future__ import annotations

from typing import Any

from .domain import BossPurchaseRequest, BossSettlementRequest


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def boss_damage_note(
    total: Any,
    actual: Any = None,
    max_single: Any = None,
    hp_left: Any = None,
) -> str:
    """返回本次伤害被上限截断的原因说明；未被截断时返回空串。

    判定与 `xiuxian_boss` 的
    `actual_damage = min(total_damage, boss_all_hp*0.2, boss_old_hp)` 同序：

    1. 实际伤害已不低于总伤害 → 没有截断，返回空串；
    2. 单次上限（`boss_all_hp * 0.2`）是最紧的一项 → 命名「单次最多削减 20%」；
       上限与剩余血量同分时优先报单次上限，因为那是每个玩家都会撞到的固定规则；
    3. 否则说明是被剩余血量截断（超出的伤害无处结算）。

    两个上限都缺失、或数值无法解析时返回空串，避免在旧调用点上凭空造出说明。
    """
    total_value = max(0, _int(total))
    if actual is not None and max(0, _int(actual)) >= total_value:
        return ""

    cap = None if max_single is None else max(0, _int(max_single))
    remaining = None if hp_left is None else max(0, _int(hp_left))
    if cap is None and remaining is None:
        return ""
    if cap is not None and (remaining is None or cap <= remaining):
        return (
            f"说明：单次讨伐最多削减世界BOSS最大血量的20%（本次上限 {cap}），"
            f"超出的伤害不结算。"
        )
    return (
        f"说明：世界BOSS剩余血量只剩 {remaining}，"
        f"超出的伤害无处结算。"
    )


__all__ = ["BossPurchaseRequest", "BossSettlementRequest", "boss_damage_note"]
