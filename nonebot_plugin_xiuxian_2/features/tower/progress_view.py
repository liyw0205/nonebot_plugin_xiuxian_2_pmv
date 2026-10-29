from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class TowerPurchaseSummary:
    used: int
    limit: int
    exhausted: int


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def next_milestone(max_floor: Any) -> int:
    floor = max(_integer(max_floor, 0), 0)
    return ((floor // 10) + 1) * 10


def weekly_purchase_summary(
    weekly_purchases: Any, shop_items: Any
) -> TowerPurchaseSummary:
    purchases = weekly_purchases if isinstance(weekly_purchases, Mapping) else {}
    items = shop_items if isinstance(shop_items, Mapping) else {}
    used = limit = exhausted = 0
    for item_id, item in items.items():
        if not isinstance(item, Mapping):
            continue
        cap = _integer(item.get("weekly_limit"), 0)
        if cap <= 0:
            continue
        raw_amount = purchases.get(str(item_id))
        if raw_amount is None:
            raw_amount = purchases.get(item_id)
        if raw_amount is None:
            try:
                raw_amount = purchases.get(int(item_id))
            except (TypeError, ValueError):
                raw_amount = None
        amount = max(_integer(raw_amount, 0), 0)
        used += min(amount, cap)
        limit += cap
        if amount >= cap:
            exhausted += 1
    return TowerPurchaseSummary(used, limit, exhausted)


def render_tower_progress(
    *,
    current_floor: Any,
    max_floor: Any,
    score: Any,
    weekly_purchases: Any,
    shop_items: Any,
    config: Any,
) -> str:
    floor = max(_integer(max_floor, 0), 0)
    milestone = next_milestone(floor)
    remaining = max(milestone - floor, 0)
    summary = weekly_purchase_summary(weekly_purchases, shop_items)
    settings = config if isinstance(config, Mapping) else {}
    score_config = settings.get("积分奖励")
    stone_config = settings.get("灵石奖励")
    exp_config = settings.get("修为奖励")
    extra_score = _integer(
        score_config.get("每10层额外") if isinstance(score_config, Mapping) else 0,
        0,
    )
    extra_stone = _integer(
        stone_config.get("每10层额外") if isinstance(stone_config, Mapping) else 0,
        0,
    )
    try:
        exp_rate = (
            _number(exp_config.get("每10层"))
            if isinstance(exp_config, Mapping)
            else 0.0
        )
    except (TypeError, ValueError):
        exp_rate = 0.0

    lines = [
        "",
        "【进度与里程碑】",
        f"下一里程碑：第{milestone}层（还差{remaining}层）",
        "首通奖励："
        f"积分+{extra_score:,} / 灵石+{extra_stone:,} / 修为+{max(exp_rate, 0.0) * 100:.2f}%",
        (
            f"本周兑换：{summary.used}/{summary.limit}（已兑完 {summary.exhausted} 项）"
            if summary.limit > 0
            else "本周兑换：暂无有效限购商品"
        ),
        "每周一 0 点重置层数与商店限购。",
    ]
    return "\n".join(lines)


__all__ = [
    "TowerPurchaseSummary",
    "next_milestone",
    "render_tower_progress",
    "weekly_purchase_summary",
]
