from __future__ import annotations

import math
from typing import Any


BONUS_FIELDS = (
    "impart_two_exp", "impart_exp_up", "impart_atk_per", "impart_hp_per",
    "impart_mp_per", "boss_atk", "impart_know_per", "impart_burst_per",
    "impart_mix_per", "impart_reap_per",
)

# 虚神界修为乘区成长（非结算硬限速）：卡堆叠收益递减；lv 线性系数低于旧 0.1
# 目标：closing_exp 抬高后，满卡+活跃 lv 相对普通闭关仍有优势，但不至理论数天破境
XU_EXP_UP_SOFT_MAX = 1.0
XU_EXP_UP_TAU = 1.2
XU_IMPART_LV_RATE = 0.03
XU_IMPART_LV_MAX = 30


def calculate_card_bonuses(cards, definitions):
    bonuses = {field: 0 for field in BONUS_FIELDS}
    for card_name, count in dict(cards).items():
        card = definitions.get(card_name)
        if not card or card.get("type") not in bonuses:
            continue
        effective_count = min(max(int(count), 0), 25)
        bonuses[card["type"]] += card["vale"] * (1 + effective_count // 5)
    return bonuses


def refresh_card_bonuses(conn, user_id, definitions):
    cards = dict(conn.execute(
        "SELECT card_name,quantity FROM impart_cards WHERE user_id=%s", (str(user_id),)
    ).fetchall())
    bonuses = calculate_card_bonuses(cards, definitions)
    assignments = ",".join(f"{field}=%s" for field in BONUS_FIELDS)
    values = [bonuses[field] for field in BONUS_FIELDS]
    updated = conn.execute(
        f"UPDATE xiuxian_impart SET {assignments} WHERE user_id=%s", (*values, str(user_id))
    )
    if updated.rowcount != 1:
        raise ValueError("impart user is missing")
    return bonuses


def effective_xu_impart_exp_up(raw_exp_up: Any) -> float:
    """Concave map of stacked card impart_exp_up for 虚神界修为 only.

    Stored card totals unchanged (展示/养成仍见原值)；仅乘入 per_min 时用本值。
    """
    try:
        raw = float(raw_exp_up or 0.0)
    except (TypeError, ValueError):
        raw = 0.0
    if raw <= 0:
        return 0.0
    return float(XU_EXP_UP_SOFT_MAX * (1.0 - math.exp(-raw / XU_EXP_UP_TAU)))


def xu_impart_lv_bonus(impart_lv: Any) -> float:
    """Level contribution to 虚神界 double/修炼: was lv*0.1, now lv*0.03."""
    try:
        lv = int(impart_lv or 0)
    except (TypeError, ValueError):
        lv = 0
    lv = max(0, min(lv, XU_IMPART_LV_MAX))
    return float(lv * XU_IMPART_LV_RATE)


__all__ = [
    "BONUS_FIELDS",
    "calculate_card_bonuses",
    "refresh_card_bonuses",
    "effective_xu_impart_exp_up",
    "xu_impart_lv_bonus",
    "XU_EXP_UP_SOFT_MAX",
    "XU_EXP_UP_TAU",
    "XU_IMPART_LV_RATE",
    "XU_IMPART_LV_MAX",
]
