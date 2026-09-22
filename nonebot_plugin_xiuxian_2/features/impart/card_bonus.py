from __future__ import annotations

from typing import Any

BONUS_FIELDS = (
    "impart_two_exp", "impart_exp_up", "impart_atk_per", "impart_hp_per",
    "impart_mp_per", "boss_atk", "impart_know_per", "impart_burst_per",
    "impart_mix_per", "impart_reap_per",
)


def calculate_card_bonuses(cards: dict[str, int], definitions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    bonuses = {field: 0 for field in BONUS_FIELDS}
    for card_name, count in cards.items():
        card = definitions.get(card_name)
        if not card or card.get("type") not in bonuses:
            continue
        effective_count = min(max(int(count), 0), 25)
        bonuses[card["type"]] += card["vale"] * (1 + effective_count // 5)
    return bonuses


def refresh_card_bonuses(connection, user_id: str, definitions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    cards = {
        str(row["card_name"]): int(row["quantity"])
        for row in connection.execute(
            "SELECT card_name,quantity FROM impart_data.impart_cards WHERE user_id=?",
            (str(user_id),),
        ).fetchall()
    }
    bonuses = calculate_card_bonuses(cards, definitions)
    assignments = ",".join(f'"{field}"=?' for field in BONUS_FIELDS)
    values = [bonuses[field] for field in BONUS_FIELDS]
    updated = connection.execute(
        f"UPDATE impart_data.xiuxian_impart SET {assignments} WHERE user_id=?",
        (*values, str(user_id)),
    )
    if updated.rowcount != 1:
        raise ValueError("impart user is missing")
    return bonuses


__all__ = ["BONUS_FIELDS", "calculate_card_bonuses", "refresh_card_bonuses"]
