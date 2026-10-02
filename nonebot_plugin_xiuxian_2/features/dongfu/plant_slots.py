from __future__ import annotations

import json
from typing import Any, Mapping


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def empty_plant_slot(slot_no: int) -> dict[str, Any]:
    return {
        "slot": slot_no,
        "seed_id": 0,
        "seed_name": "",
        "plant_start": "",
        "plant_finish": "",
        "fertilizer": 0,
    }


def normalize_plant_slots(
    dongfu: dict[str, Any],
    *,
    base_plot_count: int,
    max_plot_count: int,
    fertilizer_max: int,
    seed_names: Mapping[int, str],
) -> list[dict[str, Any]]:
    plot_count = min(
        max_plot_count,
        max(base_plot_count, as_int(dongfu.get("plot_count"), base_plot_count)),
    )
    raw_slots = dongfu.get("plant_slots")
    if isinstance(raw_slots, str):
        try:
            raw_slots = json.loads(raw_slots)
        except (TypeError, ValueError):
            raw_slots = []
    if not isinstance(raw_slots, list):
        raw_slots = []

    slots = []
    for index in range(plot_count):
        raw = raw_slots[index] if index < len(raw_slots) and isinstance(raw_slots[index], dict) else {}
        seed_id = as_int(raw.get("seed_id"))
        slots.append(
            {
                "slot": index + 1,
                "seed_id": seed_id,
                "seed_name": str(raw.get("seed_name") or seed_names.get(seed_id, "")),
                "plant_start": str(raw.get("plant_start") or ""),
                "plant_finish": str(raw.get("plant_finish") or ""),
                "fertilizer": min(fertilizer_max, max(0, as_int(raw.get("fertilizer")))),
            }
        )

    legacy_seed_id = as_int(dongfu.get("plant_seed_id"))
    if not any(slot["seed_id"] for slot in slots) and as_int(dongfu.get("planting")) == 1 and legacy_seed_id:
        slots[0].update(
            {
                "seed_id": legacy_seed_id,
                "seed_name": str(slots[0]["seed_name"] or seed_names.get(legacy_seed_id, "")),
                "plant_start": str(dongfu.get("plant_start") or ""),
                "plant_finish": str(dongfu.get("plant_finish") or ""),
            }
        )

    dongfu["plot_count"] = plot_count
    dongfu["plant_slots"] = slots
    return slots


def legacy_plant_fields(
    slots: list[dict[str, Any]],
    *,
    valid_seed_ids: set[int] | None = None,
) -> tuple[int, int, str, str]:
    active = next(
        (
            slot
            for slot in slots
            if as_int(slot.get("seed_id")) > 0
            and (valid_seed_ids is None or as_int(slot.get("seed_id")) in valid_seed_ids)
        ),
        None,
    )
    if active is None:
        return 0, 0, "", ""
    return (
        1,
        as_int(active.get("seed_id")),
        str(active.get("plant_start") or ""),
        str(active.get("plant_finish") or ""),
    )


def canonical_plant_slots(slots: list[dict[str, Any]]) -> str:
    return json.dumps(slots, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


__all__ = [
    "as_int",
    "canonical_plant_slots",
    "empty_plant_slot",
    "legacy_plant_fields",
    "normalize_plant_slots",
]
