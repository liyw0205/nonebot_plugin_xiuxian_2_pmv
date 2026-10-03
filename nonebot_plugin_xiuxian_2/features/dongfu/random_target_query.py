from __future__ import annotations

import asyncio
import json
from typing import Mapping

from .plant_slots import as_int, normalize_plant_slots
from .random_target_repository import DongfuCandidateReadError


CANDIDATE_YIELD_INTERVAL = 32


async def select_random_target(
    repository,
    *,
    user_id: str,
    day: str,
    target_limit: int,
    base_plot_count: int,
    max_plot_count: int,
    fertilizer_max: int,
    seed_names: Mapping[int, str],
    random_source,
) -> dict[str, str] | None:
    user_id, day = str(user_id), str(day)
    seed_names = dict(seed_names)
    selected = None
    eligible_count = 0
    processed = 0
    after_rowid = None
    try:
        upper_rowid = repository.upper_rowid()
        if upper_rowid is None:
            return None
        while after_rowid is None or after_rowid < upper_rowid:
            rows = repository.page(after_rowid, upper_rowid)
            if not rows:
                break
            for row in rows:
                cursor = int(row["cursor"])
                if cursor > upper_rowid or (after_rowid is not None and cursor <= after_rowid):
                    raise DongfuCandidateReadError("candidate cursor did not advance")
                after_rowid = cursor
                processed += 1
                if processed % CANDIDATE_YIELD_INTERVAL == 0:
                    await asyncio.sleep(0)
                candidate_id = row["user_id"]
                if not row["is_built"] or candidate_id is None or str(candidate_id) == user_id:
                    continue
                data = repository.candidate(str(candidate_id))
                if data is None:
                    continue
                dongfu = dict(data)
                for field in (
                    "built", "plant_slots", "plot_count", "planting",
                    "plant_seed_id", "plant_start", "plant_finish",
                    "intrude_date", "intrude_count",
                ):
                    value = dongfu.get(field)
                    if isinstance(value, str):
                        try:
                            dongfu[field] = json.loads(value)
                        except ValueError:
                            pass
                if as_int(dongfu.get("built")) != 1:
                    continue
                slots = normalize_plant_slots(
                    dongfu,
                    base_plot_count=base_plot_count,
                    max_plot_count=max_plot_count,
                    fertilizer_max=fertilizer_max,
                    seed_names=seed_names,
                )
                if not any(slot["seed_id"] in seed_names for slot in slots):
                    continue
                count = as_int(dongfu.get("intrude_count")) if dongfu.get("intrude_date") == day else 0
                if count >= target_limit:
                    continue
                eligible_count += 1
                if random_source.randint(1, eligible_count) == 1:
                    selected = {"user_id": str(data["user_id"]), "user_name": data["user_name"]}
            await asyncio.sleep(0)
    except DongfuCandidateReadError:
        return None
    return selected


__all__ = ["select_random_target"]
