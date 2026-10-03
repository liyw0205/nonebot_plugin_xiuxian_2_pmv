from __future__ import annotations

import asyncio
from typing import Any

from .random_target_repository import CANDIDATE_PAGE_SIZE, MapCandidateReadError


CANDIDATE_YIELD_INTERVAL = 32


async def select_random_nearby_target(
    repository,
    *,
    realm: str,
    heaven: str,
    node_id: str,
    exclude_user_id: str,
    random_source,
) -> dict[str, Any] | None:
    position = tuple(str(value or "") for value in (realm, heaven, node_id))
    if not all(position):
        return None
    exclude_user_id = str(exclude_user_id)
    selected = None
    eligible_count = 0
    processed = 0
    after = None
    try:
        upper_rowids = repository.upper_rowids()
        if upper_rowids is None:
            return None
        while True:
            rows = repository.page(after, upper_rowids, position, exclude_user_id)
            if not rows:
                break
            if len(rows) > CANDIDATE_PAGE_SIZE:
                raise MapCandidateReadError("candidate page exceeded its bound")
            for row in rows:
                try:
                    cursor = int(row["map_cursor"]), int(row["profile_cursor"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise MapCandidateReadError("candidate cursor unavailable") from exc
                if (cursor[0] > upper_rowids[0] or cursor[1] > upper_rowids[1]
                        or (after is not None and cursor <= after)):
                    raise MapCandidateReadError("candidate cursor did not advance within bounds")
                after = cursor
                candidate = repository.candidate(cursor, upper_rowids, position, exclude_user_id)
                if candidate is not None:
                    eligible_count += 1
                    if random_source.randint(1, eligible_count) == 1:
                        selected = candidate
                candidate = None
                processed += 1
                if processed % CANDIDATE_YIELD_INTERVAL == 0:
                    await asyncio.sleep(0)
            del rows
            await asyncio.sleep(0)
    except MapCandidateReadError:
        return None
    return selected


__all__ = ["select_random_nearby_target"]
