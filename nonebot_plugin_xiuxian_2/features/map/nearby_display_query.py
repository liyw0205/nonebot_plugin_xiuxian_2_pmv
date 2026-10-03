from __future__ import annotations

import asyncio
from typing import Any

from .nearby_display_repository import DISPLAY_PAGE_SIZE
from .random_target_repository import MapCandidateReadError


DISPLAY_LIMIT = 10


async def select_nearby_display(
    repository,
    *,
    realm: str,
    heaven: str,
    node_id: str,
    exclude_user_id: str,
    random_source,
) -> list[dict[str, Any]]:
    position = tuple(str(value or "") for value in (realm, heaven, node_id))
    if not all(position):
        return []
    exclude_user_id = str(exclude_user_id)
    selected = []
    eligible_count = 0
    after = None
    try:
        upper_rowids = repository.upper_rowids()
        if upper_rowids is None:
            return []
        while True:
            rows = repository.page(after, upper_rowids, position, exclude_user_id)
            if not rows:
                break
            if len(rows) > DISPLAY_PAGE_SIZE:
                raise MapCandidateReadError("display page exceeded its bound")
            for row in rows:
                try:
                    user_cursor = row["user_cursor"]
                    cursor = int(row["map_cursor"]), int(row["profile_cursor"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise MapCandidateReadError("display cursor unavailable") from exc
                if (not isinstance(user_cursor, str)
                        or cursor[0] > upper_rowids[0] or cursor[1] > upper_rowids[1]
                        or (after is not None and user_cursor <= after)):
                    raise MapCandidateReadError("display cursor did not advance within bounds")
                after = user_cursor
                candidate = repository.candidate(
                    cursor, upper_rowids, position, exclude_user_id,
                    expected_user_id=user_cursor,
                )
                if candidate is not None:
                    if candidate["user_id"] != user_cursor:
                        raise MapCandidateReadError("display identity changed during read")
                    eligible_count += 1
                    if len(selected) < DISPLAY_LIMIT:
                        selected.append((cursor, candidate))
                    else:
                        slot = random_source.randint(1, eligible_count)
                        if slot <= DISPLAY_LIMIT:
                            selected[slot - 1] = (cursor, candidate)
                candidate = None
            del rows
            await asyncio.sleep(0)
    except MapCandidateReadError:
        return []
    if eligible_count > DISPLAY_LIMIT:
        selected = random_source.sample(selected, len(selected))
    else:
        selected.sort(key=lambda entry: entry[0])
    return [user for _, user in selected]


__all__ = ["select_nearby_display"]
