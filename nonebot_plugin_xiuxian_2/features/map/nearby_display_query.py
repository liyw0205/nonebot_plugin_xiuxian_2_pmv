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

    def finish() -> list[dict[str, Any]]:
        if eligible_count > DISPLAY_LIMIT:
            return [user for _, user in random_source.sample(selected, len(selected))]
        selected.sort(key=lambda entry: entry[0])
        return [user for _, user in selected]

    try:
        if getattr(repository, "optimized", False) and hasattr(repository, "candidates"):
            processed = 0
            with repository.candidates((0, 0), position, exclude_user_id) as candidates:
                for candidate in candidates:
                    cursor = (processed, processed)
                    eligible_count += 1
                    if len(selected) < DISPLAY_LIMIT:
                        selected.append((cursor, candidate))
                    else:
                        slot = random_source.randint(1, eligible_count)
                        if slot <= DISPLAY_LIMIT:
                            selected[slot - 1] = (cursor, candidate)
                    processed += 1
                    if processed % 32 == 0:
                        await asyncio.sleep(0)
            return finish()
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
    return finish()


async def select_nearby_display_for_user(
    repository,
    *,
    user_id: str,
    exclude_user_id: str,
    status_validator,
    random_source,
) -> tuple[dict[str, str], list[dict[str, Any]]] | None:
    """Combine the actor-position and nearby read transaction for the hot command."""
    try:
        with repository.candidates_for_user(
            user_id, exclude_user_id, status_validator,
        ) as snapshot:
            if snapshot is None:
                return None
            status, candidates = snapshot
            selected: list[tuple[int, dict[str, Any]]] = []
            eligible_count = 0
            processed = 0
            for candidate in candidates:
                eligible_count += 1
                cursor = (processed, processed)
                if len(selected) < DISPLAY_LIMIT:
                    selected.append((cursor[0], candidate))
                else:
                    slot = random_source.randint(1, eligible_count)
                    if slot <= DISPLAY_LIMIT:
                        selected[slot - 1] = (cursor[0], candidate)
                processed += 1
                if processed % 32 == 0:
                    await asyncio.sleep(0)
            if eligible_count > DISPLAY_LIMIT:
                users = [user for _, user in random_source.sample(selected, len(selected))]
            else:
                selected.sort(key=lambda entry: entry[0])
                users = [user for _, user in selected]
            return status, users
    except MapCandidateReadError:
        return None


__all__ = ["select_nearby_display", "select_nearby_display_for_user"]
