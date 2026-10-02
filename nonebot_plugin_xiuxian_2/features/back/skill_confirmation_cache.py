"""Bounded in-process state for pending skill-learning confirmations."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import time
from typing import Callable


@dataclass(frozen=True)
class PendingSkillConfirmation:
    goods_id: int
    item_name: str
    skill_type: str
    invite_id: str
    expires_at: float


class SkillConfirmationCache:
    def __init__(
        self,
        *,
        ttl_seconds: float = 30.0,
        max_entries: int = 2048,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._ttl_seconds = float(ttl_seconds)
        self._max_entries = int(max_entries)
        self._clock = clock
        self._entries: OrderedDict[str, PendingSkillConfirmation] = OrderedDict()

    def put(
        self,
        user_id: str,
        *,
        goods_id: int,
        item_name: str,
        skill_type: str,
        invite_id: str,
    ) -> PendingSkillConfirmation:
        now = self._clock()
        self._purge_expired(now)
        key = str(user_id)
        self._entries.pop(key, None)
        if len(self._entries) >= self._max_entries:
            self._entries.popitem(last=False)
        confirmation = PendingSkillConfirmation(
            goods_id=int(goods_id),
            item_name=str(item_name),
            skill_type=str(skill_type),
            invite_id=str(invite_id),
            expires_at=now + self._ttl_seconds,
        )
        self._entries[key] = confirmation
        return confirmation

    def get(self, user_id: str) -> PendingSkillConfirmation | None:
        self._purge_expired(self._clock())
        return self._entries.get(str(user_id))

    def discard(self, user_id: str, *, expected_invite_id: str | None = None) -> bool:
        self._purge_expired(self._clock())
        key = str(user_id)
        confirmation = self._entries.get(key)
        if confirmation is None:
            return False
        if expected_invite_id is not None and confirmation.invite_id != str(expected_invite_id):
            return False
        del self._entries[key]
        return True

    def seconds_until_next_expiry(self) -> float | None:
        now = self._clock()
        self._purge_expired(now)
        if not self._entries:
            return None
        first = next(iter(self._entries.values()))
        return max(0.0, first.expires_at - now)

    def purge_expired(self) -> int:
        return self._purge_expired(self._clock())

    def _purge_expired(self, now: float) -> int:
        removed = 0
        while self._entries:
            _, confirmation = next(iter(self._entries.items()))
            if confirmation.expires_at > now:
                break
            self._entries.popitem(last=False)
            removed += 1
        return removed
