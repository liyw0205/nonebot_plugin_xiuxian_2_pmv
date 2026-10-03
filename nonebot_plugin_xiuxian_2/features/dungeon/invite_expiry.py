from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

from ...core.ports import Clock
from ...infrastructure.database import is_database_busy
from .team_application import DungeonTeamApplication
from .team_repository import TeamInviteSnapshot


@dataclass(frozen=True)
class InviteExpiryReport:
    status: str
    scanned: int = 0
    applied: int = 0
    failures: int = 0
    notification_failures: int = 0


class DungeonInviteExpiryWorker:
    """Bound state recovery independently of best-effort notification delivery."""

    def __init__(
        self,
        application: DungeonTeamApplication,
        *,
        clock: Clock,
        notify: Callable[[TeamInviteSnapshot], Awaitable[None]],
        notification_timeout: float = 2.0,
        notification_budget: float = 10.0,
    ) -> None:
        self.application = application
        self.clock = clock
        self.notify = notify
        self.notification_timeout = max(float(notification_timeout), 0.01)
        self.notification_budget = max(float(notification_budget), 0.0)
        self._lock = asyncio.Lock()
        self._cursor = None

    async def run(self) -> InviteExpiryReport:
        if self._lock.locked():
            return InviteExpiryReport("busy")
        async with self._lock:
            try:
                batch = self.application.due_invites(self.clock.now().timestamp(), limit=100, after=self._cursor)
            except Exception as exc:
                if is_database_busy(exc):
                    self._cursor = None
                    return InviteExpiryReport("busy", failures=1)
                raise
            if batch.status != "ok":
                return InviteExpiryReport(batch.status)
            applied = notification_failures = 0
            failures = batch.failures
            notifications = []
            busy = False
            for invite in batch.invites:
                await asyncio.sleep(0)
                try:
                    result = self.application.expire(
                        f"dungeon-team-expire:{invite.invite_id}",
                        invite.invite_id,
                        self.clock.now().timestamp(),
                        lock_timeout=0,
                    )
                except Exception as exc:
                    failures += 1
                    if is_database_busy(exc):
                        busy = True
                        break
                    continue
                if result.status not in {"applied", "duplicate", "invite_invalid", "not_expired"}:
                    failures += 1
                if result.status != "applied":
                    continue
                applied += 1
                notifications.append(invite)
            self._cursor = None if busy else batch.next_cursor
            # Commit every state transition before spending time on delivery.
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self.notification_budget
            for index, invite in enumerate(notifications):
                remaining = deadline - loop.time()
                if remaining <= 0:
                    notification_failures += len(notifications) - index
                    break
                try:
                    await asyncio.wait_for(
                        self.notify(invite), timeout=min(self.notification_timeout, remaining)
                    )
                except Exception:
                    notification_failures += 1
            return InviteExpiryReport("busy" if busy else "ok", len(batch.invites) + batch.failures, applied, failures, notification_failures)
