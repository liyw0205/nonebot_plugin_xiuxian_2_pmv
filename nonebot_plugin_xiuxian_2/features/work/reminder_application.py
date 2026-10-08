"""Application facade for Work delayed reminders.

The facade intentionally has no NoneBot dependency.  A matcher adapter can
pass its bot/event as ``context`` and turn :class:`ReminderNotification` into
the existing ``handle_send`` call when the Work matcher is cut over.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .reminder_scheduler import (
    Notifier,
    ReminderResult,
    ReminderState,
    StatusReader,
    WorkReminderScheduler,
)


class WorkReminderApplication:
    """Feature-owned schedule/cancel/consume boundary for reminders."""

    def __init__(self, scheduler: WorkReminderScheduler | None = None, **kwargs: Any) -> None:
        self.scheduler = scheduler or WorkReminderScheduler(**kwargs)

    def state(self, user_id: str) -> ReminderState | None:
        return self.scheduler.state(user_id)

    def schedule(
        self,
        user_id: str,
        *,
        status: int,
        status_reader: StatusReader,
        notifier: Notifier,
        refresh_time: datetime | None = None,
        context: Any = None,
    ) -> ReminderResult:
        return self.scheduler.schedule(
            user_id,
            status=status,
            status_reader=status_reader,
            notifier=notifier,
            refresh_time=refresh_time,
            context=context,
        )

    def cancel(self, user_id: str) -> ReminderResult:
        return self.scheduler.cancel(user_id)

    async def consume(
        self,
        user_id: str,
        *,
        status_reader: StatusReader,
        notifier: Notifier,
        generation: int | None = None,
        context: Any = None,
    ) -> ReminderResult:
        return await self.scheduler.consume(
            user_id,
            generation=generation,
            status_reader=status_reader,
            notifier=notifier,
            context=context,
        )

    def shutdown(self) -> None:
        self.scheduler.shutdown()


__all__ = ["WorkReminderApplication"]
