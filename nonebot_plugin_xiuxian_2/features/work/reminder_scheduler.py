"""In-memory scheduler for delayed Work offer reminders.

The legacy matcher keeps reminder state in two module globals.  This module
owns that state and the task lifecycle without depending on NoneBot objects.
The caller supplies a status reader and notification callback so the owner can
be wired into the matcher incrementally.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Awaitable, Callable, Mapping, Protocol

from ...infrastructure.clock import SystemClock


class ReminderClock(Protocol):
    def now(self) -> datetime: ...


StatusReader = Callable[[str], Any | Awaitable[Any]]
Notifier = Callable[["ReminderNotification"], Any | Awaitable[Any]]
Sleep = Callable[[float], Awaitable[Any]]
CreateTask = Callable[[Awaitable[Any]], Any]


@dataclass(frozen=True)
class ReminderState:
    """Current lifecycle state for one user's reminder."""

    user_id: str
    pending: bool
    reminded: bool
    refresh_time: datetime
    generation: int
    outcome: str = "scheduled"


@dataclass(frozen=True)
class ReminderNotification:
    """Payload handed to the adapter that renders/sends a reminder."""

    user_id: str
    refresh_time: datetime
    now: datetime
    remaining_minutes: int
    context: Any = None


@dataclass(frozen=True)
class ReminderResult:
    status: str
    state: ReminderState | None = None
    notification: ReminderNotification | None = None
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"scheduled", "replaced", "consumed"}


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _status_value(raw: Any) -> int:
    """Accept both the legacy ``(status, work_data)`` result and an int."""
    if isinstance(raw, tuple) and raw:
        raw = raw[0]
    if isinstance(raw, Mapping):
        raw = raw.get("status")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("work status must be an integer") from exc


class WorkReminderScheduler:
    """Own per-user reminder state and delayed task lifecycle.

    A new schedule invalidates the previous generation before creating a task.
    This prevents a cancelled or slow task from consuming a replacement
    reminder after it wakes up.
    """

    def __init__(
        self,
        *,
        delay_seconds: float = 180,
        expire_minutes: int = 30,
        clock: ReminderClock | None = None,
        sleep: Sleep = asyncio.sleep,
        create_task: CreateTask = asyncio.create_task,
    ) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must not be negative")
        if expire_minutes < 0:
            raise ValueError("expire_minutes must not be negative")
        self.delay_seconds = float(delay_seconds)
        self.expire_minutes = int(expire_minutes)
        self.clock = clock or SystemClock()
        self._sleep = sleep
        self._create_task = create_task
        self._states: dict[str, ReminderState] = {}
        self._tasks: dict[str, Any] = {}
        self._next_generation: dict[str, int] = {}

    def state(self, user_id: str) -> ReminderState | None:
        return self._states.get(str(user_id))

    def task(self, user_id: str) -> Any | None:
        return self._tasks.get(str(user_id))

    def _generation(self, user_id: str) -> int:
        user_id = str(user_id)
        generation = self._next_generation.get(user_id, 0) + 1
        self._next_generation[user_id] = generation
        return generation

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
        """Schedule a reminder only for an unaccepted offer (status ``3``).

        Non-3 status cancels any existing reminder and returns ``ignored``;
        this makes stale tasks harmless when a claim/settlement races with the
        scheduler.  ``status_reader`` is checked again when the delay elapses.
        """
        user_id = str(user_id)
        if not callable(status_reader) or not callable(notifier):
            raise TypeError("status_reader and notifier must be callable")
        if int(status) != 3:
            cancelled = self.cancel(user_id)
            return ReminderResult("ignored", cancelled.state)

        previous = self._states.get(user_id)
        replaced = previous is not None and previous.pending
        if previous is not None or user_id in self._tasks:
            self.cancel(user_id)

        generation = self._generation(user_id)
        state = ReminderState(
            user_id=user_id,
            pending=True,
            reminded=False,
            refresh_time=refresh_time or self.clock.now(),
            generation=generation,
        )
        self._states[user_id] = state
        task = self._create_task(
            self._run(
                user_id,
                generation,
                status_reader=status_reader,
                notifier=notifier,
                context=context,
            )
        )
        self._tasks[user_id] = task
        return ReminderResult("replaced" if replaced else "scheduled", state)

    def cancel(self, user_id: str) -> ReminderResult:
        """Cancel a pending reminder and invalidate its task generation."""
        user_id = str(user_id)
        previous = self._states.get(user_id)
        task = self._tasks.pop(user_id, None)
        if task is not None and not getattr(task, "done", lambda: False)():
            cancel = getattr(task, "cancel", None)
            if callable(cancel):
                cancel()
        if previous is None:
            return ReminderResult("not_found")
        generation = self._generation(user_id)
        state = replace(
            previous,
            pending=False,
            reminded=False,
            generation=generation,
            outcome="cancelled",
        )
        self._states[user_id] = state
        return ReminderResult("cancelled", state)

    async def consume(
        self,
        user_id: str,
        *,
        generation: int | None = None,
        status_reader: StatusReader,
        notifier: Notifier,
        context: Any = None,
    ) -> ReminderResult:
        """Consume one pending reminder after checking the current work status."""
        user_id = str(user_id)
        state = self._states.get(user_id)
        if state is None or not state.pending:
            return ReminderResult("not_pending", state)
        if generation is not None and state.generation != generation:
            return ReminderResult("stale", state)

        try:
            current_status = _status_value(await _maybe_await(status_reader(user_id)))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failed = replace(state, outcome="failed")
            self._states[user_id] = failed
            return ReminderResult("failed", failed, error=str(exc))

        if current_status != 3:
            skipped = replace(
                state,
                pending=False,
                reminded=False,
                outcome="skipped",
            )
            self._states[user_id] = skipped
            return ReminderResult("skipped", skipped)

        now = self.clock.now()
        elapsed_minutes = (now - state.refresh_time).total_seconds() / 60
        remaining_minutes = max(int(self.expire_minutes - elapsed_minutes), 0)
        notification = ReminderNotification(
            user_id=user_id,
            refresh_time=state.refresh_time,
            now=now,
            remaining_minutes=remaining_minutes,
            context=context,
        )
        try:
            await _maybe_await(notifier(notification))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failed = replace(state, outcome="failed")
            self._states[user_id] = failed
            return ReminderResult("failed", failed, notification, str(exc))

        consumed = replace(
            state,
            pending=False,
            reminded=True,
            outcome="consumed",
        )
        self._states[user_id] = consumed
        return ReminderResult("consumed", consumed, notification)

    async def _run(
        self,
        user_id: str,
        generation: int,
        *,
        status_reader: StatusReader,
        notifier: Notifier,
        context: Any,
    ) -> ReminderResult:
        try:
            await self._sleep(self.delay_seconds)
            return await self.consume(
                user_id,
                generation=generation,
                status_reader=status_reader,
                notifier=notifier,
                context=context,
            )
        except asyncio.CancelledError:
            # Direct task cancellation is treated like owner cancellation, but
            # never touches a newer generation scheduled for the same user.
            state = self._states.get(user_id)
            if state is not None and state.generation == generation and state.pending:
                self._states[user_id] = replace(
                    state, pending=False, reminded=False, outcome="cancelled"
                )
            raise
        finally:
            if self._states.get(user_id) is not None and self._states[user_id].generation == generation:
                self._tasks.pop(user_id, None)

    def shutdown(self) -> None:
        """Cancel all owned tasks; useful for plugin/application teardown."""
        for user_id in tuple(self._tasks):
            self.cancel(user_id)


__all__ = [
    "ReminderNotification",
    "ReminderResult",
    "ReminderState",
    "WorkReminderScheduler",
]
