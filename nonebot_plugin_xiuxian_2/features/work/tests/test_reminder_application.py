from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone

from ..reminder_application import WorkReminderApplication
from ..reminder_scheduler import WorkReminderScheduler


class FixedClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.current

    def advance(self, **kwargs: int) -> None:
        self.current += timedelta(**kwargs)


class WorkReminderApplicationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.clock = FixedClock()
        self.sent = []
        self.status = 3
        self.application = WorkReminderApplication(
            delay_seconds=0,
            expire_minutes=30,
            clock=self.clock,
        )

    async def asyncTearDown(self) -> None:
        self.application.shutdown()
        await asyncio.sleep(0)

    async def test_schedule_and_consume_status_three_with_injected_clock(self) -> None:
        async def notify(notification):
            self.sent.append(notification)

        result = self.application.schedule(
            "u",
            status=3,
            status_reader=lambda _user_id: (self.status, {"status": 1}),
            notifier=notify,
            context=("bot", "event"),
        )
        self.assertEqual(result.status, "scheduled")
        self.assertTrue(result.state.pending)

        self.clock.advance(minutes=3, seconds=30)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        state = self.application.state("u")
        self.assertIsNotNone(state)
        self.assertFalse(state.pending)
        self.assertTrue(state.reminded)
        self.assertEqual(state.outcome, "consumed")
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0].remaining_minutes, 26)
        self.assertEqual(self.sent[0].context, ("bot", "event"))

    async def test_status_change_to_claimed_skips_without_sending(self) -> None:
        async def notify(notification):
            self.sent.append(notification)

        self.application.schedule(
            "u",
            status=3,
            status_reader=lambda _user_id: 2,
            notifier=notify,
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        state = self.application.state("u")
        self.assertFalse(state.pending)
        self.assertFalse(state.reminded)
        self.assertEqual(state.outcome, "skipped")
        self.assertEqual(self.sent, [])

    async def test_non_three_schedule_cancels_existing_task_and_does_not_schedule(self) -> None:
        gate = asyncio.Event()

        async def wait_forever(_delay):
            await gate.wait()

        scheduler = WorkReminderScheduler(
            delay_seconds=180,
            clock=self.clock,
            sleep=wait_forever,
        )
        application = WorkReminderApplication(scheduler)
        application.schedule(
            "u",
            status=3,
            status_reader=lambda _user_id: 3,
            notifier=lambda _notification: None,
        )
        ignored = application.schedule(
            "u",
            status=2,
            status_reader=lambda _user_id: 2,
            notifier=lambda _notification: None,
        )
        self.assertEqual(ignored.status, "ignored")
        self.assertEqual(ignored.state.outcome, "cancelled")
        self.assertFalse(ignored.state.pending)
        task = scheduler.task("u")
        self.assertIsNone(task)
        application.shutdown()
        await asyncio.sleep(0)

    async def test_cancel_marks_state_and_cancels_task(self) -> None:
        gate = asyncio.Event()

        async def wait_forever(_delay):
            await gate.wait()

        scheduler = WorkReminderScheduler(
            delay_seconds=180,
            clock=self.clock,
            sleep=wait_forever,
        )
        application = WorkReminderApplication(scheduler)
        application.schedule(
            "u",
            status=3,
            status_reader=lambda _user_id: 3,
            notifier=lambda _notification: None,
        )
        await asyncio.sleep(0)
        task = scheduler.task("u")
        cancelled = application.cancel("u")
        self.assertEqual(cancelled.status, "cancelled")
        self.assertFalse(cancelled.state.pending)
        self.assertFalse(cancelled.state.reminded)
        self.assertEqual(cancelled.state.outcome, "cancelled")
        self.assertIsNone(scheduler.task("u"))
        await asyncio.gather(task, return_exceptions=True)

    async def test_send_failure_keeps_pending_for_retry_and_reports_failure(self) -> None:
        attempts = 0

        async def failing_notify(_notification):
            nonlocal attempts
            attempts += 1
            raise RuntimeError("transport unavailable")

        self.application.schedule(
            "u",
            status=3,
            status_reader=lambda _user_id: 3,
            notifier=failing_notify,
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        state = self.application.state("u")
        self.assertTrue(state.pending)
        self.assertFalse(state.reminded)
        self.assertEqual(state.outcome, "failed")
        self.assertEqual(attempts, 1)

        async def successful_notify(notification):
            self.sent.append(notification)

        retried = await self.application.consume(
            "u",
            status_reader=lambda _user_id: 3,
            notifier=successful_notify,
        )
        self.assertEqual(retried.status, "consumed")
        self.assertFalse(self.application.state("u").pending)
        self.assertTrue(self.application.state("u").reminded)

    async def test_direct_task_cancellation_marks_current_generation_cancelled(self) -> None:
        gate = asyncio.Event()

        async def wait_forever(_delay):
            await gate.wait()

        scheduler = WorkReminderScheduler(
            delay_seconds=180,
            clock=self.clock,
            sleep=wait_forever,
        )
        scheduler.schedule(
            "u",
            status=3,
            status_reader=lambda _user_id: 3,
            notifier=lambda _notification: None,
        )
        await asyncio.sleep(0)
        task = scheduler.task("u")
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        state = scheduler.state("u")
        self.assertFalse(state.pending)
        self.assertEqual(state.outcome, "cancelled")


if __name__ == "__main__":
    unittest.main()
