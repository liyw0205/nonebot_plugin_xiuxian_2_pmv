"""Stable job handlers for auction settlement."""

from __future__ import annotations

from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.ids import UUIDGenerator


def settle(
    application: Any,
    *,
    operation_id: str | None = None,
    scheduled_at: str | None = None,
    fee_rate: float = 0.1,
    clock: Any | None = None,
    ids: Any | None = None,
) -> Any:
    # The scheduler's stable idempotency key is based on scheduled_at.  Use
    # the same value for the business operation so a process restart cannot
    # settle one slot twice.  Direct/manual callers still get a unique key.
    clock = clock or SystemClock()
    ids = ids or UUIDGenerator()
    stable_operation_id = operation_id or (
        f"auction-settle:{scheduled_at}" if scheduled_at else f"auction-settle:{ids.new_id()}"
    )
    return application.settle_active(
        operation_id=stable_operation_id,
        end_time=_end_time(scheduled_at, clock),
        fee_rate=fee_rate,
    )


def _end_time(scheduled_at: str | None, clock: Any) -> float:
    if scheduled_at:
        try:
            return float(scheduled_at)
        except (TypeError, ValueError):
            pass
    return float(clock.now().timestamp())


JOBS = ("auction.settle",)

__all__ = ["JOBS", "settle"]
