from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from ...infrastructure.clock import SystemClock


_current_clock: ContextVar[Any] = ContextVar("bank_clock", default=None)


def set_bank_clock(clock: Any) -> Any:
    return _current_clock.set(clock)


def reset_bank_clock(token: Any) -> None:
    _current_clock.reset(token)


def bank_clock() -> Any:
    return _current_clock.get() or SystemClock()


__all__ = ["bank_clock", "reset_bank_clock", "set_bank_clock"]
