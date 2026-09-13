from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from ...infrastructure.clock import SystemClock


_current_clock: ContextVar[Any] = ContextVar("sign_in_clock", default=None)


def set_sign_in_clock(clock: Any) -> Any:
    return _current_clock.set(clock)


def reset_sign_in_clock(token: Any) -> None:
    _current_clock.reset(token)


def sign_in_clock() -> Any:
    return _current_clock.get() or SystemClock()


__all__ = ["reset_sign_in_clock", "set_sign_in_clock", "sign_in_clock"]
