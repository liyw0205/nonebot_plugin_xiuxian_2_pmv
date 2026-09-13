"""Explicit lifecycle bridge for legacy feature modules.

Legacy modules are imported for compatibility and therefore cannot own a
NoneBot driver hook anymore.  They register callbacks here; the composition
root invokes the bridge from the refactored lifecycle in a deterministic,
idempotent order.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any


Callback = Callable[[], Any | Awaitable[Any]]
_startup: list[Callback] = []
_shutdown: list[Callback] = []
_started = False


def _register(target: list[Callback], callback: Callback) -> Callback:
    key = (getattr(callback, "__module__", ""), getattr(callback, "__qualname__", repr(callback)))
    if not any(
        (getattr(item, "__module__", ""), getattr(item, "__qualname__", repr(item))) == key
        for item in target
    ):
        target.append(callback)
    return callback


def register_legacy_startup(callback: Callback) -> Callback:
    return _register(_startup, callback)


def register_legacy_shutdown(callback: Callback) -> Callback:
    return _register(_shutdown, callback)


def startup_callbacks() -> tuple[Callback, ...]:
    return tuple(_startup)


def shutdown_callbacks() -> tuple[Callback, ...]:
    return tuple(_shutdown)


async def run_legacy_startup() -> None:
    global _started
    if _started:
        return
    _started = True
    try:
        for callback in startup_callbacks():
            result = callback()
            if inspect.isawaitable(result):
                await result
    except Exception:
        await run_legacy_shutdown()
        raise


async def run_legacy_shutdown() -> None:
    global _started
    if not _started:
        return
    for callback in reversed(shutdown_callbacks()):
        try:
            result = callback()
            if inspect.isawaitable(result):
                await result
        except Exception:
            # Shutdown is best effort; Lifecycle preserves the original
            # startup error and continues draining remaining resources.
            continue
    _started = False


def reset_for_test() -> None:
    """Clear callback state for isolated lifecycle tests."""
    global _started
    _startup.clear()
    _shutdown.clear()
    _started = False


__all__ = [
    "register_legacy_shutdown",
    "register_legacy_startup",
    "reset_for_test",
    "run_legacy_shutdown",
    "run_legacy_startup",
    "shutdown_callbacks",
    "startup_callbacks",
]
