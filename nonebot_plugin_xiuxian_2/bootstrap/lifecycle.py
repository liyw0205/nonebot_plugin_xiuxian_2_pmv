from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable


class LifecyclePhase(str, Enum):
    NEW = "new"
    FILESYSTEM = "filesystem"
    DATABASE = "database"
    MIGRATIONS = "migrations"
    REPOSITORIES = "repositories"
    JOBS = "jobs"
    WEB = "web"
    READY = "ready"
    NOT_READY = "not_ready"
    STOPPED = "stopped"


@dataclass(frozen=True)
class LifecycleState:
    phase: LifecyclePhase
    started: tuple[str, ...] = ()
    error: str | None = None


Callback = Callable[[], Any | Awaitable[Any]]


class Lifecycle:
    """Idempotent startup/shutdown state machine.

    Callbacks are registered explicitly by the composition root.  Calling
    ``start`` or ``shutdown`` repeatedly never invokes a completed callback a
    second time.
    """

    ORDER = (
        LifecyclePhase.FILESYSTEM,
        LifecyclePhase.DATABASE,
        LifecyclePhase.MIGRATIONS,
        LifecyclePhase.REPOSITORIES,
        LifecyclePhase.JOBS,
        LifecyclePhase.WEB,
    )

    def __init__(self) -> None:
        self._callbacks: dict[LifecyclePhase, Callback] = {}
        self._shutdown_callbacks: dict[LifecyclePhase, Callback] = {}
        self._completed: list[LifecyclePhase] = []
        self._state = LifecycleState(LifecyclePhase.NEW)
        self._lock = asyncio.Lock()
        self._active_phase: LifecyclePhase | None = None

    @property
    def state(self) -> LifecycleState:
        return self._state

    def register(
        self,
        phase: LifecyclePhase,
        ensure: Callback,
        *,
        shutdown: Callback | None = None,
    ) -> None:
        if phase not in self.ORDER:
            raise ValueError(f"unsupported lifecycle phase: {phase}")
        if self._state.phase not in {LifecyclePhase.NEW, LifecyclePhase.NOT_READY, LifecyclePhase.STOPPED}:
            raise RuntimeError("lifecycle callbacks cannot be changed after startup")
        if phase in self._callbacks:
            raise ValueError(f"duplicate lifecycle phase: {phase.value}")
        self._callbacks[phase] = ensure
        if shutdown is not None:
            self._shutdown_callbacks[phase] = shutdown

    async def _invoke(self, callback: Callback) -> Any:
        result = callback()
        return await result if inspect.isawaitable(result) else result

    async def _start(self) -> LifecycleState:
        if self._state.phase == LifecyclePhase.READY:
            return self._state
        if self._state.phase == LifecyclePhase.STOPPED:
            # Resources were drained by shutdown; the same composition can be
            # started again during a test or a controlled hot reload.
            self._state = LifecycleState(LifecyclePhase.NEW)
        try:
            for phase in self.ORDER:
                if phase in self._completed:
                    continue
                self._active_phase = phase
                callback = self._callbacks.get(phase)
                if callback is not None:
                    await self._invoke(callback)
                self._completed.append(phase)
                self._active_phase = None
                self._state = LifecycleState(phase, tuple(item.value for item in self._completed))
            self._state = LifecycleState(LifecyclePhase.READY, tuple(item.value for item in self._completed))
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            completed = tuple(item.value for item in self._completed)
            failed_phase = self._active_phase
            self._active_phase = None
            if failed_phase is not None:
                callback = self._shutdown_callbacks.get(failed_phase)
                if callback is not None:
                    try:
                        await self._invoke(callback)
                    except Exception:
                        pass
            await self._shutdown()
            self._state = LifecycleState(LifecyclePhase.NOT_READY, completed, error)
        return self._state

    async def start(self) -> LifecycleState:
        async with self._lock:
            return await self._start()

    async def _shutdown(self) -> LifecycleState:
        for phase in reversed(self._completed):
            callback = self._shutdown_callbacks.get(phase)
            if callback is None:
                continue
            try:
                await self._invoke(callback)
            except Exception:
                # Shutdown is best effort; the original startup error is more useful.
                pass
        self._completed.clear()
        self._state = LifecycleState(LifecyclePhase.STOPPED)
        return self._state

    async def shutdown(self) -> LifecycleState:
        async with self._lock:
            return await self._shutdown()

    def export(self) -> dict[str, Any]:
        return {
            "phase": self._state.phase.value,
            "started": list(self._state.started),
            "error": self._state.error,
            "registered": [phase.value for phase in self.ORDER if phase in self._callbacks],
        }


__all__ = ["Lifecycle", "LifecyclePhase", "LifecycleState"]
