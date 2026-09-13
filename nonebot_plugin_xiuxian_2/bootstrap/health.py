from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class HealthReport:
    ready: bool
    checks: dict[str, bool] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"ready": self.ready, "checks": dict(self.checks), "details": dict(self.details)}


class Readiness:
    """Small health registry used by CLI, Web and startup diagnostics."""

    def __init__(self) -> None:
        self._checks: dict[str, tuple[Callable[[], bool], str]] = {}
        self._forced: dict[str, bool] = {}

    def register(self, name: str, check: Callable[[], bool], *, detail: str = "") -> None:
        if not name.strip():
            raise ValueError("health check name is required")
        if name in self._checks:
            raise ValueError(f"duplicate health check: {name}")
        self._checks[name] = (check, detail)

    def set(self, name: str, value: bool) -> None:
        self._forced[name] = bool(value)

    def report(self) -> HealthReport:
        checks: dict[str, bool] = {}
        details: dict[str, Any] = {}
        for name, (check, detail) in self._checks.items():
            try:
                # Do not evaluate the live callback when an operator has
                # explicitly forced a state; ``dict.get`` evaluates its
                # default eagerly and would turn a forced healthy state into
                # an exception if the backing resource is already gone.
                checks[name] = self._forced[name] if name in self._forced else bool(check())
            except Exception as exc:
                checks[name] = False
                details[name] = f"{type(exc).__name__}: {exc}"
            if detail and name not in details:
                details[name] = detail
        return HealthReport(bool(checks) and all(checks.values()), checks, details)

    def is_ready(self) -> bool:
        return self.report().ready


__all__ = ["HealthReport", "Readiness"]
