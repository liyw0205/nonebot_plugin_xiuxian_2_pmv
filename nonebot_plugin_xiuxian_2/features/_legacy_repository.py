"""Lazy adapters for historical transaction service classes."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable


class LegacyRepository:
    def __init__(self, *databases: str | Path) -> None:
        self.databases = tuple(str(path) for path in databases)

    def _call(self, factory: Callable[..., Any], method: str, *args: Any, **kwargs: Any) -> Any:
        service = factory(*self.databases)
        return getattr(service, method)(*args, **kwargs)


__all__ = ["LegacyRepository"]
