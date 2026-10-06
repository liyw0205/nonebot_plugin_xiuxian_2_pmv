from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from threading import RLock
from typing import Any


_MISSING = object()


class AdminImpersonationRepository(MutableMapping[str, str]):
    """One process-local identity owner with a legacy mapping interface."""

    def __init__(self) -> None:
        self._targets: dict[str, str] = {}
        self._lock = RLock()

    @staticmethod
    def _validate_id(value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("impersonation identity must be a nonempty string")
        return value

    def get(self, admin_id: str, default: Any = None) -> Any:
        with self._lock:
            return self._targets.get(admin_id, default)

    def set(self, admin_id: str, target_id: str) -> str | None:
        admin_id = self._validate_id(admin_id)
        target_id = self._validate_id(target_id)
        with self._lock:
            previous = self._targets.get(admin_id)
            self._targets[admin_id] = target_id
            return previous

    def cancel(self, admin_id: str) -> str | None:
        with self._lock:
            return self._targets.pop(admin_id, None)

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return dict(self._targets)

    def copy(self) -> dict[str, str]:
        return self.snapshot()

    def __getitem__(self, key: str) -> str:
        with self._lock:
            return self._targets[key]

    def __setitem__(self, key: str, value: str) -> None:
        self.set(key, value)

    def __delitem__(self, key: str) -> None:
        with self._lock:
            del self._targets[key]

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(tuple(self._targets))

    def __len__(self) -> int:
        with self._lock:
            return len(self._targets)

    def pop(self, key: str, default: Any = _MISSING) -> Any:
        with self._lock:
            if default is _MISSING:
                return self._targets.pop(key)
            return self._targets.pop(key, default)

    def setdefault(self, key: str, default: str | None = None) -> str:
        with self._lock:
            if key in self._targets:
                return self._targets[key]
            if default is None:
                raise ValueError("impersonation identity must be a nonempty string")
            self.set(key, default)
            return self._targets[key]

    def clear(self) -> None:
        with self._lock:
            self._targets.clear()

    def items(self):
        return self.snapshot().items()

    def values(self):
        return self.snapshot().values()


default_impersonation_repository = AdminImpersonationRepository()


__all__ = ["AdminImpersonationRepository", "default_impersonation_repository"]
