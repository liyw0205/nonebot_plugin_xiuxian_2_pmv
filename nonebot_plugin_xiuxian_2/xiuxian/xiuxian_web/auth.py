from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from threading import RLock
from typing import Callable

from werkzeug.security import check_password_hash


SUPPORTED_PASSWORD_HASH_PREFIXES = ("scrypt:", "pbkdf2:")


def is_supported_password_hash(value: object) -> bool:
    password_hash = str(value or "").strip()
    return password_hash.startswith(SUPPORTED_PASSWORD_HASH_PREFIXES)


def verify_password_hash(password_hash: object, password: object) -> bool:
    encoded = str(password_hash or "").strip()
    candidate = str(password or "")
    if not is_supported_password_hash(encoded) or not candidate:
        return False
    try:
        return bool(check_password_hash(encoded, candidate))
    except (TypeError, ValueError):
        return False


@dataclass
class _AttemptState:
    failures: deque[float] = field(default_factory=deque)
    locked_until: float = 0.0


class LoginAttemptLimiter:
    """进程内登录失败限速；不记录提交的账号或密码内容。"""

    def __init__(
        self,
        *,
        max_attempts: int = 5,
        window_seconds: float = 300,
        lock_seconds: float = 900,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_attempts = max(1, int(max_attempts))
        self.window_seconds = max(1.0, float(window_seconds))
        self.lock_seconds = max(1.0, float(lock_seconds))
        self._clock = clock
        self._states: dict[str, _AttemptState] = {}
        self._lock = RLock()

    def is_blocked(self, key: str) -> bool:
        now = self._clock()
        with self._lock:
            state = self._states.get(str(key))
            if state is None:
                return False
            self._prune(state, now)
            if state.locked_until > now:
                return True
            if not state.failures:
                self._states.pop(str(key), None)
            return False

    def record_failure(self, key: str) -> bool:
        now = self._clock()
        normalized = str(key)
        with self._lock:
            state = self._states.setdefault(normalized, _AttemptState())
            self._prune(state, now)
            if state.locked_until > now:
                return True
            state.failures.append(now)
            if len(state.failures) >= self.max_attempts:
                state.failures.clear()
                state.locked_until = now + self.lock_seconds
                return True
            return False

    def record_success(self, key: str) -> None:
        with self._lock:
            self._states.pop(str(key), None)

    def clear(self) -> None:
        with self._lock:
            self._states.clear()

    def _prune(self, state: _AttemptState, now: float) -> None:
        cutoff = now - self.window_seconds
        while state.failures and state.failures[0] <= cutoff:
            state.failures.popleft()
        if state.locked_until <= now:
            state.locked_until = 0.0


__all__ = [
    "LoginAttemptLimiter",
    "is_supported_password_hash",
    "verify_password_hash",
]
