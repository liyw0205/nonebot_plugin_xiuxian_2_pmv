from __future__ import annotations

import secrets
from functools import wraps
from typing import Callable, Iterable


def csrf_token() -> str:
    from flask import session

    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def require_permission(permission: str, resolver: Callable[[str], bool] | None = None):
    from .api import api_error

    def decorator(function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            if resolver is not None and not resolver(permission):
                return api_error("forbidden", "权限不足", status=403)
            return function(*args, **kwargs)

        return wrapped

    return decorator


class HostPolicy:
    """Constant-time-ish allow-list for management-panel Host headers."""

    def __init__(self, allowed: Iterable[str] = ("127.0.0.1", "localhost", "::1")) -> None:
        self.allowed = frozenset(self._normalize(item) for item in allowed if self._normalize(item))

    @staticmethod
    def _normalize(host: str | None) -> str:
        """Normalize host headers without truncating IPv6 literals."""
        value = str(host or "").strip().casefold()
        if value.startswith("["):
            closing = value.find("]")
            if closing > 0:
                return value[1:closing]
        # Only a single colon can be a host:port separator.  An unbracketed
        # IPv6 literal contains multiple colons and must remain intact.
        if value.count(":") == 1:
            return value.split(":", 1)[0]
        return value

    def allows(self, host: str | None) -> bool:
        return self._normalize(host) in self.allowed


__all__ = ["HostPolicy", "csrf_token", "require_permission"]
