from __future__ import annotations

from functools import wraps
from typing import Any, Callable

from ..api import _require_csrf, api_error
from ....core.errors import DomainError


def guard(permission: str, resolver: Callable[[str], bool], *, write: bool = False):
    """Apply the same permission, CSRF and error contract to every endpoint."""

    def decorator(function):
        @wraps(function)
        def wrapped(*args: Any, **kwargs: Any):
            if not resolver(permission):
                return api_error("forbidden", "权限不足", status=403)
            if write:
                failed = _require_csrf()
                if failed is not None:
                    return failed
            try:
                return function(*args, **kwargs)
            except DomainError as exc:
                status = 403 if exc.code == "forbidden" else 404 if exc.code == "not_found" else 409 if exc.code == "conflict" else 400
                return api_error(exc.code, exc.message, details=exc.details, status=status)
            except (TypeError, ValueError) as exc:
                # Missing or malformed DTO fields must be a client error.  In
                # particular, calling an application method with an incomplete
                # payload raises TypeError before its domain validator runs.
                return api_error("validation_error", str(exc), status=400)
            except Exception:
                return api_error("internal_error", "服务暂时不可用", status=500)

        return wrapped

    return decorator


__all__ = ["guard"]
