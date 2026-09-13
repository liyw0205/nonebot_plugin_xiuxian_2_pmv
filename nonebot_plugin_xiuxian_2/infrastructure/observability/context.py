from __future__ import annotations

import contextvars
from contextlib import contextmanager
from time import monotonic
from typing import Any, Iterator

from ..ids import UUIDGenerator


_request_id = contextvars.ContextVar("request_id", default="")
_operation_id = contextvars.ContextVar("operation_id", default="")
_job_id = contextvars.ContextVar("job_id", default="")
_user_scope = contextvars.ContextVar("user_scope", default="")
_started_at = contextvars.ContextVar("observability_started_at", default=None)


def current_context() -> dict[str, str]:
    return {
        "request_id": _request_id.get(),
        "operation_id": _operation_id.get(),
        "job_id": _job_id.get(),
        "user_scope": _user_scope.get(),
    }


def elapsed_ms() -> int:
    """Return elapsed milliseconds for the active trace, or zero outside one."""
    started = _started_at.get()
    if started is None:
        return 0
    return max(0, int((monotonic() - started) * 1000))


@contextmanager
def trace_context(*, request_id: str | None = None, operation_id: str | None = None, job_id: str | None = None, user_scope: str | None = None, ids: Any | None = None) -> Iterator[dict[str, str]]:
    ids = ids or UUIDGenerator()
    tokens = []
    started_token = None
    if any(value is not None for value in (request_id, operation_id, job_id)):
        started_token = _started_at.set(monotonic())
    if request_id is not None:
        tokens.append((_request_id, _request_id.set(request_id or ids.new_id())))
    if operation_id is not None:
        tokens.append((_operation_id, _operation_id.set(operation_id)))
    if job_id is not None:
        tokens.append((_job_id, _job_id.set(job_id)))
    if user_scope is not None:
        tokens.append((_user_scope, _user_scope.set(_redact(user_scope))))
    try:
        yield current_context()
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)
        if started_token is not None:
            _started_at.reset(started_token)


def _redact(value: str) -> str:
    text = str(value)
    if len(text) <= 4:
        return "***"
    return f"{text[:2]}***{text[-2:]}"


def redact_scope(value: str | None) -> str:
    """Expose the shared user-scope redaction for structured adapters."""
    return _redact(str(value or "")) if value else ""


__all__ = ["current_context", "trace_context"]
