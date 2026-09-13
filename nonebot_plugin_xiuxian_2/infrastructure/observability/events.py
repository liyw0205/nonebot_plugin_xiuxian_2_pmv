"""Small structured-event bridge shared by adapters and infrastructure.

The gameplay layer does not need to know which logger NoneBot uses.  Events
are emitted with the same correlation fields as the delivery and audit
records, while the fallback keeps CLI and test environments dependency-free.
"""

from __future__ import annotations

import logging
from typing import Any

from .context import current_context, elapsed_ms, redact_scope


def observation_fields(
    *,
    request_id: str | None = None,
    operation_id: str | None = None,
    job_id: str | None = None,
    user_scope: str | None = None,
    duration_ms: int | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build a bounded, consistently named structured-event payload."""
    fields: dict[str, Any] = current_context()
    overrides = {
        "request_id": request_id,
        "operation_id": operation_id,
        "job_id": job_id,
    }
    for name, value in overrides.items():
        if value is not None:
            fields[name] = str(value)[:128]
    if user_scope is not None:
        fields["user_scope"] = redact_scope(user_scope)
    fields["duration_ms"] = elapsed_ms() if duration_ms is None else max(0, int(duration_ms))
    fields.update(extra)
    return fields


def emit(level: str, message: str, *, logger: Any | None = None, **fields: Any) -> dict[str, Any]:
    """Emit one structured event and return the fields for deterministic tests."""
    payload = observation_fields(**fields)
    target = logger
    if target is None:
        try:
            from nonebot.log import logger as target
        except Exception:
            target = None
    try:
        if target is not None:
            bound = target.bind(**payload) if hasattr(target, "bind") else target
            method = getattr(bound, str(level).casefold(), None)
            if callable(method):
                method(message)
            elif hasattr(bound, "log"):
                bound.log(str(level).upper(), message)
            return payload
    except Exception:
        # Observability must not turn a successful operation into a failure.
        pass
    logging.getLogger("nonebot_plugin_xiuxian_2").log(
        getattr(logging, str(level).upper(), logging.INFO),
        "%s %s",
        message,
        payload,
        extra=payload,
    )
    return payload


__all__ = ["emit", "observation_fields"]
