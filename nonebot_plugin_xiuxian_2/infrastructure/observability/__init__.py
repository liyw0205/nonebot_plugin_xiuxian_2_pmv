from .metrics import Metrics
from .context import current_context, elapsed_ms, redact_scope, trace_context
from .events import emit, observation_fields

__all__ = [
    "AuditLogger",
    "Metrics",
    "current_context",
    "elapsed_ms",
    "emit",
    "observation_fields",
    "redact_scope",
    "trace_context",
]


def __getattr__(name):
    if name == "AuditLogger":
        from .audit import AuditLogger

        return AuditLogger
    raise AttributeError(name)
