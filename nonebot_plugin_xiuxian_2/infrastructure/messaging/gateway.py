from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from typing import Any, Awaitable, Callable

from ...core.result import ReplyPlan
from ..observability import current_context


@dataclass(frozen=True)
class DeliveryOutcome:
    ok: bool
    message_id: str | None = None
    error_code: str | None = None
    elapsed_ms: int = 0
    request_id: str = ""
    operation_id: str = ""
    job_id: str = ""
    user_scope: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "message_id": self.message_id,
            "error_code": self.error_code,
            "elapsed_ms": self.elapsed_ms,
            "request_id": self.request_id,
            "operation_id": self.operation_id,
            "job_id": self.job_id,
            "user_scope": self.user_scope,
        }


class MessageGatewayAdapter:
    """Transport-neutral facade around the existing delivery implementation."""

    def __init__(self, sender: Callable[[Any, ReplyPlan], Any | Awaitable[Any]] | None = None) -> None:
        self.sender = sender

    async def send(self, context: Any, reply: ReplyPlan) -> DeliveryOutcome:
        trace = current_context()
        trace.update(
            {
                key: str(getattr(context, key, "") or value)
                for key, value in trace.items()
            }
        )
        if self.sender is None:
            return DeliveryOutcome(False, error_code="gateway_unavailable", **trace)
        started = monotonic()
        try:
            result = self.sender(context, reply)
            if hasattr(result, "__await__"):
                result = await result
            message_id = getattr(result, "message_id", None) or (str(result) if result is not None else None)
            return DeliveryOutcome(True, message_id=message_id, elapsed_ms=int((monotonic() - started) * 1000), **trace)
        except Exception as exc:
            return DeliveryOutcome(False, error_code=type(exc).__name__, elapsed_ms=int((monotonic() - started) * 1000), **trace)


__all__ = ["DeliveryOutcome", "MessageGatewayAdapter"]
