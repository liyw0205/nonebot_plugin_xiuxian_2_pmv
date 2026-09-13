from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, Mapping, Protocol, TypeVar


T = TypeVar("T")


class _Clock(Protocol):
    def now(self) -> Any: ...


def utc_now(clock: _Clock | None = None) -> str:
    """Format an injected clock value for an operation result.

    Core result objects do not own a wall clock.  Callers that need a
    timestamp pass their RuntimeContext clock; persistence adapters fill a
    timestamp at commit time when a legacy caller omitted one.
    """
    if clock is None:
        return ""
    return clock.now().isoformat()


@dataclass(frozen=True)
class ReplyPlan:
    """A transport-neutral response returned by an application use case."""

    content: Any
    reference: bool = True
    revoke_after: float | None = None
    audit_timeout: float = 0


@dataclass(frozen=True)
class OperationOutcome(Generic[T]):
    """Serializable result for an idempotent business operation."""

    status: str
    operation_id: str
    action: str
    data: T | None = None
    code: str | None = None
    message: str = ""
    before: Mapping[str, Any] = field(default_factory=dict)
    after: Mapping[str, Any] = field(default_factory=dict)
    consumed: Mapping[str, Any] = field(default_factory=dict)
    granted: Mapping[str, Any] = field(default_factory=dict)
    audit_category: str = ""
    occurred_at: str = ""
    replayed: bool = False

    @property
    def ok(self) -> bool:
        return self.status in {"applied", "replayed"}

    @classmethod
    def applied(
        cls,
        operation_id: str,
        action: str,
        data: T | None = None,
        **kwargs: Any,
    ) -> "OperationOutcome[T]":
        clock = kwargs.pop("clock", None)
        kwargs.setdefault("occurred_at", utc_now(clock))
        return cls("applied", operation_id, action, data=data, **kwargs)

    @classmethod
    def rejected(
        cls,
        operation_id: str,
        action: str,
        message: str,
        *,
        code: str = "rejected",
        **kwargs: Any,
    ) -> "OperationOutcome[T]":
        clock = kwargs.pop("clock", None)
        kwargs.setdefault("occurred_at", utc_now(clock))
        return cls(
            "rejected", operation_id, action, code=code, message=message, **kwargs
        )

    @classmethod
    def failed(
        cls,
        operation_id: str,
        action: str,
        message: str,
        *,
        code: str = "internal_error",
        **kwargs: Any,
    ) -> "OperationOutcome[T]":
        clock = kwargs.pop("clock", None)
        kwargs.setdefault("occurred_at", utc_now(clock))
        return cls(
            "failed", operation_id, action, code=code, message=message, **kwargs
        )

    def replay(self) -> "OperationOutcome[T]":
        return OperationOutcome(
            # Replaying a rejected request must remain rejected; otherwise a
            # second call could turn a business refusal into a successful API
            # response merely because it used the same idempotency key.
            status="replayed" if self.status == "applied" else self.status,
            operation_id=self.operation_id,
            action=self.action,
            data=self.data,
            code=self.code,
            message=self.message,
            before=self.before,
            after=self.after,
            consumed=self.consumed,
            granted=self.granted,
            audit_category=self.audit_category,
            occurred_at=self.occurred_at,
            replayed=True,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "ok": self.ok,
            "operation_id": self.operation_id,
            "action": self.action,
            "data": self.data,
            "code": self.code,
            "message": self.message,
            "before": dict(self.before),
            "after": dict(self.after),
            "consumed": dict(self.consumed),
            "granted": dict(self.granted),
            "audit_category": self.audit_category,
            "occurred_at": self.occurred_at,
            "replayed": self.replayed,
        }


__all__ = ["OperationOutcome", "ReplyPlan", "utc_now"]
