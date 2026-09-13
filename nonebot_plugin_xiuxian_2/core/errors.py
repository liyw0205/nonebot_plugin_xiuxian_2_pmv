from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DomainError(Exception):
    """Expected business failure which must not mutate assets."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)


class ValidationError(DomainError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__("validation_error", message, details)


class ConflictError(DomainError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__("conflict", message, details)


class ForbiddenError(DomainError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__("forbidden", message, details)


class NotFoundError(DomainError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__("not_found", message, details)


class OperationConflictError(ConflictError):
    """The same operation key was reused with a different request payload."""

    def __init__(self, operation_id: str, action: str) -> None:
        super().__init__(
            "操作号已用于其他请求",
            operation_id=operation_id,
            action=action,
        )


__all__ = [
    "ConflictError",
    "DomainError",
    "ForbiddenError",
    "NotFoundError",
    "OperationConflictError",
    "ValidationError",
]
