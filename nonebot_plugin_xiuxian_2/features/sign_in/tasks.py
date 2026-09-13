from __future__ import annotations

from typing import Any, Callable


class SignInTaskEffects:
    """Boundary adapter for task progress after a committed sign-in."""

    def __init__(self, record_progress: Callable[..., Any]) -> None:
        self.record_progress = record_progress

    def record(self, *, user_id: str, operation_id: str, amount: int = 1) -> list[str]:
        return list(
            self.record_progress(
                str(user_id),
                "sign_in",
                int(amount),
                operation_id=f"task-progress:{operation_id}",
            )
            or []
        )


__all__ = ["SignInTaskEffects"]
