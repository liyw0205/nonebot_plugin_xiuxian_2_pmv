from __future__ import annotations

from typing import Any

from .tasks import SignInTaskRepository


class ApplicationSignInTaskEffects:
    def __init__(self, repository: SignInTaskRepository, clock: Any) -> None:
        self.repository = repository
        self.clock = clock

    def record(self, *, user_id: str, operation_id: str, amount: int = 1) -> list[str]:
        if int(amount) != 1:
            raise ValueError("sign-in task amount must be one")
        now = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        return self.repository.record(user_id=str(user_id), operation_id=str(operation_id), occurred_at=now)


__all__ = ["ApplicationSignInTaskEffects"]
