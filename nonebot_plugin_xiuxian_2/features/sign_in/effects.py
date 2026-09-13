from __future__ import annotations

from typing import Any, Protocol


class SignInEffects(Protocol):
    """Post-commit side effects owned by the sign-in application boundary."""

    def on_signed(
        self,
        *,
        user_id: str,
        operation_id: str,
        stone: int,
        replayed: bool,
    ) -> None:
        """Apply idempotent lottery/statistics/task effects for one operation."""


class NullSignInEffects:
    """Default no-op until deployment wiring supplies the real adapters."""

    def on_signed(self, *, user_id: str, operation_id: str, stone: int, replayed: bool) -> None:
        return None


__all__ = ["NullSignInEffects", "SignInEffects"]
