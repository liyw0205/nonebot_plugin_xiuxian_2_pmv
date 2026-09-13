"""Compatibility facade for the historical ``SignInService`` contract.

The message handler still owns presentation and secondary side effects, but
the player mutation is delegated to the refactored application service.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Callable
import warnings

from ..core.errors import DomainError
from ..features.sign_in.application import SignInApplication


@dataclass(frozen=True)
class SignInResult:
    status: str
    user_id: str
    stone: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "signed"

    @property
    def succeeded(self) -> bool:
        return self.status in {"signed", "duplicate"}


class SignInService:
    """Preserve the old synchronous facade while using the new use case."""

    def __init__(
        self,
        database: str | Path,
        *,
        randint: Callable[[int, int], int] | None = None,
    ) -> None:
        random_source: Any | None = None
        if randint is not None:
            class _Random:
                @staticmethod
                def randint(lower: int, upper: int) -> int:
                    return randint(lower, upper)

            random_source = _Random()
        self.application = SignInApplication(str(database), random_source=random_source)
        enabled = os.environ.get("XIUXIAN_SIGN_IN_ENABLED", "true").strip().lower()
        self._enabled = enabled not in {"0", "false", "no", "off"}
        self._legacy = None
        if not self._enabled:
            from ..xiuxian.xiuxian_base.transaction_service import SignInService as LegacySignInService

            self._legacy = LegacySignInService(database, randint=randint)

    def get_result(self, operation_id: str) -> SignInResult | None:
        warnings.warn(
            "SignInService is a compatibility facade; use SignInApplication",
            DeprecationWarning,
            stacklevel=2,
        )
        from .commands import record_compatibility_hit

        record_compatibility_hit("sign_in")
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        if self._legacy is not None:
            return self._legacy.get_result(operation_id)
        record = self.application.lookup(operation_id)
        if record is None:
            return None
        return SignInResult("duplicate", record.user_id, record.stone)

    def sign(self, operation_id: str, user_id: str, stone_lower: int, stone_upper: int) -> SignInResult:
        warnings.warn(
            "SignInService is a compatibility facade; use SignInApplication",
            DeprecationWarning,
            stacklevel=2,
        )
        from .commands import record_compatibility_hit

        record_compatibility_hit("sign_in")
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if self._legacy is not None:
            return self._legacy.sign(operation_id, user_id, stone_lower, stone_upper)
        try:
            outcome = self.application.claim(
                user_id=user_id,
                operation_id=operation_id,
                lower_limit=int(stone_lower),
                upper_limit=int(stone_upper),
            )
        except DomainError as exc:
            if exc.code == "validation_error":
                raise ValueError(exc.message) from exc
            raise
        if outcome.status in {"applied", "replayed"}:
            data = outcome.data or {}
            record = data.get("sign_in", {}) if isinstance(data, dict) else {}
            return SignInResult(
                "duplicate" if outcome.status == "replayed" else "signed",
                str(record.get("user_id", user_id)),
                int(record.get("stone", outcome.granted.get("stone", 0)) or 0),
            )
        return SignInResult(outcome.code or outcome.status, user_id)


__all__ = ["SignInResult", "SignInService"]
