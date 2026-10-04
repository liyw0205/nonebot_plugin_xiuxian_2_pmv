from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping


@dataclass(frozen=True)
class EntertainmentRequest:
    operation_id: str
    user_id: str
    payload: Mapping[str, Any]

    def validate(self) -> None:
        if not self.operation_id.strip() or not self.user_id.strip():
            raise ValueError("operation_id and user_id are required")


@dataclass(frozen=True)
class NewApiAccountSummary:
    api_user_id: str
    mode: Literal["token", "cookie"]
    base_url: str
    label: str
    auto_checkin: bool


@dataclass(frozen=True)
class NewApiAccountListResult:
    status: Literal["ok", "missing", "invalid", "too_large", "too_many", "unavailable"]
    accounts: tuple[NewApiAccountSummary, ...] = ()


__all__ = ["EntertainmentRequest", "NewApiAccountListResult", "NewApiAccountSummary"]
