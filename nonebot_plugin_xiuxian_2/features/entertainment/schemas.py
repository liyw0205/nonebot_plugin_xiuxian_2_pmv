from __future__ import annotations

from dataclasses import dataclass, field
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


@dataclass(frozen=True)
class NewApiCheckinTarget:
    index: int
    api_user_id: str
    mode: str
    secret: str = field(repr=False)
    base_url: str


@dataclass(frozen=True)
class NewApiCheckinTargetsResult:
    status: Literal[
        "ok",
        "empty",
        "missing",
        "invalid",
        "too_large",
        "too_many",
        "unavailable",
        "invalid_selector",
    ]
    targets: tuple[NewApiCheckinTarget, ...] = ()
    message: str = ""


@dataclass(frozen=True)
class WebDavBinding:
    index: int
    label: str
    dav_url: str
    username: str
    password: str = field(repr=False)


@dataclass(frozen=True)
class WebDavEntry:
    href: str
    name: str
    is_dir: bool
    size: str
    modified: str
    content_type: str


@dataclass(frozen=True)
class WebDavQueryResult:
    binding: WebDavBinding
    path: str
    entries: tuple[WebDavEntry, ...]


__all__ = [
    "EntertainmentRequest",
    "NewApiAccountListResult",
    "NewApiAccountSummary",
    "NewApiCheckinTarget",
    "NewApiCheckinTargetsResult",
    "WebDavBinding",
    "WebDavEntry",
    "WebDavQueryResult",
]
