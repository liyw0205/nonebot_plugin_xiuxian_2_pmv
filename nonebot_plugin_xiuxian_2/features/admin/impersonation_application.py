from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from .impersonation_repository import (
    AdminImpersonationRepository,
    default_impersonation_repository,
)


class AdminImpersonationApplication:
    def __init__(self, repository: AdminImpersonationRepository | None = None) -> None:
        self.repository = (
            repository if repository is not None else default_impersonation_repository
        )

    @property
    def mapping(self) -> AdminImpersonationRepository:
        return self.repository

    def get_target(self, admin_id: str) -> str | None:
        return self.repository.get(admin_id)

    def set_target(self, admin_id: str, target_id: str) -> str | None:
        return self.repository.set(admin_id, target_id)

    def cancel(self, admin_id: str) -> str | None:
        return self.repository.cancel(admin_id)

    @staticmethod
    def resolve_target(
        text: str,
        *,
        mentioned_id: str | None = None,
        by_id: Callable[[str], Mapping[str, Any] | None],
        by_name: Callable[[str], Mapping[str, Any] | None],
    ) -> tuple[str | None, Mapping[str, Any] | None]:
        if mentioned_id:
            target_id = str(mentioned_id)
            return target_id, by_id(target_id)
        text = str(text or "").strip()
        if not text:
            return None, None
        profile = by_name(text)
        if profile:
            return str(profile["user_id"]), profile
        return text, by_id(text)


__all__ = ["AdminImpersonationApplication"]
