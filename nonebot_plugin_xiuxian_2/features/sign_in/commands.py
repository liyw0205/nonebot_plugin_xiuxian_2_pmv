from __future__ import annotations

from typing import Any

from ...core.result import ReplyPlan
from .application import SignInApplication


def handle_sign_in(application: SignInApplication, *, user_id: str, operation_id: str, lower_limit: int, upper_limit: int) -> ReplyPlan:
    return application.reply(
        user_id=user_id,
        operation_id=operation_id,
        lower_limit=lower_limit,
        upper_limit=upper_limit,
    )


__all__ = ["handle_sign_in"]
