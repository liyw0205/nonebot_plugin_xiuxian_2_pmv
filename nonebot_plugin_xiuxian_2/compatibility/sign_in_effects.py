"""Compatibility boundary for sign-in post-commit side effects.

The sign-in asset mutation stays in ``SignInApplication``.  This adapter keeps
legacy lottery/task behavior at an explicit boundary while those side effects
are migrated into their own applications.  It must not be imported by domain
or feature code directly.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from ..features.sign_in.effects import SignInEffects
from ..infrastructure.database import DatabaseUnitOfWork


class LegacySignInEffects(SignInEffects):
    """Preserve legacy lottery/task behavior after a committed sign-in."""

    def __init__(
        self,
        database: str | Path,
        *,
        lottery_service: Any,
        clock: Any,
        task_progress: Callable[..., Any] | None = None,
        statistics: Callable[..., Any] | None = None,
        logger: Callable[..., Any] | None = None,
    ) -> None:
        self.database = str(database)
        self.lottery_service = lottery_service
        self.clock = clock
        self.task_progress = task_progress
        self.statistics = statistics
        self.logger = logger

    def _user_name(self, user_id: str) -> str:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT user_name FROM user_xiuxian WHERE user_id = ? LIMIT 1", (str(user_id),))
        return str(row["user_name"] if row else user_id)

    def on_signed(self, *, user_id: str, operation_id: str, stone: int, replayed: bool) -> None:
        now = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        business_date = now.date().isoformat() if isinstance(now, datetime) else str(now)[:10]
        self.lottery_service.settle(
            f"lottery:{operation_id}",
            str(user_id),
            self._user_name(str(user_id)),
            business_date,
            occurred_at=now,
        )
        if replayed:
            return
        if self.statistics is not None:
            self.statistics(str(user_id), "修仙签到")
        if self.task_progress is not None:
            self.task_progress(str(user_id), "sign_in", operation_id=f"task-progress:{operation_id}")
        if self.logger is not None:
            self.logger(str(user_id), f"签到成功，获取{int(stone)}块灵石")


__all__ = ["LegacySignInEffects"]
