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
        statistics_repository: Any | None = None,
        task_effects: Any | None = None,
    ) -> None:
        self.database = str(database)
        self.lottery_service = lottery_service
        self.clock = clock
        self.task_progress = task_progress
        self.statistics = statistics
        self.statistics_repository = statistics_repository
        self.task_effects = task_effects
        self.logger = logger

    def _user_name(self, user_id: str) -> str:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT user_name FROM user_xiuxian WHERE user_id = ? LIMIT 1", (str(user_id),))
        return str(row["user_name"] if row else user_id)

    def _claim_non_lottery_effects(self, operation_id: str) -> bool:
        """Persist the once-only claim before invoking legacy side effects."""
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE IF NOT EXISTS sign_in_effect_operations ("
                "operation_id TEXT PRIMARY KEY, created_at TEXT NOT NULL)"
            )
            cursor = uow.execute(
                "INSERT INTO sign_in_effect_operations(operation_id, created_at) "
                "VALUES (?, ?) ON CONFLICT(operation_id) DO NOTHING",
                (operation_id, self.clock.now().isoformat()),
            )
            return cursor.rowcount == 1

    def on_signed(self, *, user_id: str, operation_id: str, stone: int, replayed: bool) -> str | None:
        now = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        business_date = now.date().isoformat() if isinstance(now, datetime) else str(now)[:10]
        settled = self.lottery_service.settle(
            f"lottery:{operation_id}",
            str(user_id),
            self._user_name(str(user_id)),
            business_date,
            occurred_at=now,
        )
        if settled.status == "operation_conflict":
            return "鸿运结算记录冲突，请联系管理员处理。"
        if settled.status == "user_missing":
            return "未找到修仙存档，本次鸿运未结算。"
        if settled.status == "already_participated" and not settled.lottery_number:
            message = "本期鸿运已经参与，奖池继续累积~"
        elif settled.prize_tier == "grand":
            message = f"✨鸿运当头！恭喜道友获得特等奖！\n中奖号码：{settled.lottery_number}\n获得奖池中{settled.prize}灵石！🎉🎉🎉"
        else:
            prize_names = {"first": "一等奖", "second": "二等奖", "third": "三等奖"}
            if settled.prize_tier in prize_names:
                message = f"🎉恭喜道友获得{prize_names[settled.prize_tier]}！\n中奖号码：{settled.lottery_number}\n获得奖池的{settled.prize}灵石！🎉"
            else:
                message = "本次签到未中奖，奖池继续累积~"
        if replayed or not self._claim_non_lottery_effects(operation_id):
            return message + "\n该签到请求已经处理，无需重复提交。"
        if self.statistics_repository is not None:
            self.statistics_repository.record(
                user_id=str(user_id), operation_id=f"statistics:{operation_id}",
                event_key="修仙签到", occurred_at=now,
            )
        elif self.statistics is not None:
            self.statistics(str(user_id), "修仙签到")
        if self.task_effects is not None:
            self.task_effects.record(user_id=str(user_id), operation_id=operation_id)
        elif self.task_progress is not None:
            self.task_progress(str(user_id), "sign_in", operation_id=f"task-progress:{operation_id}")
        if self.logger is not None:
            self.logger(str(user_id), f"签到成功，获取{int(stone)}块灵石")
        return message


__all__ = ["LegacySignInEffects"]
