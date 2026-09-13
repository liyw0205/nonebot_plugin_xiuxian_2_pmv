from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .effects import SignInEffects


class SignInApplicationEffects(SignInEffects):
    """Post-commit effects owned by the sign-in feature."""

    def __init__(self, database: str | Path, *, lottery: Any, clock: Any, statistics: Any, tasks: Any) -> None:
        self.database = str(database)
        self.lottery = lottery
        self.clock = clock
        self.statistics = statistics
        self.tasks = tasks

    def _user_name(self, user_id: str) -> str:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT user_name FROM user_xiuxian WHERE user_id=? LIMIT 1", (str(user_id),))
        return str(row["user_name"] if row else user_id)

    def on_signed(self, *, user_id: str, operation_id: str, stone: int, replayed: bool) -> str | None:
        now = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        if not isinstance(now, datetime):
            raise TypeError("clock must return datetime")
        settled = self.lottery.settle(
            operation_id=f"lottery:{operation_id}",
            user_id=str(user_id),
            user_name=self._user_name(str(user_id)),
            business_date=now.date().isoformat(),
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
        if replayed:
            return message + "\n该签到请求已经处理，无需重复提交。"
        self.statistics.record(user_id=str(user_id), operation_id=f"statistics:{operation_id}", event_key="修仙签到", occurred_at=now)
        self.tasks.record(user_id=str(user_id), operation_id=operation_id)
        return message


__all__ = ["SignInApplicationEffects"]
