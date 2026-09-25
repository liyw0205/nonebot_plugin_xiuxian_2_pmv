from __future__ import annotations

from pathlib import Path

from .lottery_talisman_repository import LotteryReward, LotteryTalismanResult, LotteryTalismanSqlRepository


class LotteryTalismanApplication:
    """Feature-owned use case for consuming lottery talismans."""

    def __init__(self, database: str | Path, *, repository: LotteryTalismanSqlRepository | None = None) -> None:
        self.repository = repository or LotteryTalismanSqlRepository(database)

    def apply(self, operation_id: str, user_id: str, talisman_id: int, quantity: int, rewards, *, max_goods_num: int) -> LotteryTalismanResult:
        return self.repository.apply(operation_id, user_id, talisman_id, quantity, rewards, max_goods_num=max_goods_num)


__all__ = ["LotteryReward", "LotteryTalismanApplication", "LotteryTalismanResult"]
