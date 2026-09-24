from __future__ import annotations

from pathlib import Path
from typing import Mapping

from ...core.errors import OperationConflictError
from ...infrastructure.database import DatabaseUnitOfWork


class AuctionSettlementStatisticsRepository:
    """Idempotently project legacy auction settlement counters."""

    _METRICS = frozenset({
        "拍卖成交次数", "拍卖消费灵石", "拍卖售出次数", "拍卖收入灵石",
        "拍卖手续费消耗", "拍卖流拍次数", "交易购买", "交易出售", "拍卖成交",
    })

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def record(
        self,
        *,
        event_id: str,
        user_id: str,
        increments: Mapping[str, int],
        occurred_at: str,
    ) -> bool:
        event_id = str(event_id)
        user_id = str(user_id)
        changed = False
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            for event_key, raw_increment in sorted(increments.items()):
                key = str(event_key)
                increment = int(raw_increment)
                if not increment:
                    continue
                if key not in self._METRICS:
                    raise ValueError(f"unsupported auction settlement statistic: {key}")
                old = uow.query_one(
                    "SELECT user_id,increment FROM auction_settlement_statistics_events "
                    "WHERE event_id=? AND event_key=?",
                    (event_id, key),
                )
                if old is not None:
                    if str(old["user_id"]) != user_id or int(old["increment"]) != increment:
                        raise OperationConflictError(event_id, "auction.settlement.statistics")
                    continue
                changed = True
                uow.execute(
                    "INSERT INTO auction_settlement_statistics_events "
                    "(event_id,event_key,user_id,increment,created_at) VALUES (?,?,?,?,?)",
                    (event_id, key, user_id, increment, str(occurred_at)),
                )
                uow.execute(
                    f'INSERT INTO statistics(user_id,"{key}") VALUES (?,?) '
                    f'ON CONFLICT(user_id) DO UPDATE SET "{key}"='
                    f'COALESCE(statistics."{key}",0)+excluded."{key}"',
                    (user_id, increment),
                )
        return changed


__all__ = ["AuctionSettlementStatisticsRepository"]
