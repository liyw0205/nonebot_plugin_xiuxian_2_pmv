from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol


class WorkClaimRepository(Protocol):
    def claim(
        self,
        operation_id: str,
        user_id: str,
        expected_count: int,
        expected_offer: Mapping[str, Any],
        task_index: int,
        started_at: str,
    ) -> Any: ...


class WorkSettlementRepository(Protocol):
    def settle(
        self,
        operation_id: str,
        user_id: str,
        expected_work: Mapping[str, Any],
        exp_gain: int,
        item: Mapping[str, Any] | None,
        max_exp: int,
        max_goods_num: int,
        *,
        success_kind: str = "",
        item_msg: str = "",
    ) -> Any: ...


class LegacyWorkClaimRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def claim(
        self,
        operation_id: str,
        user_id: str,
        expected_count: int,
        expected_offer: Mapping[str, Any],
        task_index: int,
        started_at: str,
    ) -> Any:
        from ...xiuxian.xiuxian_work.transaction_service import WorkClaimService

        return WorkClaimService(self.database).claim(
            operation_id, user_id, expected_count, expected_offer, task_index, started_at
        )


class LegacyWorkSettlementRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def settle(
        self,
        operation_id: str,
        user_id: str,
        expected_work: Mapping[str, Any],
        exp_gain: int,
        item: Mapping[str, Any] | None,
        max_exp: int,
        max_goods_num: int,
        *,
        success_kind: str = "",
        item_msg: str = "",
    ) -> Any:
        from ...xiuxian.xiuxian_work.transaction_service import WorkSettlementService

        return WorkSettlementService(self.database).settle(
            operation_id,
            user_id,
            expected_work,
            exp_gain,
            item,
            max_exp,
            max_goods_num,
            success_kind=success_kind,
            item_msg=item_msg,
        )


__all__ = [
    "LegacyWorkClaimRepository",
    "LegacyWorkSettlementRepository",
    "WorkClaimRepository",
    "WorkSettlementRepository",
]
