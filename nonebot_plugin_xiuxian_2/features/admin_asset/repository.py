from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class AdminStoneRepository(Protocol):
    def adjust(self, operation_id: str, operator_id: str, user_id: str, expected_stone: int,
               requested_delta: int, *, target_name: str = "") -> Any: ...


class AdminItemRepository(Protocol):
    def grant(self, operation_id: str, operator_id: str, user_id: str, item_id: int,
              item_name: str, item_type: str, quantity: int, expected_quantity: int,
              max_goods_num: int, *, target_name: str = "") -> Any: ...


class LegacyAdminStoneRepository:
    """Lazy adapter for the existing atomic administrator economy service."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def adjust(self, operation_id: str, operator_id: str, user_id: str, expected_stone: int,
               requested_delta: int, *, target_name: str = "") -> Any:
        try:
            from ...xiuxian.xiuxian_admin.transaction_service import AdminStoneAdjustmentService
        except (ImportError, RuntimeError, ValueError):
            return {"status": "not_ready"}
        return AdminStoneAdjustmentService(self.database).adjust(
            operation_id,
            operator_id,
            user_id,
            expected_stone,
            requested_delta,
            target_name=target_name,
        )


class LegacyAdminItemRepository:
    """Lazy adapter for one ordinary item grant."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def grant(self, operation_id: str, operator_id: str, user_id: str, item_id: int,
              item_name: str, item_type: str, quantity: int, expected_quantity: int,
              max_goods_num: int, *, target_name: str = "") -> Any:
        try:
            from ...xiuxian.xiuxian_admin.transaction_service import AdminItemGrantService
        except (ImportError, RuntimeError, ValueError):
            return {"status": "not_ready"}
        return AdminItemGrantService(self.database).grant(
            operation_id, operator_id, user_id, item_id, item_name, item_type,
            quantity, expected_quantity, max_goods_num, target_name=target_name,
        )


__all__ = ["AdminItemRepository", "AdminStoneRepository", "LegacyAdminItemRepository", "LegacyAdminStoneRepository"]
