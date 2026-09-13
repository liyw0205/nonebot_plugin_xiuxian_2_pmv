"""Compatibility facades for dungeon purchase and exploration operations."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

from ..features.dungeon.application import DungeonApplication


def _warn(name: str) -> None:
    warnings.warn(f"{name} is a compatibility facade; use DungeonApplication", DeprecationWarning, stacklevel=3)
    from .commands import record_compatibility_hit

    record_compatibility_hit("dungeon")


class DungeonPurchaseService:
    def __init__(self, game_database: str | Path) -> None:
        self.application = DungeonApplication(game_database, game_database)

    def operation_result(self, *args: Any, **kwargs: Any):
        _warn("DungeonPurchaseService")
        return self.application.operation_result(operation_id=kwargs.get("operation_id", args[0] if args else ""), user_id=kwargs.get("user_id", args[1] if len(args) > 1 else ""), item_id=kwargs.get("item_id", args[2] if len(args) > 2 else 0), quantity=kwargs.get("quantity", args[3] if len(args) > 3 else 0), bind_flag=kwargs.get("bind_flag", args[4] if len(args) > 4 else 1))

    def purchase(self, *args: Any, **kwargs: Any):
        _warn("DungeonPurchaseService")
        from ..xiuxian.xiuxian_dungeon.transaction_service import DungeonPurchaseResult

        names = ("operation_id", "user_id", "item_id", "item_name", "item_type", "quantity", "unit_cost", "expected_stone", "max_goods", "bind_flag")
        values = dict(zip(names, args))
        values.update(kwargs)
        outcome = self.application.purchase(**values)
        data = outcome.data or {}
        return DungeonPurchaseResult(str(data.get("status", outcome.status)), int(data.get("quantity", values.get("quantity", 0)) or 0), int(data.get("cost", 0) or 0), int(data.get("stone", values.get("expected_stone", 0)) or 0), int(data.get("inventory", 0) or 0), str(data.get("response", outcome.message or "")))


class DungeonExploreOperationService:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.application = DungeonApplication(game_database, player_database)

    def replay(self, operation_id: str, user_id: str):
        _warn("DungeonExploreOperationService")
        return self.application.replay(operation_id=operation_id, user_id=user_id)

    def prepare(self, operation_id: str, user_id: str, plan: dict[str, Any]):
        _warn("DungeonExploreOperationService")
        return self.application.prepare(operation_id=operation_id, user_id=user_id, plan=plan)

    def settle(self, operation_id: str, user_id: str, max_goods_num: int):
        _warn("DungeonExploreOperationService")
        return self.application.settle(operation_id=operation_id, user_id=user_id, max_goods_num=max_goods_num)

    def resolve_rejection(self, operation_id: str, user_id: str, result_status: str, response: dict[str, Any], max_goods_num: int, **kwargs: Any):
        _warn("DungeonExploreOperationService")
        return self.application.resolve_rejection(operation_id=operation_id, user_id=user_id, result_status=result_status, response=response, max_goods_num=max_goods_num, **kwargs)


__all__ = ["DungeonPurchaseService", "DungeonExploreOperationService"]
