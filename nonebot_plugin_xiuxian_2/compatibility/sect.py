"""Compatibility facades for migrated high-risk sect operations."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

from ..features.sect.application import SectApplication


def _warn(name: str) -> None:
    warnings.warn(f"{name} is a compatibility facade; use SectApplication", DeprecationWarning, stacklevel=3)
    from .commands import record_compatibility_hit
    record_compatibility_hit("sect")


class SectMemberJoinService:
    def __init__(self, database: str | Path) -> None:
        self.application = SectApplication(database)

    def join(self, *args: Any, **kwargs: Any):
        _warn("SectMemberJoinService")
        from ..xiuxian.xiuxian_sect.transaction_service import SectMemberJoinResult
        names = ("operation_id", "user_id", "sect_id")
        values = dict(zip(names, args)); values.update(kwargs)
        outcome = self.application.join(**values); data = outcome.data or {}
        return SectMemberJoinResult(str(data.get("status", outcome.status)), str(data.get("user_id", values.get("user_id", ""))), int(data.get("sect_id", values.get("sect_id", 0)) or 0), str(data.get("sect_name", "")), int(data.get("member_count", 0) or 0), int(data.get("member_limit", 0) or 0))


class SectShopPurchaseService:
    def __init__(self, database: str | Path) -> None:
        self.application = SectApplication(database)

    def purchase(self, *args: Any, **kwargs: Any):
        _warn("SectShopPurchaseService")
        from ..xiuxian.xiuxian_sect.transaction_service import SectShopPurchaseResult
        names = ("operation_id", "user_id", "sect_id", "item_id", "item_name", "item_type", "quantity", "unit_cost", "weekly_limit", "legacy_purchased", "max_goods_num", "week_key")
        values = dict(zip(names, args)); values.update(kwargs)
        outcome = self.application.purchase(**values); data = outcome.data or {}
        return SectShopPurchaseResult(str(data.get("status", outcome.status)), int(data.get("quantity", 0) or 0), int(data.get("cost", 0) or 0), int(data.get("contribution", 0) or 0), int(data.get("materials", 0) or 0), int(data.get("purchased", 0) or 0))


class _BuffFacade:
    method = "learn_main"
    result_name = "SectMainBuffLearnResult"

    def __init__(self, database: str | Path) -> None:
        self.application = SectApplication(database)

    def learn(self, *args: Any, **kwargs: Any):
        _warn(self.__class__.__name__)
        from ..xiuxian.xiuxian_sect import transaction_service
        names = ("operation_id", "user_id", "sect_id", "buff_id", "materials_cost")
        values = dict(zip(names, args)); values.update(kwargs)
        outcome = getattr(self.application, self.method)(**values); data = outcome.data or {}
        cls = getattr(transaction_service, self.result_name)
        return cls(str(data.get("status", outcome.status)), str(values.get("user_id", "")), int(values.get("sect_id", 0)), int(values.get("buff_id", 0)), int(data.get("materials_cost", 0) or 0), int(data.get("materials_left", 0) or 0))


class SectMainBuffLearnService(_BuffFacade):
    method = "learn_main"
    result_name = "SectMainBuffLearnResult"


class SectSecBuffLearnService(_BuffFacade):
    method = "learn_secondary"
    result_name = "SectSecBuffLearnResult"


class SectElixirClaimService:
    def __init__(self, database: str | Path) -> None:
        self.application = SectApplication(database)

    def claim(self, *args: Any, **kwargs: Any):
        _warn("SectElixirClaimService")
        from ..xiuxian.xiuxian_sect.transaction_service import SectElixirClaimResult
        names = ("operation_id", "user_id", "sect_id", "contribution_required", "materials_required", "rewards", "max_goods_num")
        values = dict(zip(names, args)); values.update(kwargs)
        outcome = self.application.claim_elixir(**values); data = outcome.data or {}
        return SectElixirClaimResult(str(data.get("status", outcome.status)), tuple(tuple(item) for item in data.get("rewards", ()) or ()))


__all__ = ["SectMemberJoinService", "SectShopPurchaseService", "SectMainBuffLearnService", "SectSecBuffLearnService", "SectElixirClaimService"]
