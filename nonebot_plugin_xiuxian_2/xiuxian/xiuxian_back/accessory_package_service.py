"""Compatibility facade for the refactored cross-database accessory flow."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import warnings

from ...features.accessory_package.application import AccessoryPackageApplication, AccessoryPackageResult

# The implementation owns ATTACH DATABASE and BEGIN IMMEDIATE for this boundary.
OPERATION_TABLE = "accessory_package_operations"

class AccessoryPackageService:
    """Keep the historical synchronous API while using the new protocol."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.application = AccessoryPackageApplication(game_database, player_database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        package_id: int,
        quantity: int,
        rewards: Any,
        accessories: Any,
        *,
        max_goods_num: int,
        accessory_limit: int,
    ) -> AccessoryPackageResult:
        warnings.warn(
            "AccessoryPackageService is a compatibility facade; use AccessoryPackageApplication",
            DeprecationWarning,
            stacklevel=2,
        )
        from ...compatibility.commands import record_compatibility_hit

        record_compatibility_hit("accessory_package")
        outcome = self.application.open_package(
            operation_id=operation_id,
            user_id=user_id,
            package_id=package_id,
            quantity=quantity,
            rewards=rewards,
            accessories=accessories,
            max_goods_num=max_goods_num,
            accessory_limit=accessory_limit,
        )
        data = outcome.data if isinstance(outcome.data, dict) else {}
        status = "duplicate" if outcome.status == "replayed" else outcome.status
        return AccessoryPackageResult(
            status=status,
            user_id=str(data.get("user_id", user_id)),
            package_id=int(data.get("package_id", package_id)),
            quantity=int(data.get("quantity", quantity)),
            rewards=tuple(rewards),
            accessories=tuple(data.get("accessories", accessories)),
        )


__all__ = ["AccessoryPackageResult", "AccessoryPackageService", "OPERATION_TABLE"]
