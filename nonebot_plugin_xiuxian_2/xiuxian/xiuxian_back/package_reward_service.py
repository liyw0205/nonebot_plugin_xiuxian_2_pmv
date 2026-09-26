"""Compatibility facade for the historical package reward API."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any
import warnings

from ...core.errors import DomainError
from ...features.package_reward.application import PackageRewardApplication
from ...features.package_reward.domain import PackageReward

OPERATION_TABLE = "package_reward_operations"
# The feature application owns a BEGIN IMMEDIATE transaction and this
# package_reward_operations projection; this module is only a compatibility
# facade for callers that still import the historical service.


@dataclass(frozen=True)
class PackageOpenResult:
    status: str
    user_id: str
    package_id: int
    quantity: int
    rewards: tuple[PackageReward, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PackageRewardService:
    """Keep the old synchronous result contract while forwarding to the use case."""

    def __init__(self, database: str | Path) -> None:
        self.application = PackageRewardApplication(str(database))
        enabled = os.environ.get("XIUXIAN_PACKAGE_REWARD_ENABLED", "true").strip().lower()
        self._legacy = None
        if enabled in {"0", "false", "no", "off"}:
            from ...compatibility.legacy_back_package_reward import (
                PackageRewardService as LegacyPackageRewardService,
            )

            self._legacy = LegacyPackageRewardService(database)

    @staticmethod
    def _warn() -> None:
        warnings.warn(
            "PackageRewardService is a compatibility facade; use PackageRewardApplication",
            DeprecationWarning,
            stacklevel=3,
        )
        from ...compatibility.commands import record_compatibility_hit

        record_compatibility_hit("package_reward")

    @staticmethod
    def _result(status: str, user_id: str, package_id: int, quantity: int, rewards: tuple[PackageReward, ...]) -> PackageOpenResult:
        return PackageOpenResult(status, str(user_id), int(package_id), int(quantity), tuple(rewards))

    def apply(self, operation_id: str, user_id: str, package_id: int, quantity: int, rewards: Any, *, max_goods_num: int) -> PackageOpenResult:
        self._warn()
        if self._legacy is not None:
            return self._legacy.apply(operation_id, user_id, package_id, quantity, rewards, max_goods_num=max_goods_num)
        operation_id = str(operation_id).strip()
        normalized = tuple(rewards)
        previous = self.application.lookup(operation_id)
        if previous is not None:
            if previous["user_id"] != str(user_id) or previous["package_id"] != int(package_id) or previous["quantity"] != int(quantity):
                return self._result("state_changed", user_id, package_id, quantity, normalized)
            return self._result("duplicate", previous["user_id"], previous["package_id"], previous["quantity"], previous["rewards"])
        try:
            outcome = self.application.open_package(
                operation_id=operation_id,
                user_id=user_id,
                package_id=package_id,
                quantity=quantity,
                rewards=normalized,
                max_goods_num=max_goods_num,
            )
        except DomainError as exc:
            if exc.code == "validation_error":
                raise ValueError(exc.message) from exc
            raise
        data = outcome.data or {}
        raw = data.get("package_reward", {}) if isinstance(data, dict) else {}
        decoded = tuple(PackageReward(**item) for item in raw.get("rewards", []))
        status = "applied" if outcome.status == "applied" else "duplicate" if outcome.status == "replayed" else outcome.code or outcome.status
        return self._result(status, raw.get("user_id", user_id), raw.get("package_id", package_id), raw.get("quantity", quantity), decoded or normalized)


__all__ = ["PackageOpenResult", "PackageReward", "PackageRewardService", "OPERATION_TABLE"]
