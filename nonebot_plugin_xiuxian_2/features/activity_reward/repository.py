from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class ActivityRewardRepository(Protocol):
    def claim_all(self, operation_id: str, user_id: str) -> Any: ...


class LegacyActivityRewardRepository:
    """Lazy adapter around the existing activity claim coordinator."""

    def __init__(self, activity_database: str | Path) -> None:
        self.activity_database = str(activity_database)

    def claim_all(self, operation_id: str, user_id: str) -> Any:
        try:
            from ...xiuxian.xiuxian_activity.service import claim_activity_rewards
        except (ImportError, RuntimeError, ValueError):
            return False, "活动兼容服务尚未就绪"
        return claim_activity_rewards(str(user_id), str(operation_id))


__all__ = ["ActivityRewardRepository", "LegacyActivityRewardRepository"]
