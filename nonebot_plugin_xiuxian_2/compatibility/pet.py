"""Compatibility facades for migrated pet asset actions."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

from ..features.pet.application import PetApplication


def _warn(name: str) -> None:
    warnings.warn(f"{name} is a compatibility facade; use PetApplication", DeprecationWarning, stacklevel=3)
    from .commands import record_compatibility_hit

    record_compatibility_hit("pet")


class PetTravelClaimService:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.application = PetApplication(game_database, player_database)

    def claim(self, *args: Any, **kwargs: Any):
        _warn("PetTravelClaimService")
        from ..xiuxian.xiuxian_pet.transaction_service import PetTravelClaimResult

        names = ("operation_id", "user_id", "expected_travel", "stone", "exp", "items", "max_goods_num")
        values = dict(zip(names, args)); values.update(kwargs)
        outcome = self.application.claim_travel(**values)
        data = outcome.data or {}
        return PetTravelClaimResult(str(data.get("status", outcome.status)), int(data.get("stone", 0) or 0), int(data.get("exp", 0) or 0), tuple(tuple(item) for item in data.get("items", ()) or ()))


class PetFeedService:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.application = PetApplication(game_database, player_database)

    def feed(self, *args: Any, **kwargs: Any):
        _warn("PetFeedService")
        from ..xiuxian.xiuxian_pet.transaction_service import PetFeedResult

        names = ("operation_id", "user_id", "uid", "item_id", "count", "expected", "updated")
        values = dict(zip(names, args)); values.update(kwargs)
        outcome = self.application.feed(**values)
        data = outcome.data or {}
        return PetFeedResult(str(data.get("status", outcome.status)), int(data.get("stars", 0) or 0), int(data.get("exp", 0) or 0), int(data.get("total_exp", 0) or 0))


class PetTravelStartService:
    def __init__(self, player_database: str | Path) -> None:
        self.application = PetApplication(player_database, player_database)

    def start(self, *args: Any, **kwargs: Any):
        _warn("PetTravelStartService")
        from ..xiuxian.xiuxian_pet.transaction_service import PetTravelStartResult

        names = ("operation_id", "user_id", "pet_uid", "expected_travel", "travel")
        values = dict(zip(names, args)); values.update(kwargs)
        outcome = self.application.start_travel(**values)
        return PetTravelStartResult(str((outcome.data or {}).get("status", outcome.status)))


class PetHatchService:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.application = PetApplication(game_database, player_database)

    def get_result(self, operation_id: str):
        _warn("PetHatchService")
        return self.application.hatch_result(operation_id=operation_id)

    def hatch(self, *args: Any, **kwargs: Any):
        _warn("PetHatchService")
        from ..xiuxian.xiuxian_pet.transaction_service import PetHatchResult

        names = ("operation_id", "user_id", "expected_stone", "cost", "expected_meta", "pets", "updated_meta", "bag_limit")
        values = dict(zip(names, args)); values.update(kwargs)
        outcome = self.application.hatch(**values)
        data = outcome.data or {}
        return PetHatchResult(str(data.get("status", outcome.status)), int(data.get("cost", values.get("cost", 0)) or 0), tuple(data.get("pets", ()) or ()), tuple(data.get("updated_meta", ()) or ()), int(data.get("bag_limit", values.get("bag_limit", 0)) or 0))


__all__ = ["PetTravelClaimService", "PetFeedService", "PetTravelStartService", "PetHatchService"]
