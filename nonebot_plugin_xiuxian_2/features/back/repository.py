from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class BackRepository(Protocol):
    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class LegacyBackRepository:
    def __init__(self, database: str | Path, player_database: str | Path | None = None) -> None:
        self.database = str(database)
        self.player_database = str(player_database or database)

    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_back.transaction_service import (
            SkillLearningService, UnbindItemService,
        )
        from ...compatibility.legacy_back_alchemy import AlchemyService
        from ...compatibility.legacy_back_equipment import EquipmentService
        from ...compatibility.legacy_back_repair import BackpackRepairService
        from ...compatibility.legacy_back_pet_egg import BatchItemUseService
        from ...compatibility.legacy_back_package_reward import PackageRewardService
        from ...compatibility.legacy_back_accessory_package import AccessoryPackageService
        from ...compatibility.legacy_back_accessory_transaction import AccessoryTransactionService
        mapping = {
            "open_package": (PackageRewardService, "apply"),
            "use_item": (BatchItemUseService, "use_pet_eggs"),
            "change_equipment": (EquipmentService, "change"),
            "learn_skill": (SkillLearningService, "learn"),
            "repair": (BackpackRepairService, "run"),
            "use_pet_eggs": (BatchItemUseService, "use_pet_eggs"),
            "alchemy": (AlchemyService, "apply"),
            "unbind": (UnbindItemService, "apply"),
            "accessory_package": (AccessoryPackageService, "apply"),
            "accessory": (AccessoryTransactionService, "upgrade"),
        }
        if action == "repair":
            return BackpackRepairService(self.database).run(operation_id, **kwargs)
        cls, method = mapping[action]
        service = cls(self.database, self.player_database) if cls in {BatchItemUseService, AccessoryPackageService, AccessoryTransactionService} else cls(self.database)
        return getattr(service, method)(operation_id, user_id, **kwargs)


__all__ = ["BackRepository", "LegacyBackRepository"]
