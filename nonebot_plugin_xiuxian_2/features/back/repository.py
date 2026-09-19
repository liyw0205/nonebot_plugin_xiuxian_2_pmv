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
            AccessoryPackageService, AccessoryTransactionService, AlchemyService,
            BackpackRepairService, BatchItemUseService, EquipmentService,
            PackageRewardService, SkillLearningService, UnbindItemService,
        )
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
