from __future__ import annotations

from pathlib import Path

from .equipment_repository import EquipmentChange, EquipmentSqlRepository


class EquipmentApplication:
    """Feature-owned use case for equipping and unequipping equipment."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: EquipmentSqlRepository | None = None,
    ) -> None:
        self.repository = repository or EquipmentSqlRepository(database)

    def change(
        self,
        operation_id: str,
        user_id: str,
        goods_id: int,
        item_type: str,
        *,
        equip: bool,
    ) -> EquipmentChange:
        return self.repository.change(
            operation_id,
            user_id,
            goods_id,
            item_type,
            equip=equip,
        )


__all__ = ["EquipmentApplication", "EquipmentChange"]
