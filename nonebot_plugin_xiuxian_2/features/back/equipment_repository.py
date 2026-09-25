from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class EquipmentChange:
    status: str
    user_id: str
    goods_id: int
    previous_id: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"equipped", "unequipped", "duplicate"}


class EquipmentSqlRepository:
    """Atomically update the selected equipment slot and inventory state."""

    SLOT_COLUMNS = {"法器": "faqi_buff", "防具": "armor_buff"}

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _payload(user_id: str, goods_id: int, item_type: str, equip: bool) -> str:
        return json.dumps(
            [user_id, int(goods_id), str(item_type), bool(equip)],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    def change(
        self,
        operation_id: str,
        user_id: str,
        goods_id: int,
        item_type: str,
        *,
        equip: bool,
    ) -> EquipmentChange:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        goods_id = int(goods_id)
        item_type = str(item_type)
        equip = bool(equip)
        column = self.SLOT_COLUMNS.get(item_type)
        if column is None:
            return EquipmentChange("unsupported_type", user_id, goods_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        payload = self._payload(user_id, goods_id, item_type, equip)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT previous_id,payload FROM equipment_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"] or "") != payload:
                    return EquipmentChange("state_changed", user_id, goods_id, int(previous["previous_id"]))
                return EquipmentChange("duplicate", user_id, goods_id, int(previous["previous_id"]))

            inventory = uow.query_one(
                "SELECT goods_num,state FROM back WHERE user_id=? AND goods_id=?",
                (user_id, goods_id),
            )
            if inventory is None or int(inventory["goods_num"] or 0) <= 0:
                return EquipmentChange("item_missing", user_id, goods_id)

            buff = uow.query_one(
                f"SELECT {column} AS equipped_id FROM BuffInfo WHERE user_id=?",
                (user_id,),
            )
            if buff is None:
                return EquipmentChange("buff_missing", user_id, goods_id)
            previous_id = int(buff["equipped_id"] or 0)
            if equip and previous_id == goods_id and int(inventory["state"] or 0) == 1:
                return EquipmentChange("already_equipped", user_id, goods_id, previous_id)
            if not equip and previous_id != goods_id:
                return EquipmentChange("not_equipped", user_id, goods_id, previous_id)

            if previous_id:
                uow.execute(
                    "UPDATE back SET state=0,update_time=CURRENT_TIMESTAMP,"
                    "action_time=CURRENT_TIMESTAMP WHERE user_id=? AND goods_id=?",
                    (user_id, previous_id),
                )
            target_id = goods_id if equip else 0
            if equip:
                uow.execute(
                    "UPDATE back SET state=1,update_time=CURRENT_TIMESTAMP,"
                    "action_time=CURRENT_TIMESTAMP WHERE user_id=? AND goods_id=?",
                    (user_id, goods_id),
                )
            uow.execute(
                f"UPDATE BuffInfo SET {column}=? WHERE user_id=?",
                (target_id, user_id),
            )
            uow.execute(
                "INSERT INTO equipment_operations "
                "(operation_id,user_id,goods_id,action,previous_id,payload) "
                "VALUES(?,?,?,?,?,?)",
                (
                    operation_id,
                    user_id,
                    goods_id,
                    "equip" if equip else "unequip",
                    previous_id,
                    payload,
                ),
            )
            return EquipmentChange(
                "equipped" if equip else "unequipped",
                user_id,
                goods_id,
                previous_id,
            )


__all__ = ["EquipmentChange", "EquipmentSqlRepository"]
