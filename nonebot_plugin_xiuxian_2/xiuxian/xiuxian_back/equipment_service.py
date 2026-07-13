from __future__ import annotations

from contextlib import closing
import json
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class EquipmentChange:
    status: str
    user_id: str
    goods_id: int
    previous_id: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"equipped", "unequipped", "duplicate"}


class EquipmentService:
    SLOT_COLUMNS = {"法器": "faqi_buff", "防具": "armor_buff"}

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS equipment_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                goods_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                previous_id INTEGER NOT NULL DEFAULT 0,
                payload TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        if "payload" not in conn.column_names("equipment_operations"):
            conn.execute("ALTER TABLE equipment_operations ADD COLUMN payload TEXT NOT NULL DEFAULT ''")
    def change(self, operation_id, user_id, goods_id, item_type, *, equip: bool):
        column = self.SLOT_COLUMNS.get(str(item_type))
        if column is None:
            return EquipmentChange("unsupported_type", str(user_id), int(goods_id))
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        payload = json.dumps([user_id, goods_id, str(item_type), bool(equip)], ensure_ascii=True)
        goods_id = int(goods_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                row = conn.execute(
                    "SELECT previous_id, payload FROM equipment_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if row:
                    conn.rollback()
                    if str(row[1] or "") != payload:
                        return EquipmentChange("state_changed", user_id, goods_id, int(row[0]))
                    return EquipmentChange("duplicate", user_id, goods_id, int(row[0]))
                inventory = conn.execute(
                    "SELECT goods_num, state FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, goods_id),
                ).fetchone()
                if inventory is None or int(inventory[0] or 0) <= 0:
                    conn.rollback()
                    return EquipmentChange("item_missing", user_id, goods_id)
                buff = conn.execute(
                    f"SELECT {column} FROM BuffInfo WHERE user_id=%s", (user_id,)
                ).fetchone()
                if buff is None:
                    conn.rollback()
                    return EquipmentChange("buff_missing", user_id, goods_id)
                previous_id = int(buff[0] or 0)
                if equip and previous_id == goods_id and int(inventory[1] or 0) == 1:
                    conn.rollback()
                    return EquipmentChange("already_equipped", user_id, goods_id, previous_id)
                if not equip and previous_id != goods_id:
                    conn.rollback()
                    return EquipmentChange("not_equipped", user_id, goods_id, previous_id)

                if previous_id:
                    conn.execute(
                        "UPDATE back SET state=0, update_time=CURRENT_TIMESTAMP, "
                        "action_time=CURRENT_TIMESTAMP WHERE user_id=%s AND goods_id=%s",
                        (user_id, previous_id),
                    )
                target_id = goods_id if equip else 0
                if equip:
                    conn.execute(
                        "UPDATE back SET state=1, update_time=CURRENT_TIMESTAMP, "
                        "action_time=CURRENT_TIMESTAMP WHERE user_id=%s AND goods_id=%s",
                        (user_id, goods_id),
                    )
                conn.execute(
                    f"UPDATE BuffInfo SET {column}=%s WHERE user_id=%s",
                    (target_id, user_id),
                )
                conn.execute(
                    "INSERT INTO equipment_operations (operation_id,user_id,goods_id,action,previous_id,payload,created_at) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, user_id, goods_id, "equip" if equip else "unequip", previous_id, payload),
                )
                conn.commit()
                return EquipmentChange(
                    "equipped" if equip else "unequipped", user_id, goods_id, previous_id
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["EquipmentChange", "EquipmentService"]
