from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectSecBuffLearnResult:
    status: str
    user_id: str
    sect_id: int
    buff_id: int
    materials_cost: int = 0
    materials_left: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "learned"


class SectSecBuffLearnService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sect_secbuff_learn_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_id INTEGER NOT NULL, "
            "buff_id INTEGER NOT NULL, materials_cost INTEGER NOT NULL, materials_left INTEGER NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def learn(
        self,
        operation_id,
        user_id,
        sect_id,
        buff_id,
        materials_cost,
        *,
        expected_catalog,
        forbidden_positions=(12, 14, 15),
    ) -> SectSecBuffLearnResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        sect_id = int(sect_id)
        buff_id = int(buff_id)
        materials_cost = int(materials_cost)
        if materials_cost < 0:
            raise ValueError("materials_cost must not be negative")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT user_id, sect_id, buff_id, materials_cost, materials_left "
                    "FROM sect_secbuff_learn_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    if (str(previous[0]), int(previous[1]), int(previous[2])) != (
                        user_id,
                        sect_id,
                        buff_id,
                    ):
                        return SectSecBuffLearnResult("state_changed", user_id, sect_id, buff_id)
                    return SectSecBuffLearnResult(
                        "duplicate", user_id, sect_id, buff_id, int(previous[3]), int(previous[4])
                    )

                user = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or user[0] is None or int(user[0]) != sect_id:
                    conn.rollback()
                    return SectSecBuffLearnResult("membership_changed", user_id, sect_id, buff_id)
                if int(user[1]) in {int(value) for value in forbidden_positions}:
                    conn.rollback()
                    return SectSecBuffLearnResult("position_forbidden", user_id, sect_id, buff_id)

                sect = conn.execute(
                    "SELECT secbuff, COALESCE(sect_materials, 0) FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None or str(sect[0]) != str(expected_catalog):
                    conn.rollback()
                    return SectSecBuffLearnResult("catalog_changed", user_id, sect_id, buff_id)
                materials = int(sect[1])
                if materials < materials_cost:
                    conn.rollback()
                    return SectSecBuffLearnResult(
                        "materials_insufficient", user_id, sect_id, buff_id, materials_cost, materials
                    )

                buff = conn.execute(
                    "SELECT sec_buff FROM BuffInfo WHERE user_id=%s", (user_id,)
                ).fetchone()
                if buff is None:
                    conn.rollback()
                    return SectSecBuffLearnResult("buff_missing", user_id, sect_id, buff_id)
                if int(buff[0] or 0) == buff_id:
                    conn.rollback()
                    return SectSecBuffLearnResult("already_learned", user_id, sect_id, buff_id)

                materials_left = materials - materials_cost
                sect_update = conn.execute(
                    "UPDATE sects SET sect_materials=%s WHERE sect_id=%s AND sect_materials=%s AND secbuff=%s",
                    (materials_left, sect_id, materials, str(expected_catalog)),
                )
                buff_update = conn.execute(
                    "UPDATE BuffInfo SET sec_buff=%s WHERE user_id=%s AND COALESCE(sec_buff, 0)<>%s",
                    (buff_id, user_id, buff_id),
                )
                if sect_update.rowcount != 1 or buff_update.rowcount != 1:
                    raise db_backend.IntegrityError("sect secondary buff learning state changed concurrently")
                conn.execute(
                    "INSERT INTO sect_secbuff_learn_operations "
                    "(operation_id, user_id, sect_id, buff_id, materials_cost, materials_left) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, sect_id, buff_id, materials_cost, materials_left),
                )
                conn.commit()
                return SectSecBuffLearnResult(
                    "learned", user_id, sect_id, buff_id, materials_cost, materials_left
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["SectSecBuffLearnResult", "SectSecBuffLearnService"]
