from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class SectFairylandSqlRepository:
    _COLUMNS = {
        "operation_id",
        "actor_id",
        "sect_id",
        "from_level",
        "to_level",
        "stone_cost",
        "materials_cost",
        "created_at",
    }

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='sect_fairyland_operations'"
        )
        if table is None:
            return False
        columns = {
            str(row["name"])
            for row in uow.query_all("PRAGMA table_info(sect_fairyland_operations)")
        }
        return cls._COLUMNS.issubset(columns)

    def upgrade(
        self,
        operation_id: str,
        actor_id: str,
        sect_id: int,
        expected_level: int,
        next_level: int,
        stone_cost: int,
        materials_cost: int,
        *,
        owner_position: int = 0,
    ) -> dict[str, Any]:
        operation_id, actor_id = str(operation_id).strip(), str(actor_id)
        sect_id = int(sect_id)
        expected_level, next_level = int(expected_level), int(next_level)
        stone_cost, materials_cost = max(int(stone_cost), 0), max(int(materials_cost), 0)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return {"status": "schema_missing", "actor_id": actor_id, "sect_id": sect_id}

            previous = uow.query_one(
                "SELECT from_level,to_level,stone_cost,materials_cost "
                "FROM sect_fairyland_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return {
                    "status": "duplicate",
                    "actor_id": actor_id,
                    "sect_id": sect_id,
                    "from_level": int(previous["from_level"]),
                    "to_level": int(previous["to_level"]),
                    "stone_cost": int(previous["stone_cost"]),
                    "materials_cost": int(previous["materials_cost"]),
                }

            actor = uow.query_one(
                "SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?",
                (actor_id,),
            )
            sect = uow.query_one(
                "SELECT sect_owner,COALESCE(sect_fairyland,0) AS level,"
                "COALESCE(sect_used_stone,0) AS stones,"
                "COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            if actor is None:
                return {"status": "actor_missing", "actor_id": actor_id, "sect_id": sect_id}
            if sect is None:
                return {"status": "sect_missing", "actor_id": actor_id, "sect_id": sect_id}

            current_level = int(sect["level"])
            result = {
                "actor_id": actor_id,
                "sect_id": sect_id,
                "from_level": current_level,
                "to_level": next_level,
                "stone_cost": stone_cost,
                "materials_cost": materials_cost,
            }
            if int(actor["sect_id"] or 0) != sect_id:
                return {"status": "membership_changed", **result}
            if str(sect["sect_owner"]) != actor_id or int(actor["sect_position"] or 0) != int(owner_position):
                return {"status": "not_owner", **result}
            if current_level != expected_level or next_level != current_level + 1:
                return {"status": "level_changed", **result}
            if int(sect["stones"]) < stone_cost:
                return {"status": "stone_insufficient", **result}
            if int(sect["materials"]) < materials_cost:
                return {"status": "materials_insufficient", **result}

            changed = uow.execute(
                "UPDATE sects SET sect_fairyland=?,"
                "sect_used_stone=CAST(COALESCE(sect_used_stone,0) AS REAL)-CAST(? AS REAL),"
                "sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)-CAST(? AS REAL) "
                "WHERE sect_id=? AND sect_owner=? AND COALESCE(sect_fairyland,0)=? "
                "AND COALESCE(sect_used_stone,0)>=? AND COALESCE(sect_materials,0)>=?",
                (
                    next_level,
                    stone_cost,
                    materials_cost,
                    sect_id,
                    actor_id,
                    current_level,
                    stone_cost,
                    materials_cost,
                ),
            )
            if changed.rowcount != 1:
                raise RuntimeError("sect fairyland changed concurrently")

            uow.execute(
                "INSERT INTO sect_fairyland_operations(operation_id,actor_id,sect_id,from_level,"
                "to_level,stone_cost,materials_cost) VALUES(?,?,?,?,?,?,?)",
                (operation_id, actor_id, sect_id, current_level, next_level, stone_cost, materials_cost),
            )
            return {"status": "upgraded", **result}


__all__ = ["SectFairylandSqlRepository"]
