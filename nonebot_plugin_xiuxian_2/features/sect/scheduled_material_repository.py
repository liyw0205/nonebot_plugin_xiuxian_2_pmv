from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class SectScheduledMaterialSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def list_targets(self) -> list[tuple[Any, ...]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT sect_id,sect_scale,elixir_room_level FROM sects "
                "WHERE sect_owner IS NOT NULL ORDER BY sect_scale DESC"
            )
            return [
                (row["sect_id"], row["sect_scale"], row["elixir_room_level"])
                for row in rows
            ]

    def grant(self, grant_key: str, sect_id: int, multiplier: int) -> dict[str, Any]:
        grant_key = str(grant_key).strip()
        sect_id, multiplier = int(sect_id), max(int(multiplier), 0)
        if not grant_key:
            raise ValueError("grant_key must not be empty")

        base = {
            "grant_key": grant_key,
            "sect_id": sect_id,
            "materials": 0,
            "combat_power": 0,
        }
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            receipt_table = uow.query_one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                ("sect_scheduled_material_grants",),
            )
            if receipt_table is None:
                return {"status": "schema_missing", **base}

            old = uow.query_one(
                "SELECT materials,combat_power FROM sect_scheduled_material_grants "
                "WHERE grant_key=? AND sect_id=?",
                (grant_key, sect_id),
            )
            if old:
                return {
                    "status": "duplicate",
                    **base,
                    "materials": int(old["materials"]),
                    "combat_power": int(old["combat_power"]),
                }

            sect = uow.query_one(
                "SELECT sect_scale,sect_owner FROM sects WHERE sect_id=?", (sect_id,)
            )
            if sect is None:
                return {"status": "sect_missing", **base}
            if sect["sect_owner"] is None:
                return {"status": "sect_inactive", **base}

            materials = max(int(sect["sect_scale"] or 0), 0) * multiplier
            power = uow.query_one(
                "SELECT COALESCE(SUM(power),0) AS total FROM user_xiuxian WHERE sect_id=?",
                (sect_id,),
            )
            combat_power = int(power["total"] or 0)
            uow.execute(
                "UPDATE sects SET sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)+?,"
                "combat_power=? WHERE sect_id=?",
                (materials, combat_power, sect_id),
            )
            uow.execute(
                "INSERT INTO sect_scheduled_material_grants "
                "(grant_key,sect_id,materials,combat_power) VALUES(?,?,?,?)",
                (grant_key, sect_id, materials, combat_power),
            )
            return {
                "status": "granted",
                **base,
                "materials": materials,
                "combat_power": combat_power,
            }


__all__ = ["SectScheduledMaterialSqlRepository"]
