from __future__ import annotations

from ...core.numeric import as_int_like
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...core.errors import DomainError


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

    @staticmethod
    def _reject(step: str, subject: str | int) -> None:
        reason = f"sect_fairyland_{step}_write_rejected"
        raise DomainError(reason, f"{reason}: {subject}", {"reason": reason, "subject": str(subject)})

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

        try:
            return self._upgrade(
                operation_id, actor_id, sect_id, expected_level, next_level,
                stone_cost, materials_cost, owner_position,
            )
        except DomainError as exc:
            if exc.code.startswith("sect_fairyland_") and exc.code.endswith("_write_rejected"):
                return {
                    "status": "state_changed",
                    "actor_id": actor_id,
                    "sect_id": sect_id,
                    "from_level": expected_level,
                    "to_level": next_level,
                    "stone_cost": stone_cost,
                    "materials_cost": materials_cost,
                    "reason": exc.details.get("reason") or exc.code,
                    "subject": str(exc.details.get("subject") or operation_id),
                }
            raise

    def _upgrade(
        self,
        operation_id: str,
        actor_id: str,
        sect_id: int,
        expected_level: int,
        next_level: int,
        stone_cost: int,
        materials_cost: int,
        owner_position: int,
    ) -> dict[str, Any]:
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not self._schema_ready(uow):
                return {"status": "schema_missing", "actor_id": actor_id, "sect_id": sect_id}

            previous = uow.query_one(
                "SELECT actor_id,sect_id,from_level,to_level,stone_cost,materials_cost "
                "FROM sect_fairyland_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                # The command derives the next level and its cost from current
                # state, so a replay naturally carries newer values. The
                # operation ledger identity is authoritative for the same actor
                # and sect; a different actor/sect remains a conflict.
                same_request = str(previous["actor_id"]) == actor_id and int(previous["sect_id"]) == sect_id
                if not same_request:
                    return {
                        "status": "state_changed",
                        "actor_id": actor_id,
                        "sect_id": sect_id,
                        "from_level": expected_level,
                        "to_level": next_level,
                        "stone_cost": stone_cost,
                        "materials_cost": materials_cost,
                        "reason": "operation_conflict",
                        "subject": "actor",
                    }
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
                return {"status": "not_owner", "reason": "sect_owner_changed" if str(sect["sect_owner"]) != actor_id else "sect_position_changed", **result}
            if current_level != expected_level or next_level != current_level + 1:
                return {"status": "level_changed", "reason": "sect_fairyland_level_changed" if current_level != expected_level else "sect_upgrade_invalid", **result}
            if as_int_like(sect["stones"]) < stone_cost:
                return {"status": "stone_insufficient", "reason": "sect_stone_insufficient", **result}
            if as_int_like(sect["materials"]) < materials_cost:
                return {"status": "materials_insufficient", **result}

            changed = uow.execute(
                "UPDATE sects SET sect_fairyland=?,"
                "sect_used_stone=xiuxian_int_store(xiuxian_int_sub(COALESCE(sect_used_stone,0),?,'sect_used_stone','actor')),"
                "sect_materials=xiuxian_int_store(xiuxian_int_sub(COALESCE(sect_materials,0),?,'sect_materials','actor')) "
                "WHERE sect_id=? AND sect_owner=? AND COALESCE(sect_fairyland,0)=? "
                "AND xiuxian_int_cmp(COALESCE(sect_used_stone,0),?,1,'sect_used_stone','actor')>=0 AND xiuxian_int_cmp(COALESCE(sect_materials,0),?,1,'sect_materials','actor')>=0",
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
                self._reject("state", sect_id)
            saved_sect = uow.query_one(
                "SELECT sect_owner,COALESCE(sect_fairyland,0) AS level,"
                "COALESCE(sect_used_stone,0) AS stones,COALESCE(sect_materials,0) AS materials "
                "FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            if (
                saved_sect is None
                or str(saved_sect["sect_owner"]) != actor_id
                or as_int_like(saved_sect["level"]) != next_level
                or as_int_like(saved_sect["stones"]) != as_int_like(sect["stones"]) - stone_cost
                or as_int_like(saved_sect["materials"]) != as_int_like(sect["materials"]) - materials_cost
            ):
                self._reject("state", sect_id)

            recorded = uow.execute(
                "INSERT INTO sect_fairyland_operations(operation_id,actor_id,sect_id,from_level,"
                "to_level,stone_cost,materials_cost) VALUES(?,?,?,?,?,?,?)",
                (operation_id, actor_id, sect_id, current_level, next_level, stone_cost, materials_cost),
            )
            if recorded.rowcount != 1:
                self._reject("receipt", operation_id)
            saved_receipt = uow.query_one(
                "SELECT actor_id,sect_id,from_level,to_level,stone_cost,materials_cost "
                "FROM sect_fairyland_operations WHERE operation_id=?",
                (operation_id,),
            )
            if (
                saved_receipt is None
                or str(saved_receipt["actor_id"]) != actor_id
                or int(saved_receipt["sect_id"]) != sect_id
                or int(saved_receipt["from_level"]) != current_level
                or int(saved_receipt["to_level"]) != next_level
                or int(saved_receipt["stone_cost"]) != stone_cost
                or int(saved_receipt["materials_cost"]) != materials_cost
            ):
                self._reject("receipt", operation_id)
            final_sect = uow.query_one(
                "SELECT sect_owner,COALESCE(sect_fairyland,0) AS level,"
                "COALESCE(sect_used_stone,0) AS stones,COALESCE(sect_materials,0) AS materials "
                "FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            if (
                final_sect is None
                or str(final_sect["sect_owner"]) != actor_id
                or as_int_like(final_sect["level"]) != next_level
                or as_int_like(final_sect["stones"]) != as_int_like(sect["stones"]) - stone_cost
                or as_int_like(final_sect["materials"]) != as_int_like(sect["materials"]) - materials_cost
            ):
                self._reject("state", sect_id)
            return {"status": "upgraded", **result}


__all__ = ["SectFairylandSqlRepository"]
