from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import as_int_like
from ...infrastructure.database import DatabaseUnitOfWork
from .schema import BUFF_SEARCH_COLUMNS, SCHEMA_MISSING, table_ready


# `sects` is a legacy table: these are the live catalog columns used by both
# command handlers and the startup schema, not the old prefixed names.
_BUFF_COLUMNS = {"main": "mainbuff", "secondary": "secbuff"}


class SectBuffSearchSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _reject(step: str, subject: str | int) -> None:
        reason = f"sect_buff_search_{step}_write_rejected"
        raise DomainError(
            reason,
            f"{reason}: {subject}",
            {"reason": reason, "subject": str(subject)},
        )

    def apply(
        self,
        operation_id,
        actor_id,
        sect_id,
        buff_type,
        previous_value,
        new_value,
        stone_cost,
        materials_cost,
        *,
        owner_position: int = 0,
    ) -> dict[str, Any]:
        operation_id = str(operation_id).strip()
        actor_id = str(actor_id)
        buff_type = str(buff_type).strip().lower()
        sect_id = int(sect_id)
        stone_cost = max(int(stone_cost), 0)
        materials_cost = max(int(materials_cost), 0)
        previous_value = str(previous_value)
        new_value = str(new_value)
        owner_position = int(owner_position)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        try:
            return self._apply(
                operation_id,
                actor_id,
                sect_id,
                buff_type,
                previous_value,
                new_value,
                stone_cost,
                materials_cost,
                owner_position=owner_position,
            )
        except DomainError as exc:
            if exc.code.startswith("sect_buff_search_") and exc.code.endswith("_write_rejected"):
                return {
                    "status": "state_changed",
                    "actor_id": actor_id,
                    "sect_id": sect_id,
                    "buff_type": buff_type,
                    "previous_value": previous_value,
                    "new_value": new_value,
                    "stone_cost": stone_cost,
                    "materials_cost": materials_cost,
                    "reason": exc.details.get("reason") or exc.code,
                    "subject": str(exc.details.get("subject") or operation_id),
                }
            raise

    def _apply(
        self,
        operation_id: str,
        actor_id: str,
        sect_id: int,
        buff_type: str,
        previous_value: str,
        new_value: str,
        stone_cost: int,
        materials_cost: int,
        *,
        owner_position: int,
    ) -> dict[str, Any]:
        column = _BUFF_COLUMNS.get(buff_type)
        base = {
            "actor_id": actor_id,
            "sect_id": sect_id,
            "buff_type": buff_type,
            "previous_value": previous_value,
            "new_value": new_value,
            "stone_cost": stone_cost,
            "materials_cost": materials_cost,
        }
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not table_ready(uow, "sect_buff_search_operations", BUFF_SEARCH_COLUMNS):
                return {"status": SCHEMA_MISSING, **base, "reason": SCHEMA_MISSING, "subject": operation_id}

            old = uow.query_one(
                "SELECT buff_type,previous_value,new_value,stone_cost,materials_cost "
                "FROM sect_buff_search_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old:
                return {
                    "status": "duplicate",
                    "actor_id": actor_id,
                    "sect_id": sect_id,
                    "buff_type": str(old["buff_type"]),
                    "previous_value": str(old["previous_value"]),
                    "new_value": str(old["new_value"]),
                    "stone_cost": int(old["stone_cost"]),
                    "materials_cost": int(old["materials_cost"]),
                }

            user = uow.query_one(
                "SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?",
                (actor_id,),
            )
            select_column = column or "mainbuff"
            sect = uow.query_one(
                f"SELECT sect_owner,{select_column} AS catalog,"
                "COALESCE(sect_used_stone,0) AS stones,"
                "COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            if user is None:
                return {"status": "actor_missing", **base}
            if sect is None:
                return {"status": "sect_missing", **base}
            if int(user["sect_id"] or 0) != sect_id:
                return {"status": "membership_changed", **base}
            if str(sect["sect_owner"]) != actor_id or int(user["sect_position"] or 0) != owner_position:
                return {
                    "status": "not_owner",
                    "reason": "sect_owner_changed" if str(sect["sect_owner"]) != actor_id else "sect_position_changed",
                    **base,
                }
            if column is None:
                return {"status": "invalid_buff_type", **base}

            current_value = str(sect["catalog"] or "")
            if current_value != previous_value:
                return {
                    "status": "catalog_changed",
                    "reason": "sect_catalog_changed",
                    "subject": str(sect_id),
                    "current_value": current_value,
                    **base,
                }
            if as_int_like(sect["stones"]) < stone_cost:
                return {"status": "stone_insufficient", "reason": "sect_stone_insufficient", **base}
            if as_int_like(sect["materials"]) < materials_cost:
                return {"status": "materials_insufficient", "reason": "materials_insufficient", **base}

            changed = uow.execute(
                f"UPDATE sects SET {column}=?,"
                "sect_used_stone=xiuxian_int_store(xiuxian_int_sub(sect_used_stone,?,'sect_used_stone','actor')),"
                "sect_materials=xiuxian_int_store(xiuxian_int_sub(sect_materials,?,'sect_materials','actor')) "
                f"WHERE sect_id=? AND sect_owner=? AND COALESCE({column},'')=? "
                "AND xiuxian_int_cmp(COALESCE(sect_used_stone,0),?,1,'sect_used_stone','actor')>=0 "
                "AND xiuxian_int_cmp(COALESCE(sect_materials,0),?,1,'sect_materials','actor')>=0",
                (
                    new_value,
                    stone_cost,
                    materials_cost,
                    sect_id,
                    actor_id,
                    previous_value,
                    stone_cost,
                    materials_cost,
                ),
            )
            if changed.rowcount != 1:
                self._reject("state", sect_id)

            saved_sect = uow.query_one(
                f"SELECT {column} AS catalog,sect_used_stone,sect_materials FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            if (
                saved_sect is None
                or str(saved_sect["catalog"] or "") != new_value
                or as_int_like(saved_sect["sect_used_stone"]) != as_int_like(sect["stones"]) - stone_cost
                or as_int_like(saved_sect["sect_materials"]) != as_int_like(sect["materials"]) - materials_cost
            ):
                self._reject("state", sect_id)

            recorded = uow.execute(
                "INSERT INTO sect_buff_search_operations(operation_id,actor_id,sect_id,buff_type,"
                "previous_value,new_value,stone_cost,materials_cost) VALUES(?,?,?,?,?,?,?,?)",
                (
                    operation_id,
                    actor_id,
                    sect_id,
                    buff_type,
                    previous_value,
                    new_value,
                    stone_cost,
                    materials_cost,
                ),
            )
            if recorded.rowcount != 1:
                self._reject("receipt", operation_id)
            saved_receipt = uow.query_one(
                "SELECT actor_id,sect_id,buff_type,previous_value,new_value,stone_cost,materials_cost "
                "FROM sect_buff_search_operations WHERE operation_id=?",
                (operation_id,),
            )
            if (
                saved_receipt is None
                or str(saved_receipt["actor_id"]) != actor_id
                or int(saved_receipt["sect_id"]) != sect_id
                or str(saved_receipt["buff_type"]) != buff_type
                or str(saved_receipt["previous_value"]) != previous_value
                or str(saved_receipt["new_value"]) != new_value
                or int(saved_receipt["stone_cost"]) != stone_cost
                or int(saved_receipt["materials_cost"]) != materials_cost
            ):
                self._reject("receipt", operation_id)
            return {"status": "applied", **base}


__all__ = ["SectBuffSearchSqlRepository"]
