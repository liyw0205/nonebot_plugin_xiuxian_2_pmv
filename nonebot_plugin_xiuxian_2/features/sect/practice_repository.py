from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import as_int_like
from ...infrastructure.database import DatabaseUnitOfWork
from .schema import PRACTICE_COLUMNS, SCHEMA_MISSING, table_ready


class SectPracticeSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _reject(step: str, subject: str | int) -> None:
        reason = f"sect_practice_{step}_write_rejected"
        raise DomainError(reason, f"{reason}: {subject}", {"reason": reason, "subject": str(subject)})

    def upgrade(
        self,
        operation_id,
        user_id,
        sect_id,
        practice_type,
        expected_level,
        next_level,
        stone_cost,
        materials_cost,
    ) -> dict[str, Any]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        sect_id = int(sect_id)
        practice_type = str(practice_type).strip().lower()
        expected_level = int(expected_level)
        next_level = int(next_level)
        stone_cost = max(int(stone_cost), 0)
        materials_cost = max(int(materials_cost), 0)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        columns = {
            "attack": ("atkpractice", "attack_practice"),
            "health": ("hppractice", "health_practice"),
            "mana": ("mppractice", "mana_practice"),
        }
        candidates = columns.get(practice_type)
        base = {
            "user_id": user_id,
            "sect_id": sect_id,
            "practice_type": practice_type,
            "from_level": expected_level,
            "to_level": next_level,
            "stone_cost": stone_cost,
            "materials_cost": materials_cost,
        }
        if candidates is None:
            return {"status": "invalid_practice_type", **base}
        try:
            return self._upgrade(
                operation_id,
                base,
                candidates,
            )
        except DomainError as exc:
            if exc.code.startswith("sect_practice_") and exc.code.endswith("_write_rejected"):
                return {
                    "status": "state_changed",
                    **base,
                    "reason": exc.details.get("reason") or exc.code,
                    "subject": str(exc.details.get("subject") or operation_id),
                }
            raise

    def _upgrade(self, operation_id: str, base: dict[str, Any], candidates: tuple[str, str]) -> dict[str, Any]:
        user_id = base["user_id"]
        sect_id = base["sect_id"]
        practice_type = base["practice_type"]
        expected_level = base["from_level"]
        next_level = base["to_level"]
        stone_cost = base["stone_cost"]
        materials_cost = base["materials_cost"]
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not table_ready(uow, "sect_practice_operations", PRACTICE_COLUMNS):
                return {"status": SCHEMA_MISSING, **base, "reason": SCHEMA_MISSING, "subject": operation_id}
            user_columns = {str(row["name"]) for row in uow.query_all('PRAGMA table_info("user_xiuxian")')}
            column = next((name for name in candidates if name in user_columns), None)
            if column is None or "stone" not in user_columns:
                return {"status": SCHEMA_MISSING, **base, "reason": "sect_practice_columns_missing", "subject": user_id}

            old = uow.query_one(
                "SELECT user_id,sect_id,practice_type,from_level,to_level,stone_cost,materials_cost "
                "FROM sect_practice_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old:
                old_values = (
                    str(old["user_id"]), int(old["sect_id"]), str(old["practice_type"]),
                    int(old["from_level"]), int(old["to_level"]), int(old["stone_cost"]),
                    int(old["materials_cost"]),
                )
                requested = (
                    user_id, sect_id, practice_type, expected_level, next_level,
                    stone_cost, materials_cost,
                )
                # A replay is rebuilt from the player's current level, which may
                # be the receipt's ``to_level`` after the original request.
                same_request = (
                    old_values[:3] == requested[:3]
                    and old_values[4] - old_values[3] == requested[4] - requested[3]
                    and old_values[5:] == requested[5:]
                )
                if not same_request:
                    return {
                        "status": "state_changed",
                        **base,
                        "reason": "operation_conflict",
                        "subject": operation_id,
                    }
                return {"status": "duplicate", **base}

            user = uow.query_one(
                f"SELECT sect_id,COALESCE(stone,0) AS stone,COALESCE({column},0) AS level "
                "FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            sect = uow.query_one(
                "SELECT COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            if user is None:
                return {"status": "user_missing", **base}
            if sect is None:
                return {"status": "sect_missing", **base}
            if int(user["sect_id"] or 0) != sect_id:
                return {"status": "membership_changed", **base}
            if int(user["level"]) != expected_level:
                return {"status": "level_changed", "reason": "sect_practice_level_changed", **base}
            before_stone = as_int_like(user["stone"])
            if before_stone < stone_cost:
                return {"status": "stone_insufficient", "reason": "stone_insufficient", **base}
            if as_int_like(sect["materials"]) < materials_cost:
                return {"status": "materials_insufficient", "reason": "materials_insufficient", **base}

            changed = uow.execute(
                f"UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_sub(stone,?,'stone','actor')),"
                f"{column}=? WHERE user_id=? AND sect_id=? AND COALESCE(stone,0)>=? "
                f"AND COALESCE({column},0)=?",
                (stone_cost, next_level, user_id, sect_id, stone_cost, expected_level),
            )
            if changed.rowcount != 1:
                self._reject("player", user_id)
            saved_user = uow.query_one(
                f"SELECT stone,{column} AS level FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if (
                saved_user is None
                or as_int_like(saved_user["stone"]) != before_stone - stone_cost
                or int(saved_user["level"]) != next_level
            ):
                self._reject("player", user_id)

            before_materials = as_int_like(sect["materials"])
            assets = uow.execute(
                "UPDATE sects SET sect_materials=xiuxian_int_store(xiuxian_int_sub(sect_materials,?,'sect_materials','actor')) "
                "WHERE sect_id=? AND xiuxian_int_cmp(COALESCE(sect_materials,0),?,1,'sect_materials','actor')>=0",
                (materials_cost, sect_id, materials_cost),
            )
            if assets.rowcount != 1:
                self._reject("assets", sect_id)
            saved_sect = uow.query_one(
                "SELECT COALESCE(sect_materials,0) AS materials "
                "FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            if (
                saved_sect is None
                or as_int_like(saved_sect["materials"]) != before_materials - materials_cost
            ):
                self._reject("assets", sect_id)

            recorded = uow.execute(
                "INSERT INTO sect_practice_operations(operation_id,user_id,sect_id,practice_type,"
                "from_level,to_level,stone_cost,materials_cost) VALUES(?,?,?,?,?,?,?,?)",
                (operation_id, user_id, sect_id, practice_type, expected_level, next_level, stone_cost, materials_cost),
            )
            if recorded.rowcount != 1:
                self._reject("receipt", operation_id)
            saved_receipt = uow.query_one(
                "SELECT user_id,sect_id,practice_type,from_level,to_level,stone_cost,materials_cost "
                "FROM sect_practice_operations WHERE operation_id=?",
                (operation_id,),
            )
            if saved_receipt is None or tuple(
                (str(saved_receipt["user_id"]), int(saved_receipt["sect_id"]), str(saved_receipt["practice_type"]),
                 int(saved_receipt["from_level"]), int(saved_receipt["to_level"]), int(saved_receipt["stone_cost"]),
                 int(saved_receipt["materials_cost"]))
            ) != (
                user_id, sect_id, practice_type, expected_level, next_level, stone_cost, materials_cost
            ):
                self._reject("receipt", operation_id)

            # A receipt trigger can still rewrite an earlier row after its first
            # projection check; verify the business rows again before committing.
            final_user = uow.query_one(
                f"SELECT sect_id,stone,{column} AS level FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if (
                final_user is None
                or int(final_user["sect_id"] or 0) != sect_id
                or as_int_like(final_user["stone"]) != before_stone - stone_cost
                or int(final_user["level"]) != next_level
            ):
                self._reject("player", user_id)
            final_sect = uow.query_one(
                "SELECT COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            if (
                final_sect is None
                or as_int_like(final_sect["materials"]) != before_materials - materials_cost
            ):
                self._reject("assets", sect_id)
            return {"status": "upgraded", **base}
